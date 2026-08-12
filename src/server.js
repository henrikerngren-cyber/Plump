const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const { Pool } = require('pg');
const { createDeck, shuffleDeck, cutDeck, dealCards, buildRoundSequence } = require('./cards');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  transports: ['polling', 'websocket'],
  allowUpgrades: true,
  pingTimeout: 60000,
  pingInterval: 25000
});

// ── Postgres ─────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS rooms (
      id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `);
  console.log('Databas initierad');
}

async function getRoom(roomId) {
  const res = await pool.query('SELECT data FROM rooms WHERE id = $1', [roomId]);
  return res.rows.length ? res.rows[0].data : null;
}

async function saveRoom(room) {
  await pool.query(`
    INSERT INTO rooms (id, data, updated_at) VALUES ($1, $2, NOW())
    ON CONFLICT (id) DO UPDATE SET data = $2, updated_at = NOW()
  `, [room.id, JSON.stringify(room)]);
}

async function deleteRoom(roomId) {
  await pool.query('DELETE FROM rooms WHERE id = $1', [roomId]);
}

// ── Express ───────────────────────────────────────────
const publicPath = path.join(__dirname, '../public');
console.log('Public path:', publicPath);
console.log('__dirname:', __dirname);
app.use(express.static(publicPath));

// Rensa gamla rum (äldre än 24h eller tomma)
async function cleanupOldRooms() {
  try {
    await pool.query(`DELETE FROM rooms WHERE updated_at < NOW() - INTERVAL '120 minutes'`);
    await pool.query(`DELETE FROM rooms WHERE data->>'status' = 'finished'`);
    console.log('Gamla rum rensade');
  } catch (err) { console.error('Cleanup fel:', err); }
}

// Rensa var 30:e minut
setInterval(cleanupOldRooms, 15 * 60 * 1000); // Var 15:e minut

app.get('/debug/cleanup', async (req, res) => {
  await cleanupOldRooms();
  res.json({ message: 'Gamla rum rensade!' });
});

app.get('/debug/nuke', async (req, res) => {
  await pool.query('DELETE FROM rooms');
  res.json({ message: 'Alla rum raderade!' });
});

app.get('/debug/rooms', async (req, res) => {
  const result = await pool.query('SELECT id, data FROM rooms');
  const rooms = result.rows.map(r => ({
    id: r.id,
    players: r.data.players.map(p => p.name),
    status: r.data.status,
    round: r.data.currentRoundIndex
  }));
  res.json({ rooms, count: rooms.length });
});

app.get('*', (req, res) => {
  const indexPath = path.join(__dirname, '../public/index.html');
  console.log('Serving index from:', indexPath);
  res.sendFile(indexPath);
});

// ── Socket.io ─────────────────────────────────────────
io.on('connection', (socket) => {
  console.log('Ansluten:', socket.id);

  // ── Lobby events ──
  socket.on('create_room', async ({ playerName, maxCards }, callback) => {
    try {
      const roomId = uuidv4().slice(0, 8).toUpperCase();
      const room = {
        id: roomId,
        hostSocketId: socket.id,
        players: [{
          id: socket.id,
          name: playerName.trim(),
          socketId: socket.id,
          ready: false,
          isHost: true,
          score: 0,
          consecutiveZeros: 0,
          lastZeroSuccess: false
        }],
        status: 'lobby',
        maxCards: maxCards || 10,
        createdAt: Date.now(),
        // Spelstate
        deck: [],
        hands: {},
        roundSequence: [],
        currentRoundIndex: 0,
        currentDealer: null,
        bids: {},
        tricks: {},
        currentTrick: [],
        currentPlayerIndex: 0,
        leadSuit: null,
        scores: []
      };

      await saveRoom(room);
      socket.join(roomId);
      socket.roomId = roomId;

      callback({ success: true, roomId, player: room.players[0] });
      io.to(roomId).emit('room_update', sanitizeRoom(room));
    } catch (err) {
      console.error('create_room fel:', err);
      callback({ success: false, error: 'Serverfel.' });
    }
  });

  socket.on('join_room', async ({ roomId, playerName }, callback) => {
    try {
      const cleanId = roomId.trim().toUpperCase();
      const room = await getRoom(cleanId);

      if (!room) return callback({ success: false, error: 'Rummet hittades inte.' });
      if (room.status !== 'lobby') return callback({ success: false, error: 'Spelet har redan börjat.' });
      if (room.players.length >= 7) return callback({ success: false, error: 'Rummet är fullt.' });

      const nameExists = room.players.some(p => p.name.toLowerCase() === playerName.trim().toLowerCase());
      if (nameExists) return callback({ success: false, error: 'Namnet är redan taget.' });

      const player = {
        id: socket.id,
        name: playerName.trim(),
        socketId: socket.id,
        ready: false,
        isHost: false,
        score: 0,
        consecutiveZeros: 0,
        lastZeroSuccess: false
      };
      room.players.push(player);
      await saveRoom(room);

      socket.join(cleanId);
      socket.roomId = cleanId;

      callback({ success: true, roomId: cleanId, player });
      io.to(cleanId).emit('room_update', sanitizeRoom(room));
    } catch (err) {
      console.error('join_room fel:', err);
      callback({ success: false, error: 'Serverfel.' });
    }
  });

  socket.on('set_ready', async ({ roomId, ready }) => {
    try {
      const room = await getRoom(roomId);
      if (!room) return;
      const player = room.players.find(p => p.socketId === socket.id);
      if (player) {
        player.ready = ready;
        await saveRoom(room);
        io.to(roomId).emit('room_update', sanitizeRoom(room));
      }
    } catch (err) { console.error('set_ready fel:', err); }
  });

  socket.on('update_settings', async ({ roomId, maxCards }) => {
    try {
      const room = await getRoom(roomId);
      if (!room || room.hostSocketId !== socket.id) return;
      room.maxCards = maxCards;
      await saveRoom(room);
      io.to(roomId).emit('room_update', sanitizeRoom(room));
    } catch (err) { console.error('update_settings fel:', err); }
  });

  socket.on('start_game', async ({ roomId }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room) return callback({ success: false, error: 'Rum saknas.' });
      if (room.hostSocketId !== socket.id) return callback({ success: false, error: 'Bara värden kan starta.' });
      if (room.players.length < 2) return callback({ success: false, error: 'Minst 2 spelare krävs.' });
      if (room.players.some(p => !p.ready)) return callback({ success: false, error: 'Alla spelare är inte redo.' });

      // Sätt upp spelet
      room.status = 'playing';
      room.roundSequence = buildRoundSequence(room.players.length, room.maxCards);
      room.currentRoundIndex = 0;
      room.scores = room.players.map(p => ({ playerId: p.id, name: p.name, rounds: [] }));

      // Slumpa vem som ger korten
      const dealerIdx = Math.floor(Math.random() * room.players.length);
      room.currentDealerName = room.players[dealerIdx].name;
      room.currentDealer = room.players[dealerIdx].socketId;

      await saveRoom(room);
      callback({ success: true });
      io.to(roomId).emit('game_starting', sanitizeRoom(room));
      
      // Starta första omgången
      await startRound(room, roomId);
    } catch (err) {
      console.error('start_game fel:', err);
      callback({ success: false, error: 'Serverfel.' });
    }
  });

  // ── Återanslut till pågående spel ──
  socket.on('rejoin_game', async ({ roomId, playerName }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room) return callback({ success: false, error: 'Rum hittades inte.' });

      // Hitta spelaren via namn
      const player = room.players.find(p => p.name === playerName);
      if (!player) return callback({ success: false, error: 'Spelare hittades inte.' });

      // Uppdatera socketId
      player.socketId = socket.id;
      player.id = socket.id;
      if (room.hostSocketId === player.id) room.hostSocketId = socket.id;
      if (room.currentDealerName === player.name) room.currentDealer = socket.id;

      // Uppdatera bids/tricks/hands - sök med både gammalt id och namn
      const oldSocketId = player.socketId; // redan uppdaterat till socket.id ovan, spara inte
      
      // Hitta handen - sök med flera nycklar
      const myHand = room.hands?.[socket.id] 
        || room.hands?.[player.name]
        || [];

      // Uppdatera bids med nytt socketId
      if (room.bids) {
        Object.keys(room.bids).forEach(key => {
          if (key === oldSocketId) {
            room.bids[socket.id] = room.bids[key];
            delete room.bids[key];
          }
        });
      }
      
      // Uppdatera tricks med nytt socketId  
      if (room.tricks) {
        Object.keys(room.tricks).forEach(key => {
          if (key === oldSocketId) {
            room.tricks[socket.id] = room.tricks[key];
            delete room.tricks[key];
          }
        });
      }

      // Uppdatera hands med nytt socketId
      if (room.hands) {
        if (room.hands[oldSocketId]) {
          room.hands[socket.id] = room.hands[oldSocketId];
          delete room.hands[oldSocketId];
        }
      }

      await saveRoom(room);
      socket.join(roomId);
      socket.roomId = roomId;

      const numCards = room.roundSequence?.[room.currentRoundIndex] || 0;

      callback({
        success: true,
        room: sanitizeRoom(room),
        hand: myHand,
        numCards,
        bids: room.bids || {},
        tricks: room.tricks || {},
        phase: room.deckState || 'unshuffled',
        currentDealer: room.currentDealer,
        roundIndex: room.currentRoundIndex,
        totalRounds: room.roundSequence?.length || 0
      });
    } catch (err) {
      console.error('rejoin_game fel:', err);
      callback({ success: false, error: 'Serverfel.' });
    }
  });

  // ── Spellogik events ──

  // Givaren blandar korten
  socket.on('shuffle_deck', async ({ roomId }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room || room.currentDealer !== socket.id) return;
      
      room.deck = shuffleDeck(createDeck());
      room.deckState = 'shuffled';
      await saveRoom(room);
      
      callback({ success: true });
      io.to(roomId).emit('deck_shuffled', { dealerId: socket.id, dealerName: getPlayerName(room, socket.id) });
    } catch (err) { console.error('shuffle_deck fel:', err); }
  });

  // Givaren kuperar
  socket.on('cut_deck', async ({ roomId }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room || room.currentDealer !== socket.id) return;
      
      room.deck = cutDeck(room.deck);
      room.deckState = 'cut';
      await saveRoom(room);
      
      callback({ success: true });
      io.to(roomId).emit('deck_cut', { dealerId: socket.id, dealerName: getPlayerName(room, socket.id) });
    } catch (err) { console.error('cut_deck fel:', err); }
  });

  // Givaren delar ut korten
  socket.on('deal_cards', async ({ roomId }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room || room.currentDealer !== socket.id || room.deckState !== 'cut') return;
      
      const numCards = room.roundSequence[room.currentRoundIndex];
      room.hands = dealCards(room.deck, room.players, numCards);
      room.deckState = 'dealt';
      room.bids = {};
      room.tricks = {};
      room.players.forEach(p => room.tricks[p.id] = 0);
      
      await saveRoom(room);
      callback({ success: true });

      // Skicka bara varje spelares egna kort (utom vid 1-kortsomgång)
      const isOneCardRound = numCards === 1;
      for (const player of room.players) {
        const hand = isOneCardRound ? [] : room.hands[player.id];
        io.to(player.socketId).emit('cards_dealt', {
          hand,
          numCards,
          isOneCardRound,
          dealer: room.currentDealer,
          roundIndex: room.currentRoundIndex,
          totalRounds: room.roundSequence.length
        });
      }

      // Starta budgivning
      startBidding(room, roomId);
    } catch (err) { console.error('deal_cards fel:', err); }
  });

  // Spelare lägger ett bud
  socket.on('place_bid', async ({ roomId, bid }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room) return callback({ success: false, error: 'Rum saknas.' });

      const numCards = room.roundSequence[room.currentRoundIndex];
      const biddingOrder = getBiddingOrder(room);
      const numBidsPlaced = room.players.filter(p =>
        room.bids[p.socketId] !== undefined || room.bids[p.id] !== undefined
      ).length;
      const currentBidder = biddingOrder[numBidsPlaced];

      if (currentBidder !== socket.id) {
        return callback({ success: false, error: 'Det är inte din tur att bjuda.' });
      }

      // Validera bud
      const validation = validateBid(room, socket.id, bid, numCards, biddingOrder);
      if (!validation.valid) {
        return callback({ success: false, error: validation.error });
      }

      room.bids[socket.id] = bid;
      await saveRoom(room);
      callback({ success: true });

      const bidderName = room.players.find(p => p.socketId === socket.id || p.id === socket.id)?.name || '';
      io.to(roomId).emit('bid_placed', {
        playerId: socket.id,
        playerName: bidderName,
        bid,
        bids: room.bids
      });

      // Kolla om alla har budat
      const totalBidsPlaced = room.players.filter(p =>
        room.bids[p.socketId] !== undefined || room.bids[p.id] !== undefined
      ).length;
      if (totalBidsPlaced === room.players.length) {
        await startPlaying(room, roomId);
      } else {
        // Nästa spelare ska bjuda
        const nextBidder = biddingOrder[totalBidsPlaced];
        const nextBidderName = room.players.find(p => p.socketId === nextBidder)?.name || '';
        io.to(roomId).emit('next_bidder', { playerId: nextBidder, playerName: nextBidderName });
      }
    } catch (err) {
      console.error('place_bid fel:', err);
      callback({ success: false, error: 'Serverfel.' });
    }
  });

  // Spelare spelar ett kort
  socket.on('play_card', async ({ roomId, cardIndex, card: cardData }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room) return callback({ success: false, error: 'Rum saknas.' });

      const playOrder = getPlayOrder(room);
      const currentPlayer = playOrder[room.currentPlayerIndex];

      if (currentPlayer !== socket.id) {
        return callback({ success: false, error: 'Det är inte din tur.' });
      }

      const hand = room.hands[socket.id];

      // Hitta kortet via suit+value om klienten skickade kortdata (hanterar sorterade händer)
      let resolvedIndex = cardIndex;
      if (cardData && cardData.suit && cardData.value) {
        const found = hand.findIndex(c => c.suit === cardData.suit && c.value === cardData.value);
        if (found !== -1) resolvedIndex = found;
      }

      const card = hand[resolvedIndex];
      if (!card) return callback({ success: false, error: 'Ogiltigt kort.' });

      // Validera följeplikt
      if (room.currentTrick.length > 0 && room.leadSuit) {
        const hasLeadSuit = hand.some(c => c.suit === room.leadSuit);
        if (hasLeadSuit && card.suit !== room.leadSuit) {
          return callback({ success: false, error: `Du måste följa ${room.leadSuit}!` });
        }
      }

      // Ta bort kortet från handen
      room.hands[socket.id] = hand.filter((_, i) => i !== resolvedIndex);

      // Sätt leadsuit vid första kort i sticket
      if (room.currentTrick.length === 0) {
        room.leadSuit = card.suit;
      }

      room.currentTrick.push({ playerId: socket.id, card });
      await saveRoom(room);
      callback({ success: true });

      io.to(roomId).emit('card_played', {
        playerId: socket.id,
        playerName: room.players.find(p => p.socketId === socket.id)?.name,
        card,
        trick: room.currentTrick
      });

      // Kolla om alla spelat i detta stick
      if (room.currentTrick.length === room.players.length) {
        await resolveTrick(room, roomId);
      } else {
        room.currentPlayerIndex++;
        await saveRoom(room);
        const nextPlayer = playOrder[room.currentPlayerIndex];
        io.to(roomId).emit('next_player', { playerId: nextPlayer });
      }
    } catch (err) {
      console.error('play_card fel:', err);
      callback({ success: false, error: 'Serverfel.' });
    }
  });

  // Återanslut till lobby
  socket.on('rejoin_lobby', async ({ roomId, playerName }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room) return callback({ success: false });

      // Uppdatera spelarens socketId
      const player = room.players.find(p => p.name === playerName);
      if (player) {
        const wasHost = player.isHost;
        player.socketId = socket.id;
        player.id = socket.id;
        if (wasHost) {
          room.hostSocketId = socket.id;
        }
        await saveRoom(room);
      }

      socket.join(roomId);
      socket.roomId = roomId;

      // Meddela alla i rummet om uppdateringen
      io.to(roomId).emit('room_update', sanitizeRoom(room));
      callback({ success: true, room: sanitizeRoom(room) });
    } catch (err) {
      console.error('rejoin_lobby fel:', err);
      callback({ success: false });
    }
  });

  // Keep-alive ping
  socket.on('ping_room', async ({ roomId }) => {
    try {
      const room = await getRoom(roomId);
      if (room) {
        // Uppdatera updated_at så rummet inte rensas
        await pool.query('UPDATE rooms SET updated_at = NOW() WHERE id = $1', [roomId]);
      }
    } catch (err) { console.error('ping_room fel:', err); }
  });

  socket.on('disconnect', async () => {
    const roomId = socket.roomId;
    const disconnectedSocketId = socket.id;
    if (!roomId) return;

    console.log(`Spelare ${disconnectedSocketId} frånkopplad från rum ${roomId} - väntar 90 sekunder`);

    // Vänta 90 sekunder innan spelaren tas bort - ger tid att återansluta
    setTimeout(async () => {
      try {
        const room = await getRoom(roomId);
        if (!room) return;

        // Kolla om spelaren återanslutit med nytt socketId (via rejoin)
        const stillDisconnected = room.players.find(p => p.socketId === disconnectedSocketId);
        if (!stillDisconnected) {
          console.log(`Spelare ${disconnectedSocketId} har redan återanslutit - hoppar över borttagning`);
          return;
        }

        room.players = room.players.filter(p => p.socketId !== disconnectedSocketId);

        if (room.players.length === 0) {
          await deleteRoom(roomId);
          console.log(`Rum ${roomId} raderat - inga spelare kvar`);
        } else {
          if (room.hostSocketId === disconnectedSocketId) {
            room.players[0].isHost = true;
            room.hostSocketId = room.players[0].socketId;
          }
          await saveRoom(room);
          io.to(roomId).emit('room_update', sanitizeRoom(room));
          io.to(roomId).emit('player_left', { name: 'En spelare' });
        }
      } catch (err) { console.error('disconnect timeout fel:', err); }
    }, 90000); // 90 sekunder grace period
  });
});

// ── Spelhjälpfunktioner ───────────────────────────────

async function startRound(room, roomId) {
  // Synka currentDealer mot aktuellt socketId (kan ha ändrats vid reconnect)
  if (room.currentDealerName) {
    const dealerPlayer = room.players.find(p => p.name === room.currentDealerName);
    if (dealerPlayer) room.currentDealer = dealerPlayer.socketId;
  }

  room.deckState = 'unshuffled';
  room.deck = createDeck();
  room.currentTrick = [];
  room.currentTrickStarter = null; // Nollställ så förra omgångens starter inte används
  room.leadSuit = null;
  room.bids = {};
  room.tricks = {};
  room.players.forEach(p => { room.tricks[p.id] = 0; room.tricks[p.socketId] = 0; });
  await saveRoom(room);

  const numCards = room.roundSequence[room.currentRoundIndex];
  
  // Skicka round_started individuellt till varje spelare med deras socketId
  room.players.forEach(p => {
    io.to(p.socketId).emit('round_started', {
      roundIndex: room.currentRoundIndex,
      totalRounds: room.roundSequence.length,
      numCards,
      dealer: room.currentDealer,
      dealerName: room.currentDealerName,
      mySocketId: p.socketId, // Spelarens eget socketId
      players: room.players.map(pl => ({ id: pl.id, socketId: pl.socketId, name: pl.name }))
    });
  });
}

function getBiddingOrder(room) {
  // Spelaren till vänster om givaren bjuder först
  const dealerIdx = room.players.findIndex(p => p.socketId === room.currentDealer || p.id === room.currentDealer);
  const startIdx = dealerIdx === -1 ? 0 : dealerIdx;
  const order = [];
  for (let i = 1; i <= room.players.length; i++) {
    const p = room.players[(startIdx + i) % room.players.length];
    order.push(p.socketId); // alltid socketId - p.id kan skilja sig vid reconnect
  }
  return order;
}

function getPlayOrder(room) {
  if (room.currentTrickStarter) {
    const starterIdx = room.players.findIndex(p => p.socketId === room.currentTrickStarter || p.id === room.currentTrickStarter);
    const idx = starterIdx === -1 ? 0 : starterIdx;
    return room.players.slice(idx).concat(room.players.slice(0, idx)).map(p => p.socketId || p.id);
  }
  // Spelaren till vänster om givaren startar
  const dealerIdx = room.players.findIndex(p => p.socketId === room.currentDealer || p.id === room.currentDealer);
  const startIdx = dealerIdx === -1 ? 0 : dealerIdx;
  const order = [];
  for (let i = 1; i <= room.players.length; i++) {
    const p = room.players[(startIdx + i) % room.players.length];
    order.push(p.socketId || p.id);
  }
  return order;
}

function validateBid(room, socketId, bid, numCards, biddingOrder) {
  if (bid < 0 || bid > numCards) {
    return { valid: false, error: `Bud måste vara mellan 0 och ${numCards}.` };
  }

  const player = room.players.find(p => p.socketId === socketId);
  const isOneCardRound = numCards === 1;

  // Kolla noll-regeln (ej för 1-kortsomgångar)
  if (!isOneCardRound && bid === 0) {
    if (player.consecutiveZeros >= 3) {
      return { valid: false, error: 'Du kan inte bjuda 0 tre gånger i rad med lyckat resultat.' };
    }
  }

  // Siste spelaren får inte göra summan lika med numCards
  const bidsPlaced = room.players.filter(p =>
    room.bids[p.socketId] !== undefined || room.bids[p.id] !== undefined
  ).length;
  const isLastBidder = bidsPlaced === room.players.length - 1;
  if (isLastBidder) {
    const currentSum = Object.values(room.bids).reduce((a, b) => a + b, 0);
    if (currentSum + bid === numCards) {
      return { valid: false, error: `Du kan inte bjuda ${bid} – summan får inte bli ${numCards}.` };
    }
  }

  return { valid: true };
}

async function startBidding(room, roomId) {
  const biddingOrder = getBiddingOrder(room);
  const firstBidder = biddingOrder[0];
  const numCards = room.roundSequence[room.currentRoundIndex];
  const isOneCardRound = numCards === 1;

  // Bygg en map socketId -> namn för klienten
  const playerNames = {};
  room.players.forEach(p => {
    if (p.socketId) playerNames[p.socketId] = p.name;
    if (p.id) playerNames[p.id] = p.name;
  });
  
  io.to(roomId).emit('bidding_started', {
    firstBidder,
    firstBidderName: room.players.find(p => p.socketId === firstBidder || p.id === firstBidder)?.name || '',
    biddingOrder,
    numCards,
    isOneCardRound,
    playerNames
  });
}

async function startPlaying(room, roomId) {
  // Den med flest bud börjar. Vid lika - närmast givaren medurs.
  const dealerIdx = room.players.findIndex(p => p.socketId === room.currentDealer || p.id === room.currentDealer);
  const startIdx = dealerIdx === -1 ? 0 : dealerIdx;
  
  // Sortera spelare i medurs ordning från givaren
  const orderedPlayers = [];
  for (let i = 1; i <= room.players.length; i++) {
    orderedPlayers.push(room.players[(startIdx + i) % room.players.length]);
  }
  
  // Hitta max bud
  let maxBid = -1;
  let firstPlayer = null;
  for (const p of orderedPlayers) {
    const bid = room.bids[p.socketId] ?? room.bids[p.id] ?? 0;
    if (bid > maxBid) {
      maxBid = bid;
      firstPlayer = p;
    }
  }
  
  // Fallback om ingen hittades
  if (!firstPlayer) firstPlayer = orderedPlayers[0];
  
  const firstPlayerId = firstPlayer.socketId || firstPlayer.id;
  room.currentTrickStarter = firstPlayerId;
  room.currentPlayerIndex = 0;
  room.currentTrick = [];
  await saveRoom(room);

  // Bygg spelordning med vinnaren först
  const winnerIdx = room.players.findIndex(p => p.socketId === firstPlayerId || p.id === firstPlayerId);
  const playOrder = [];
  for (let i = 0; i < room.players.length; i++) {
    const p = room.players[(winnerIdx + i) % room.players.length];
    playOrder.push(p.socketId || p.id);
  }

  const firstPlayerName = firstPlayer.name || '';
  const playerNames = {};
  room.players.forEach(p => {
    if (p.socketId) playerNames[p.socketId] = p.name;
    if (p.id) playerNames[p.id] = p.name;
  });
  
  // Bygg även bud-map med namn som nyckel
  const bidsByName = {};
  room.players.forEach(p => {
    const bid = room.bids[p.socketId] ?? room.bids[p.id];
    if (bid !== undefined) {
      bidsByName[p.name] = bid;
      bidsByName[p.socketId] = bid;
      bidsByName[p.id] = bid;
    }
  });

  io.to(roomId).emit('playing_started', {
    playOrder,
    firstPlayer: firstPlayerId,
    firstPlayerName,
    bids: bidsByName,
    playerNames
  });
}

async function resolveTrick(room, roomId) {
  const { determineStickWinner } = require('./cards');
  const cards = room.currentTrick.map(t => t.card);
  const winnerIdx = determineStickWinner(cards, room.leadSuit);
  const winner = room.currentTrick[winnerIdx];

  room.tricks[winner.playerId] = (room.tricks[winner.playerId] || 0) + 1;

  // Bygg tricks-map med namn som nyckel
  const playerTricks = {};
  room.players.forEach(p => {
    const t = room.tricks[p.socketId] ?? room.tricks[p.id] ?? 0;
    playerTricks[p.name] = t;
    playerTricks[p.socketId] = t;
    playerTricks[p.id] = t;
  });

  io.to(roomId).emit('trick_resolved', {
    winner: { playerId: winner.playerId, playerName: getPlayerName(room, winner.playerId) },
    tricks: room.tricks,
    playerTricks,
    trick: room.currentTrick
  });

  await new Promise(r => setTimeout(r, 2000));

  // Kolla om alla stick är spelade
  const numCards = room.roundSequence[room.currentRoundIndex];
  const totalTricks = Object.values(room.tricks).reduce((a, b) => a + b, 0);

  if (totalTricks >= numCards) {
    await resolveRound(room, roomId);
  } else {
    // Nästa stick – vinnaren startar
    room.currentTrick = [];
    room.leadSuit = null;
    room.currentTrickStarter = winner.playerId;
    room.currentPlayerIndex = 0;
    await saveRoom(room);

    const playOrder = getPlayOrder(room);
    const playerTricks2 = {};
    room.players.forEach(p => {
      const t = room.tricks[p.socketId] ?? room.tricks[p.id] ?? 0;
      playerTricks2[p.name] = t;
      playerTricks2[p.socketId] = t;
    });
    io.to(roomId).emit('next_trick', {
      firstPlayer: winner.playerId,
      playOrder,
      tricks: room.tricks,
      playerTricks: playerTricks2
    });
  }
}

async function resolveRound(room, roomId) {
  const numCards = room.roundSequence[room.currentRoundIndex];
  const roundResults = [];

  for (const player of room.players) {
    const bid = room.bids[player.socketId] ?? room.bids[player.id] ?? 0;
    const tricks = room.tricks[player.socketId] ?? room.tricks[player.id] ?? 0;
    const tookAll = tricks === numCards && numCards > 1;
    const success = bid === tricks;
    let points = 0;

    if (success) {
      points = tookAll ? parseInt(`${tricks}0`) : 10 + tricks;
    } else {
      points = 0;
    }

    // Uppdatera noll-streak
    if (bid === 0 && numCards > 1) {
      if (success) {
        player.consecutiveZeros = (player.consecutiveZeros || 0) + 1;
      } else {
        player.consecutiveZeros = 0;
      }
    } else if (numCards > 1) {
      if (!success) player.consecutiveZeros = 0;
      // om bid !== 0, nollställ inte streak
    }

    player.score = (player.score || 0) + points;

    const scoreEntry = room.scores.find(s => s.playerId === player.id || s.name === player.name);
    if (scoreEntry) {
      scoreEntry.rounds.push({ bid, tricks, points, success, tookAll });
    }

    roundResults.push({
      playerId: player.id,
      playerName: player.name,
      bid, tricks, points, success, tookAll,
      totalScore: player.score
    });
  }

  await saveRoom(room);

  io.to(roomId).emit('round_resolved', {
    results: roundResults,
    scores: room.scores,
    roundIndex: room.currentRoundIndex
  });

  await new Promise(r => setTimeout(r, 3000));

  // Nästa omgång eller spelet slut
  room.currentRoundIndex++;
  if (room.currentRoundIndex >= room.roundSequence.length) {
    await endGame(room, roomId);
  } else {
    // Rotera givaren
    const dealerIdx = room.players.findIndex(p => p.name === room.currentDealerName);
    const nextDealer = room.players[(dealerIdx + 1) % room.players.length];
    room.currentDealerName = nextDealer.name;
    room.currentDealer = nextDealer.socketId;
    await saveRoom(room);
    await startRound(room, roomId);
  }
}

async function endGame(room, roomId) {
  room.status = 'finished';
  const winner = room.players.reduce((a, b) => a.score > b.score ? a : b);
  await saveRoom(room);

  io.to(roomId).emit('game_ended', {
    winner: { playerId: winner.id, playerName: winner.name, score: winner.score },
    finalScores: room.players.map(p => ({ playerId: p.id, playerName: p.name, score: p.score }))
  });
}

// Hjälpfunktion: hämta spelarnamn från socketId
function getPlayerName(room, socketId) {
  const player = room.players.find(p => p.socketId === socketId || p.id === socketId);
  return player?.name || '';
}

function sanitizeRoom(room) {
  return {
    id: room.id,
    players: room.players.map(p => ({
      id: p.id,
      socketId: p.socketId,
      name: p.name,
      ready: p.ready,
      isHost: p.isHost,
      score: p.score || 0,
      consecutiveZeros: p.consecutiveZeros || 0
    })),
    status: room.status,
    maxCards: room.maxCards,
    hostSocketId: room.hostSocketId,
    currentDealer: room.currentDealer,
    currentDealerName: room.currentDealerName,
    currentRoundIndex: room.currentRoundIndex,
    roundSequence: room.roundSequence
  };
}

const PORT = process.env.PORT || 3000;
initDB().then(() => {
  server.listen(PORT, () => {
    console.log(`Plump-servern körs på port ${PORT}`);
  });
}).catch(err => {
  console.error('Kunde inte initiera databas:', err);
  process.exit(1);
});
