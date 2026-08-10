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
  transports: ['websocket', 'polling']
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
app.use(express.static(path.join(__dirname, '../public')));

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
  res.sendFile(path.join(__dirname, '../public/index.html'));
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
      room.currentDealer = room.players[dealerIdx].id;

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
      if (room.currentDealer === player.id) room.currentDealer = socket.id;

      // Uppdatera bids/tricks-nycklar
      if (room.bids[player.id]) { room.bids[socket.id] = room.bids[player.id]; delete room.bids[player.id]; }
      if (room.tricks[player.id] !== undefined) { room.tricks[socket.id] = room.tricks[player.id]; delete room.tricks[player.id]; }
      if (room.hands[player.id]) { room.hands[socket.id] = room.hands[player.id]; delete room.hands[player.id]; }

      await saveRoom(room);
      socket.join(roomId);
      socket.roomId = roomId;

      const numCards = room.roundSequence?.[room.currentRoundIndex] || 0;
      const myHand = room.hands?.[socket.id] || [];

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
      io.to(roomId).emit('deck_shuffled', { dealerId: socket.id });
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
      io.to(roomId).emit('deck_cut', { dealerId: socket.id });
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
      const currentBidder = biddingOrder[Object.keys(room.bids).length];

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

      io.to(roomId).emit('bid_placed', {
        playerId: socket.id,
        playerName: room.players.find(p => p.socketId === socket.id)?.name,
        bid,
        bids: room.bids
      });

      // Kolla om alla har budat
      if (Object.keys(room.bids).length === room.players.length) {
        await startPlaying(room, roomId);
      } else {
        // Nästa spelare ska bjuda
        const nextBidder = biddingOrder[Object.keys(room.bids).length];
        io.to(roomId).emit('next_bidder', { playerId: nextBidder });
      }
    } catch (err) {
      console.error('place_bid fel:', err);
      callback({ success: false, error: 'Serverfel.' });
    }
  });

  // Spelare spelar ett kort
  socket.on('play_card', async ({ roomId, cardIndex }, callback) => {
    try {
      const room = await getRoom(roomId);
      if (!room) return callback({ success: false, error: 'Rum saknas.' });

      const playOrder = getPlayOrder(room);
      const currentPlayer = playOrder[room.currentPlayerIndex];

      if (currentPlayer !== socket.id) {
        return callback({ success: false, error: 'Det är inte din tur.' });
      }

      const hand = room.hands[socket.id];
      const card = hand[cardIndex];
      if (!card) return callback({ success: false, error: 'Ogiltigt kort.' });

      // Validera följeplikt
      if (room.currentTrick.length > 0 && room.leadSuit) {
        const hasLeadSuit = hand.some(c => c.suit === room.leadSuit);
        if (hasLeadSuit && card.suit !== room.leadSuit) {
          return callback({ success: false, error: `Du måste följa ${room.leadSuit}!` });
        }
      }

      // Ta bort kortet från handen
      room.hands[socket.id] = hand.filter((_, i) => i !== cardIndex);

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

  socket.on('disconnect', async () => {
    const roomId = socket.roomId;
    if (!roomId) return;
    try {
      const room = await getRoom(roomId);
      if (!room) return;
      room.players = room.players.filter(p => p.socketId !== socket.id);
      if (room.players.length === 0) {
        await deleteRoom(roomId);
      } else {
        if (room.hostSocketId === socket.id) {
          room.players[0].isHost = true;
          room.hostSocketId = room.players[0].socketId;
        }
        await saveRoom(room);
        io.to(roomId).emit('room_update', sanitizeRoom(room));
        io.to(roomId).emit('player_left', { name: 'En spelare' });
      }
    } catch (err) { console.error('disconnect fel:', err); }
  });
});

// ── Spelhjälpfunktioner ───────────────────────────────

async function startRound(room, roomId) {
  room.deckState = 'unshuffled';
  room.deck = createDeck();
  room.currentTrick = [];
  room.leadSuit = null;
  room.bids = {};
  room.tricks = {};
  room.players.forEach(p => room.tricks[p.id] = 0);
  await saveRoom(room);

  const numCards = room.roundSequence[room.currentRoundIndex];
  io.to(roomId).emit('round_started', {
    roundIndex: room.currentRoundIndex,
    totalRounds: room.roundSequence.length,
    numCards,
    dealer: room.currentDealer,
    players: room.players.map(p => ({ id: p.id, name: p.name }))
  });
}

function getBiddingOrder(room) {
  // Spelaren till vänster om givaren bjuder först
  const dealerIdx = room.players.findIndex(p => p.id === room.currentDealer);
  const order = [];
  for (let i = 1; i <= room.players.length; i++) {
    order.push(room.players[(dealerIdx + i) % room.players.length].id);
  }
  return order;
}

function getPlayOrder(room) {
  // Spelaren till vänster om givaren spelar först (i första sticket)
  // Vinnaren av föregående stick spelar först
  if (room.currentTrickStarter) {
    const starterIdx = room.players.findIndex(p => p.id === room.currentTrickStarter);
    return room.players.slice(starterIdx).concat(room.players.slice(0, starterIdx)).map(p => p.id);
  }
  const dealerIdx = room.players.findIndex(p => p.id === room.currentDealer);
  const order = [];
  for (let i = 1; i <= room.players.length; i++) {
    order.push(room.players[(dealerIdx + i) % room.players.length].id);
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
  const isLastBidder = Object.keys(room.bids).length === room.players.length - 1;
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

  io.to(roomId).emit('bidding_started', {
    firstBidder,
    biddingOrder,
    numCards,
    isOneCardRound
  });
}

async function startPlaying(room, roomId) {
  const playOrder = getPlayOrder(room);
  room.currentTrickStarter = playOrder[0];
  room.currentPlayerIndex = 0;
  room.currentTrick = [];
  await saveRoom(room);

  io.to(roomId).emit('playing_started', {
    playOrder,
    firstPlayer: playOrder[0],
    bids: room.bids
  });
}

async function resolveTrick(room, roomId) {
  const { determineStickWinner } = require('./cards');
  const cards = room.currentTrick.map(t => t.card);
  const winnerIdx = determineStickWinner(cards, room.leadSuit);
  const winner = room.currentTrick[winnerIdx];

  room.tricks[winner.playerId] = (room.tricks[winner.playerId] || 0) + 1;

  io.to(roomId).emit('trick_resolved', {
    winner: { playerId: winner.playerId, playerName: room.players.find(p => p.id === winner.playerId)?.name },
    tricks: room.tricks,
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
    io.to(roomId).emit('next_trick', {
      firstPlayer: winner.playerId,
      playOrder,
      tricks: room.tricks
    });
  }
}

async function resolveRound(room, roomId) {
  const numCards = room.roundSequence[room.currentRoundIndex];
  const roundResults = [];

  for (const player of room.players) {
    const bid = room.bids[player.id] ?? 0;
    const tricks = room.tricks[player.id] ?? 0;
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

    const scoreEntry = room.scores.find(s => s.playerId === player.id);
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
    const dealerIdx = room.players.findIndex(p => p.id === room.currentDealer);
    room.currentDealer = room.players[(dealerIdx + 1) % room.players.length].id;
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

function sanitizeRoom(room) {
  return {
    id: room.id,
    players: room.players.map(p => ({
      id: p.id,
      name: p.name,
      ready: p.ready,
      isHost: p.isHost,
      score: p.score || 0
    })),
    status: room.status,
    maxCards: room.maxCards,
    hostSocketId: room.hostSocketId,
    currentDealer: room.currentDealer,
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
