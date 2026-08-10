const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, '../public')));

// Route: skapa nytt rum
app.get('/skapa', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

// Route: gå med i rum
app.get('/rum/:roomId', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

// ── Speldata i minnet ────────────────────────────────────────────
const rooms = {}; // roomId → RoomState

function createRoom(roomId, hostSocketId) {
  return {
    id: roomId,
    hostSocketId,
    players: [],       // { id, name, socketId, ready }
    status: 'lobby',   // lobby | playing | finished
    maxCards: 10,
    createdAt: Date.now()
  };
}

// ── Socket.io events ─────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log('Ansluten:', socket.id);

  // Värden skapar ett nytt rum
  socket.on('create_room', ({ playerName, maxCards }, callback) => {
    const roomId = uuidv4().slice(0, 8).toUpperCase();
    const room = createRoom(roomId, socket.id);
    room.maxCards = maxCards || 10;

    const player = {
      id: socket.id,
      name: playerName.trim(),
      socketId: socket.id,
      ready: false,
      isHost: true
    };
    room.players.push(player);
    rooms[roomId] = room;

    socket.join(roomId);
    socket.roomId = roomId;

    console.log(`Rum ${roomId} skapat av ${playerName}`);
    callback({ success: true, roomId, player });
    io.to(roomId).emit('room_update', sanitizeRoom(room));
  });

  // Spelare går med i rum via inbjudningslänk
  socket.on('join_room', ({ roomId, playerName }, callback) => {
    const room = rooms[roomId];

    if (!room) {
      return callback({ success: false, error: 'Rummet hittades inte.' });
    }
    if (room.status !== 'lobby') {
      return callback({ success: false, error: 'Spelet har redan börjat.' });
    }
    if (room.players.length >= 7) {
      return callback({ success: false, error: 'Rummet är fullt (max 7 spelare).' });
    }

    const nameExists = room.players.some(
      p => p.name.toLowerCase() === playerName.trim().toLowerCase()
    );
    if (nameExists) {
      return callback({ success: false, error: 'Namnet är redan taget i detta rum.' });
    }

    const player = {
      id: socket.id,
      name: playerName.trim(),
      socketId: socket.id,
      ready: false,
      isHost: false
    };
    room.players.push(player);

    socket.join(roomId);
    socket.roomId = roomId;

    console.log(`${playerName} gick med i rum ${roomId}`);
    callback({ success: true, roomId, player });
    io.to(roomId).emit('room_update', sanitizeRoom(room));
  });

  // Spelare markerar sig som redo
  socket.on('set_ready', ({ roomId, ready }) => {
    const room = rooms[roomId];
    if (!room) return;

    const player = room.players.find(p => p.socketId === socket.id);
    if (player) {
      player.ready = ready;
      io.to(roomId).emit('room_update', sanitizeRoom(room));
    }
  });

  // Värden uppdaterar maxCards
  socket.on('update_settings', ({ roomId, maxCards }) => {
    const room = rooms[roomId];
    if (!room || room.hostSocketId !== socket.id) return;
    room.maxCards = maxCards;
    io.to(roomId).emit('room_update', sanitizeRoom(room));
  });

  // Värden startar spelet
  socket.on('start_game', ({ roomId }, callback) => {
    const room = rooms[roomId];
    if (!room) return callback({ success: false, error: 'Rum saknas.' });
    if (room.hostSocketId !== socket.id) return callback({ success: false, error: 'Bara värden kan starta.' });
    if (room.players.length < 2) return callback({ success: false, error: 'Minst 2 spelare krävs.' });

    const notReady = room.players.filter(p => !p.ready);
    if (notReady.length > 0) {
      return callback({ success: false, error: 'Alla spelare är inte redo.' });
    }

    room.status = 'playing';
    console.log(`Spelet startar i rum ${roomId}`);
    callback({ success: true });
    io.to(roomId).emit('game_starting', sanitizeRoom(room));
  });

  // Spelare kopplar från
  socket.on('disconnect', () => {
    const roomId = socket.roomId;
    if (!roomId || !rooms[roomId]) return;

    const room = rooms[roomId];
    room.players = room.players.filter(p => p.socketId !== socket.id);

    if (room.players.length === 0) {
      delete rooms[roomId];
      console.log(`Rum ${roomId} borttaget (tomt)`);
    } else {
      // Om värden lämnar → nästa spelare blir värd
      if (room.hostSocketId === socket.id && room.players.length > 0) {
        room.players[0].isHost = true;
        room.hostSocketId = room.players[0].socketId;
      }
      io.to(roomId).emit('room_update', sanitizeRoom(room));
      io.to(roomId).emit('player_left', { name: socket.playerName || 'En spelare' });
    }
  });
});

// Ta bort känslig info innan vi skickar till klienter
function sanitizeRoom(room) {
  return {
    id: room.id,
    players: room.players.map(p => ({
      id: p.id,
      name: p.name,
      ready: p.ready,
      isHost: p.isHost
    })),
    status: room.status,
    maxCards: room.maxCards,
    hostSocketId: room.hostSocketId
  };
}

// Starta servern
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Plump-servern körs på port ${PORT}`);
});
