const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const { Pool } = require('pg');

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

// ── Express routes ───────────────────────────────────
app.use(express.static(path.join(__dirname, '../public')));

app.get('/debug/rooms', async (req, res) => {
  const result = await pool.query('SELECT id, data FROM rooms');
  const rooms = result.rows.map(r => ({
    id: r.id,
    players: r.data.players.map(p => p.name),
    status: r.data.status
  }));
  res.json({ rooms, count: rooms.length });
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

// ── Socket.io ────────────────────────────────────────
io.on('connection', (socket) => {
  console.log('Ansluten:', socket.id);

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
          isHost: true
        }],
        status: 'lobby',
        maxCards: maxCards || 10,
        createdAt: Date.now()
      };

      await saveRoom(room);
      socket.join(roomId);
      socket.roomId = roomId;

      console.log(`Rum ${roomId} skapat av ${playerName}`);
      callback({ success: true, roomId, player: room.players[0] });
      io.to(roomId).emit('room_update', sanitizeRoom(room));
    } catch (err) {
      console.error('create_room fel:', err);
      callback({ success: false, error: 'Serverfel, försök igen.' });
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
        isHost: false
      };
      room.players.push(player);
      await saveRoom(room);

      socket.join(cleanId);
      socket.roomId = cleanId;

      console.log(`${playerName} gick med i rum ${cleanId}`);
      callback({ success: true, roomId: cleanId, player });
      io.to(cleanId).emit('room_update', sanitizeRoom(room));
    } catch (err) {
      console.error('join_room fel:', err);
      callback({ success: false, error: 'Serverfel, försök igen.' });
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

      room.status = 'playing';
      await saveRoom(room);
      callback({ success: true });
      io.to(roomId).emit('game_starting', sanitizeRoom(room));
    } catch (err) {
      console.error('start_game fel:', err);
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

const PORT = process.env.PORT || 3000;
initDB().then(() => {
  server.listen(PORT, () => {
    console.log(`Plump-servern körs på port ${PORT}`);
  });
}).catch(err => {
  console.error('Kunde inte initiera databas:', err);
  process.exit(1);
});
