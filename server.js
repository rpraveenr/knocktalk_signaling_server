const http = require('http');
const WebSocket = require('ws');
const express = require('express');
const path = require('path');
const fs = require('fs');
const admin = require('firebase-admin');

// Initialize Firebase Admin
const serviceAccount = require('./knocktalk-a918d-firebase-adminsdk-fbsvc-c575d888e1.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Keep connections alive
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => clearInterval(interval));

// rooms: roomId -> { host, guests[], guestReady, lastGuestJoinTime }
const rooms = new Map();
const fcmTokens = new Map();

// Persist FCM tokens across server restarts
const TOKENS_FILE = './fcm_tokens.json';
if (fs.existsSync(TOKENS_FILE)) {
  try {
    const saved = JSON.parse(fs.readFileSync(TOKENS_FILE, 'utf8'));
    Object.entries(saved).forEach(([roomId, token]) => fcmTokens.set(roomId, token));
    console.log('Loaded persisted FCM tokens:', [...fcmTokens.keys()]);
  } catch (e) {
    console.error('Failed to load FCM tokens:', e.message);
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', async (message) => {
    const data = JSON.parse(message);

    // Host registers FCM token
    if (data.type === 'register-token') {
      fcmTokens.set(data.roomId, data.fcmToken);
      // Persist to disk so token survives server restarts
      try {
        const saved = {};
        fcmTokens.forEach((token, roomId) => saved[roomId] = token);
        fs.writeFileSync(TOKENS_FILE, JSON.stringify(saved));
      } catch (e) {
        console.error('Failed to persist FCM token:', e.message);
      }
      console.log(`FCM token registered for room: ${data.roomId}`);
    }

    // Host app is open and listening
    if (data.type === 'host-listen') {
      const roomId = data.roomId;
      if (!rooms.has(roomId)) rooms.set(roomId, { host: null, guests: [], guestReady: false });
      const room = rooms.get(roomId);
      room.host = ws;
      ws.roomId = roomId;
      ws.role = 'host';
      console.log(`Host listening on room: ${roomId}`);
      ws.send(JSON.stringify({ type: 'listening' }));

      // If there's already a guest waiting, notify host immediately
      if (room.guests.length > 0) {
        console.log('Guest already waiting — notifying host');
        ws.send(JSON.stringify({ type: 'incoming-call', roomId }));
      }
    }

    // Host reconnected after answering call
    if (data.type === 'host-reconnect') {
      const roomId = data.roomId;
      const room = rooms.get(roomId);
      if (room) {
        room.host = ws;
        ws.roomId = roomId;
        ws.role = 'host';
        console.log(`Host reconnected to room: ${roomId}`);

        // If guest is already ready, send create-offer immediately
        if (room.guestReady) {
          console.log('Guest was ready — sending create-offer to reconnected host');
          ws.send(JSON.stringify({ type: 'create-offer' }));
        }
      }
    }

    // Guest scanned QR and joined
    if (data.type === 'guest-join') {
      const roomId = data.roomId;
      if (!rooms.has(roomId)) rooms.set(roomId, { host: null, guests: [], guestReady: false });

      const room = rooms.get(roomId);
      room.guests.push(ws);
      room.lastGuestJoinTime = Date.now();
      ws.roomId = roomId;
      ws.role = 'guest';
      console.log(`Guest joined room: ${roomId}`);
      ws.send(JSON.stringify({ type: 'waiting-for-host' }));

      // If host app is open, notify via WebSocket
      if (room.host && room.host.readyState === WebSocket.OPEN) {
        room.host.send(JSON.stringify({ type: 'incoming-call', roomId }));
      }

      // Send push notification
      const token = fcmTokens.get(roomId);
      if (token) {
        try {
          await admin.messaging().send({
            token: token,
            notification: {
              title: '🔔 Someone is at your door!',
              body: 'Tap to answer the video call',
            },
            data: {
              roomId: roomId,
              type: 'incoming-call',
            },
            android: {
              priority: 'high',
              notification: {
                sound: 'default',
                channelId: 'knocktalk_calls',
              }
            },
            apns: {
              payload: {
                aps: {
                  sound: 'default',
                  badge: 1,
                }
              }
            }
          });
          console.log('Push notification sent successfully');
        } catch (err) {
          console.error('Failed to send push notification:', err.message);
        }
      } else {
        console.log('No FCM token registered for room:', roomId);
      }
    }

    // Host answered — tell guest to proceed
    if (data.type === 'host-answer') {
      const room = rooms.get(data.roomId);
      if (room && room.guests.length > 0) {
        const guest = room.guests[room.guests.length - 1];
        if (guest && guest.readyState === WebSocket.OPEN) {
          guest.send(JSON.stringify({ type: 'call-accepted' }));
        }
      }
    }

    // Guest peer connection is ready — tell host to create offer
    if (data.type === 'guest-ready') {
      console.log('Guest is ready — telling host to create offer');
      const room = rooms.get(data.roomId);
      if (!room) return;

      room.guestReady = true;

      if (room.host && room.host.readyState === WebSocket.OPEN) {
        room.host.send(JSON.stringify({ type: 'create-offer' }));
      } else {
        console.log('Host not connected yet — will send when host reconnects');
      }
    }

    // Host is in CallScreen and ready — same connection, no reconnect needed
    if (data.type === 'host-answer-ready') {
      console.log('Host is ready in call screen');
      const room = rooms.get(data.roomId);
      if (room && room.guestReady) {
        console.log('Guest already ready — sending create-offer');
        ws.send(JSON.stringify({ type: 'create-offer' }));
      }
    }

    // Host ended the call — notify guest
    if (data.type === 'host-end-call') {
      console.log('Host ended the call');
      const room = rooms.get(data.roomId);
      if (room) {
        // Notify all guests
        room.guests.forEach(guest => {
          if (guest && guest.readyState === WebSocket.OPEN) {
            guest.send(JSON.stringify({ type: 'call-ended' }));
          }
        });
        // Full reset so next guest scan starts clean
        room.guestReady = false;
        room.guests = [];
        room.lastGuestJoinTime = 0;
      }
    }

    // Ping/pong keepalive
    if (data.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong' }));
    }

    // Forward WebRTC signals between host and guest
    if (data.type === 'signal') {
      const room = rooms.get(data.roomId);
      if (!room) {
        console.log('Signal received but no room found:', data.roomId);
        return;
      }

      console.log(`Signal from ${ws.role}: ${data.signal?.type || 'candidate'}`);

      if (ws.role === 'host') {
        const guest = room.guests[room.guests.length - 1];
        if (guest && guest.readyState === WebSocket.OPEN) {
          console.log('Forwarding signal to guest');
          guest.send(JSON.stringify(data));
        } else {
          console.log('No guest available to forward signal');
        }
      } else {
        if (room.host && room.host.readyState === WebSocket.OPEN) {
          console.log('Forwarding signal to host');
          room.host.send(JSON.stringify(data));
        } else {
          console.log('No host available to forward signal');
        }
      }
    }
  });

  ws.on('close', () => {
    if (!ws.roomId) return;
    const room = rooms.get(ws.roomId);
    if (!room) return;

    if (ws.role === 'host') {
      room.host = null;
      console.log(`Host disconnected from room: ${ws.roomId}`);
      // Wait 3 seconds before notifying guest — host may be navigating between screens
      setTimeout(() => {
        const currentRoom = rooms.get(ws.roomId);
        if (!currentRoom) return;
        if (!currentRoom.host) {
	  // Don't fire if a fresh guest joined after this timeout was scheduled
          const timeSinceGuestJoin = Date.now() - (currentRoom.lastGuestJoinTime || 0);
          if (timeSinceGuestJoin < 3500) {
            console.log('New guest joined during host reconnect window — skipping call-ended');
            return;
          }
          console.log('Host did not reconnect — notifying guest call ended');
          if (currentRoom.guests.length > 0) {
            const guest = currentRoom.guests[currentRoom.guests.length - 1];
            if (guest && guest.readyState === WebSocket.OPEN) {
              guest.send(JSON.stringify({ type: 'call-ended' }));
            }
          }
        } else {
          console.log('Host reconnected in time — call continues');
        }
      }, 3000);
    } else {
      room.guests = room.guests.filter(g => g !== ws);
      console.log(`Guest disconnected from room: ${ws.roomId}`);
      if (room.host && room.host.readyState === WebSocket.OPEN) {
        // S6: if guest left before the call was established (guestReady=false),
        // send guest-cancelled so WaitingScreen can handle it differently
        // (navigate to HomeScreen + show snackbar) vs mid-call disconnect
        const msgType = room.guestReady ? 'guest-disconnected' : 'guest-cancelled';
        console.log(`Notifying host: ${msgType}`);
        room.host.send(JSON.stringify({ type: msgType }));
      }
      room.guestReady = false;
    }
  });
});

server.listen(8080, () => {
  console.log('Server listening on port 8080');
});
