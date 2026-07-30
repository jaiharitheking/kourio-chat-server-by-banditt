// ============================================
// Kour.io Chat Signaling Server
// Created by Banditt
// ============================================

const WebSocket = require('ws');
const server = new WebSocket.Server({ port: process.env.PORT || 3000 });

const clients = new Map();
let clientId = 0;
const messageStore = {};

console.log('🚀 Kour.Chat Server running on port', process.env.PORT || 3000);
console.log('👤 Server created by Banditt');

server.on('connection', (ws) => {
    const id = ++clientId;
    let userId = null;
    clients.set(id, ws);
    console.log(`✅ Client ${id} connected`);

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            console.log(`📩 ${msg.type} from ${msg.data?.from || id}`);

            if (msg.type === 'handshake') {
                userId = msg.data.id;
                console.log(`👤 User ${userId} connected`);
                
                if (messageStore[userId] && messageStore[userId].length > 0) {
                    console.log(`📦 Sending ${messageStore[userId].length} stored messages`);
                    for (const storedMsg of messageStore[userId]) {
                        ws.send(JSON.stringify({ type: 'stored_message', data: storedMsg }));
                    }
                    messageStore[userId] = [];
                }
                
                ws.send(JSON.stringify({ type: 'handshake_ack', data: { id: userId } }));
                
                for (const [cid, client] of clients) {
                    if (cid !== id && client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ type: 'peer_online', data: { id: userId } }));
                    }
                }
                return;
            }

            if (msg.type === 'message' || msg.type === 'typing' || msg.type === 'read_receipt' ||
                msg.type === 'poll_vote' || msg.type === 'group_update' || 
                msg.type === 'contact_request' || msg.type === 'contact_response' ||
                msg.type === 'status_update') {
                
                const recipient = msg.data.to;
                let recipientOnline = false;
                let recipientWs = null;

                for (const [cid, client] of clients) {
                    if (client.userId === recipient && client.readyState === WebSocket.OPEN) {
                        recipientOnline = true;
                        recipientWs = client;
                        break;
                    }
                }

                if (recipientOnline && recipientWs && msg.type !== 'status_update') {
                    recipientWs.send(JSON.stringify(msg));
                    console.log(`📤 Message sent to ${recipient} (online)`);
                } else if (msg.type === 'message') {
                    if (!messageStore[recipient]) messageStore[recipient] = [];
                    messageStore[recipient].push(msg.data);
                    console.log(`💾 Message stored for ${recipient} (offline)`);
                } else if (msg.type === 'status_update') {
                    for (const [cid, client] of clients) {
                        if (cid !== id && client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify(msg));
                        }
                    }
                } else {
                    for (const [cid, client] of clients) {
                        if (cid !== id && client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify(msg));
                        }
                    }
                }
            }

        } catch(e) {
            console.log('⚠️ Error:', e.message);
        }
    });

    ws.on('close', () => {
        clients.delete(id);
        if (userId) {
            console.log(`❌ User ${userId} disconnected`);
            for (const [cid, client] of clients) {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({ type: 'peer_offline', data: { id: userId } }));
                }
            }
        }
    });
});

setInterval(() => {
    console.log(`💓 Alive - ${clients.size} clients, ${Object.keys(messageStore).length} users have stored messages`);
}, 30000);
