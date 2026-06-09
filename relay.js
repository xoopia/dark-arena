// 暗黑角斗场 v2.1 WebSocket 中继服务器
// 部署到 Render: npm start
// 本地测试: node relay.js [端口]
const crypto = require('crypto');
const WebSocket = require('ws');
const port = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port, host: '0.0.0.0' });

console.log('[relay] 已启动 :' + port);

// 房间: { "xk3ra9": [{ ws, id, createdAt }, ...] }
const rooms = {};

// crypto 真随机 6 位房间码
function generateRoomCode() {
  return crypto.randomBytes(3).toString('hex').slice(0, 6);
}

function generateId() {
  return crypto.randomBytes(4).toString('hex');
}

// P3: 僵尸房间清理 — 单人超 5 分钟未加入就解散
setInterval(function() {
  var now = Date.now();
  for (var code in rooms) {
    var room = rooms[code];
    if (room.length === 1 && now - room[0].createdAt > 5 * 60 * 1000) {
      try { room[0].ws.close(); } catch(e) {}
      delete rooms[code];
      console.log('[清理] 僵尸房间 ' + code);
    }
  }
}, 60000);

wss.on('connection', function(ws) {
  var myRoom = null, myId = null;

  ws.on('message', function(raw) {
    var data;
    try { data = JSON.parse(raw); } catch(e) { return; }

    // P2: 心跳保活，直接忽略
    if (data.type === 'ping') return;

    if (data.type === 'create') {
      var code;
      do { code = generateRoomCode(); } while (rooms[code]);
      myRoom = code;
      myId = generateId();
      rooms[code] = [{ ws: ws, id: myId, createdAt: Date.now() }];
      console.log('[创建] ' + code);
      ws.send(JSON.stringify({ type: 'created', room: code, role: 'host' }));

    } else if (data.type === 'join') {
      if (!data.room || !rooms[data.room] || rooms[data.room].length === 0) {
        ws.send(JSON.stringify({ type: 'error', msg: '房间不存在或已解散' }));
        return;
      }
      var room = rooms[data.room];
      if (room.length >= 2) {
        ws.send(JSON.stringify({ type: 'error', msg: '房间已满' }));
        return;
      }
      myRoom = data.room;
      myId = generateId();
      var guest = { ws: ws, id: myId, createdAt: Date.now() };
      room.push(guest);
      console.log('[加入] ' + myRoom + ': ' + room.length + '/2');
      // P1.2: 给双方都发 paired
      guest.ws.send(JSON.stringify({ type: 'paired', role: 'guest' }));
      room[0].ws.send(JSON.stringify({ type: 'paired', role: 'host' }));

    } else if (data.type === 'msg') {
      if (rooms[myRoom]) {
        rooms[myRoom].forEach(function(c) {
          if (c.id !== myId && c.ws.readyState === 1) {
            try { c.ws.send(JSON.stringify({ type: 'msg', data: data.data })); } catch(e) {}
          }
        });
      }
    }
  });

  ws.on('close', function() {
    if (rooms[myRoom]) {
      rooms[myRoom] = rooms[myRoom].filter(function(c) { return c.id !== myId; });
      if (rooms[myRoom].length === 0) {
        delete rooms[myRoom];
        console.log('[解散] ' + myRoom);
      } else {
        console.log('[离开] ' + myRoom + ': 1/2');
        rooms[myRoom].forEach(function(c) {
          try { c.ws.send(JSON.stringify({ type: 'peer_left' })); } catch(e) {}
        });
      }
    }
  });

  ws.on('error', function() {});
});
