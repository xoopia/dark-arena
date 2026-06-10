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

// P3: 僵尸房间清理 + P4: 断线宽限期清理
setInterval(function() {
  var now = Date.now();
  for (var code in rooms) {
    var room = rooms[code];
    // P4: 移除宽限期过期的断线玩家（3分钟）
    var removed = [];
    rooms[code] = room.filter(function(c) {
      if (c.disconnectedAt && now - c.disconnectedAt > 3 * 60 * 1000) {
        removed.push(c);
        try { c.ws.close(); } catch(e) {}
        console.log('[超时] 移除断线玩家 ' + code + ':' + c.id);
        return false;
      }
      return true;
    });
    room = rooms[code];

    if (room.length === 0) {
      delete rooms[code];
      console.log('[解散] ' + code);
    } else if (removed.length > 0) {
      // 通知剩余玩家：对手永久离开
      room.forEach(function(c) {
        if (!c.disconnectedAt && c.ws.readyState === 1) {
          try { c.ws.send(JSON.stringify({ type: 'peer_left' })); } catch(e) {}
        }
      });
    }

    // 僵尸房间清理：单人超5分钟且未断线
    if (room && room.length === 1 && !room[0].disconnectedAt && now - room[0].createdAt > 5 * 60 * 1000) {
      try { room[0].ws.close(); } catch(e) {}
      delete rooms[code];
      console.log('[清理] 僵尸房间 ' + code);
    }
  }
}, 30000);

wss.on('connection', function(ws) {
  var myRoom = null, myId = null;

  // P2: 心跳 — 60s 无消息断开
  ws._heartbeat = Date.now();
  ws._hbTimer = setInterval(function() {
    if (Date.now() - ws._heartbeat > 60000) {
      clearInterval(ws._hbTimer);
      try { ws.terminate(); } catch(e) {}
    }
  }, 15000);

  ws.on('message', function(raw) {
    var data;
    try { data = JSON.parse(raw); } catch(e) { return; }

    // P2: 心跳保活
    if (data.type === 'ping') { ws._heartbeat = Date.now(); return; }
    ws._heartbeat = Date.now();

    if (data.type === 'create') {
      var code;
      do { code = generateRoomCode(); } while (rooms[code]);
      myRoom = code;
      myId = generateId();
      rooms[code] = [{ ws: ws, id: myId, role: 'host', createdAt: Date.now(), disconnectedAt: null }];
      console.log('[创建] ' + code);
      ws.send(JSON.stringify({ type: 'created', room: code, role: 'host', id: myId }));

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
      var guest = { ws: ws, id: myId, role: 'guest', createdAt: Date.now(), disconnectedAt: null };
      room.push(guest);
      console.log('[加入] ' + myRoom + ': ' + room.length + '/2');
      // P1.2: 给双方都发 paired
      guest.ws.send(JSON.stringify({ type: 'paired', role: 'guest', id: myId }));
      room[0].ws.send(JSON.stringify({ type: 'paired', role: 'host', id: room[0].id }));

    } else if (data.type === 'leave') {
      // P4: 主动离开，立即移除
      if (rooms[myRoom]) {
        rooms[myRoom] = rooms[myRoom].filter(function(c) { return c.id !== myId; });
        if (rooms[myRoom].length === 0) {
          delete rooms[myRoom];
          console.log('[离开-解散] ' + myRoom);
        } else {
          console.log('[离开] ' + myRoom + ': 1/2');
          rooms[myRoom].forEach(function(c) {
            try { c.ws.send(JSON.stringify({ type: 'peer_left' })); } catch(e) {}
          });
        }
      }

    } else if (data.type === 'reconnect') {
      // P4: 断线重连 — 3分钟宽限期内允许重连
      if (!data.room || !data.id || !rooms[data.room]) {
        ws.send(JSON.stringify({ type: 'error', msg: '房间不存在或已解散' }));
        return;
      }
      var room = rooms[data.room];
      var peer = room.find(function(c) { return c.id === data.id; });
      if (!peer) {
        ws.send(JSON.stringify({ type: 'error', msg: '玩家不存在' }));
        return;
      }
      // 替换旧 ws，重新绑定
      try { peer.ws.close(); } catch(e) {}
      peer.ws = ws;
      peer.disconnectedAt = null;
      myRoom = data.room;
      myId = data.id;
      ws.send(JSON.stringify({ type: 'reconnected', role: peer.role }));
      // 通知对手
      room.forEach(function(c) {
        if (c.id !== myId && c.ws.readyState === 1) {
          try { c.ws.send(JSON.stringify({ type: 'peer_reconnected' })); } catch(e) {}
        }
      });
      console.log('[重连] ' + myRoom + ': ' + myId);

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
    clearInterval(ws._hbTimer);
    // P4: 断线宽限期 — 标记断开但不立即移除
    if (!myRoom || !rooms[myRoom]) return;
    var peer = rooms[myRoom].find(function(c) { return c.id === myId; });
    if (!peer) return;
    // 只处理当前 ws 的断开（防止旧连接覆盖新连接状态）
    if (peer.ws === ws) {
      peer.disconnectedAt = Date.now();
      console.log('[断开] ' + myRoom + ': ' + myId + ' (3分钟宽限期)');
      // 通知对手：暂时离开，非永久
      rooms[myRoom].forEach(function(c) {
        if (c.id !== myId && !c.disconnectedAt && c.ws.readyState === 1) {
          try { c.ws.send(JSON.stringify({ type: 'peer_away' })); } catch(e) {}
        }
      });
    }
  });

  ws.on('error', function() {});
});
