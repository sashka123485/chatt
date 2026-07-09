// server.js
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Хранилище данных
const connectedUsers = new Map(); // ws -> userData
const messageHistory = [];
const privateMessages = new Map(); // userId_userId -> [messages]
const MAX_HISTORY = 200;
const MAX_PRIVATE_HISTORY = 100;

// Раздаём статические файлы
app.use(express.static(path.join(__dirname, 'public')));

// WebSocket обработка
wss.on('connection', (ws) => {
  console.log('Новое подключение');
  
  let userData = {
    id: generateUserId(),
    name: 'Гость',
    connectedAt: Date.now()
  };
  
  // Отправляем новому пользователю его ID и историю сообщений
  ws.send(JSON.stringify({
    type: 'init',
    userId: userData.id,
    messages: messageHistory.slice(-50)
  }));
  
  // Отправляем список онлайн пользователей
  broadcastOnlineUsers();
  
  ws.on('message', (data) => {
    try {
      const parsed = JSON.parse(data);
      
      switch(parsed.type) {
        case 'setName':
          handleSetName(ws, parsed.name);
          break;
          
        case 'message':
          handleMessage(ws, parsed.text);
          break;
          
        case 'privateMessage':
          handlePrivateMessage(ws, parsed.toUserId, parsed.text);
          break;
          
        case 'typing':
          broadcastTyping(userData);
          break;
          
        case 'privateTyping':
          handlePrivateTyping(ws, parsed.toUserId);
          break;
          
        case 'getPrivateHistory':
          handleGetPrivateHistory(ws, parsed.withUserId);
          break;
          
        default:
          console.log('Неизвестный тип сообщения:', parsed.type);
      }
    } catch (error) {
      console.error('Ошибка обработки сообщения:', error);
    }
  });
  
  ws.on('close', () => {
    console.log(`Пользователь ${userData.name} отключился`);
    connectedUsers.delete(ws);
    broadcastUserLeft(userData);
    broadcastOnlineUsers();
  });
  
  ws.on('error', (error) => {
    console.error('WebSocket ошибка:', error);
    connectedUsers.delete(ws);
    broadcastOnlineUsers();
  });
  
  function handleSetName(ws, name) {
    const oldName = userData.name;
    userData.name = name.trim().substring(0, 30) || 'Гость';
    connectedUsers.set(ws, userData);
    
    // Уведомляем всех о смене имени
    broadcast({
      type: 'userRenamed',
      userId: userData.id,
      oldName: oldName,
      newName: userData.name
    });
    
    broadcastOnlineUsers();
  }
  
  function handleMessage(ws, text) {
    if (!text || text.trim().length === 0) return;
    
    const message = {
      id: generateMessageId(),
      userId: userData.id,
      userName: userData.name,
      text: text.trim().substring(0, 1000),
      timestamp: Date.now(),
      type: 'public'
    };
    
    messageHistory.push(message);
    if (messageHistory.length > MAX_HISTORY) {
      messageHistory.shift();
    }
    
    // Отправляем сообщение всем подключенным клиентам
    broadcast({
      type: 'newMessage',
      message: message
    });
  }
  
  function handlePrivateMessage(ws, toUserId, text) {
    if (!text || text.trim().length === 0) return;
    if (!toUserId) return;
    
    const message = {
      id: generateMessageId(),
      fromUserId: userData.id,
      fromUserName: userData.name,
      toUserId: toUserId,
      text: text.trim().substring(0, 1000),
      timestamp: Date.now(),
      type: 'private'
    };
    
    // Сохраняем в историю приватных сообщений
    const chatKey = getPrivateChatKey(userData.id, toUserId);
    if (!privateMessages.has(chatKey)) {
      privateMessages.set(chatKey, []);
    }
    privateMessages.get(chatKey).push(message);
    
    // Ограничиваем историю
    if (privateMessages.get(chatKey).length > MAX_PRIVATE_HISTORY) {
      privateMessages.get(chatKey).shift();
    }
    
    // Отправляем получателю
    const recipientWs = findUserByID(toUserId);
    if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
      recipientWs.send(JSON.stringify({
        type: 'privateMessage',
        message: message
      }));
    }
    
    // Отправляем отправителю для подтверждения
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'privateMessage',
        message: message
      }));
    }
  }
  
  function handlePrivateTyping(ws, toUserId) {
    const recipientWs = findUserByID(toUserId);
    if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
      recipientWs.send(JSON.stringify({
        type: 'privateTyping',
        fromUserId: userData.id,
        fromUserName: userData.name
      }));
    }
  }
  
  function handleGetPrivateHistory(ws, withUserId) {
    const chatKey = getPrivateChatKey(userData.id, withUserId);
    const history = privateMessages.get(chatKey) || [];
    
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'privateHistory',
        withUserId: withUserId,
        messages: history.slice(-50)
      }));
    }
  }
  
  function broadcastTyping(user) {
    broadcast({
      type: 'userTyping',
      userId: user.id,
      userName: user.name
    }, ws); // исключаем отправителя
  }
  
  function broadcastUserLeft(user) {
    broadcast({
      type: 'userLeft',
      userId: user.id,
      userName: user.name
    });
  }
  
  function broadcastOnlineUsers() {
    const users = Array.from(connectedUsers.values()).map(u => ({
      id: u.id,
      name: u.name
    }));
    
    broadcast({
      type: 'onlineUsers',
      users: users,
      count: users.length
    });
  }
  
  function broadcast(data, exclude) {
    const message = JSON.stringify(data);
    wss.clients.forEach(client => {
      if (client !== exclude && client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  }
  
  function findUserByID(userId) {
    for (let [ws, user] of connectedUsers) {
      if (user.id === userId) {
        return ws;
      }
    }
    return null;
  }
  
  function getPrivateChatKey(userId1, userId2) {
    return [userId1, userId2].sort().join('_');
  }
});

function generateUserId() {
  return 'user_' + Math.random().toString(36).substr(2, 9);
}

function generateMessageId() {
  return 'msg_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Сервер чата запущен на порту ${PORT}`);
  console.log(`📱 Откройте http://localhost:${PORT} в браузере`);
  console.log(`💬 Поддерживаются публичные и приватные сообщения`);
});