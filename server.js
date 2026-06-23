// server.js
require('dotenv').config();

const express = require('express');
const http = require('http');
const dns = require('dns');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const mongoose = require('mongoose');
const { Redis } = require('@upstash/redis');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');

const app = express();

// Some campus/home DNS servers refuse MongoDB Atlas SRV lookups.
dns.setServers(['8.8.8.8', '1.1.1.1']);

app.use(express.json({ limit: '10mb' }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

const JWT_SECRET =
  process.env.JWT_SECRET || 'local_chat_app_development_secret_key_987654321';

const localDataFile = path.join(__dirname, 'local-data.json');
const sessionControlFile = path.join(__dirname, 'session-control.json');

let globalLogoutAfter = 0;

function saveSessionControl() {
  try {
    fs.writeFileSync(
      sessionControlFile,
      JSON.stringify({ globalLogoutAfter }, null, 2)
    );
  } catch (err) {
    console.warn('[SESSION CONTROL SAVE ERROR]', err.message);
  }
}

function loadSessionControl() {
  try {
    if (!fs.existsSync(sessionControlFile)) return;

    const savedData = JSON.parse(fs.readFileSync(sessionControlFile, 'utf8'));
    globalLogoutAfter = Number(savedData.globalLogoutAfter) || 0;
  } catch (err) {
    console.warn('[SESSION CONTROL LOAD ERROR]', err.message);
  }
}

function getTokenIssuedAtMs(decoded) {
  if (Number.isFinite(decoded.authIssuedAtMs)) {
    return decoded.authIssuedAtMs;
  }

  return Number(decoded.iat || 0) * 1000;
}

function isTokenGloballyInvalidated(decoded) {
  return globalLogoutAfter > 0 && getTokenIssuedAtMs(decoded) <= globalLogoutAfter;
}

function saveLocalDbData() {
  try {
    fs.writeFileSync(
      localDataFile,
      JSON.stringify(
        {
          users: localDbMock.users,
          messages: localDbMock.messages,
          groups: localDbMock.groups
        },
        null,
        2
      )
    );
  } catch (err) {
    console.warn('[LOCAL DB SAVE ERROR]', err.message);
  }
}

function loadLocalDbData() {
  try {
    if (!fs.existsSync(localDataFile)) return;

    const savedData = JSON.parse(fs.readFileSync(localDataFile, 'utf8'));

    localDbMock.users = savedData.users || [];
    localDbMock.messages = savedData.messages || [];
    localDbMock.groups = savedData.groups || [];

    console.log('[LOCAL DB] Loaded local-data.json.');
  } catch (err) {
    console.warn('[LOCAL DB LOAD ERROR]', err.message);
  }
}

let isMongoConnected = false;
let isRedisConnected = false;
let mongoConnectPromise = Promise.resolve();
let redisConnectPromise = Promise.resolve();

function normalizeUsername(value) {
  return String(value || '').trim();
}

function validateUsername(value) {
  const username = normalizeUsername(value);

  if (username.length < 3 || username.length > 15) {
    return 'Username must be 3-15 characters long.';
  }

  return '';
}

function getRenamedDmRoomId(roomId, oldUsername, newUsername) {
  if (!roomId || !roomId.startsWith('dm_')) return roomId;

  const oldLower = oldUsername.toLowerCase();
  const members = roomId
    .replace('dm_', '')
    .split('_')
    .map(member => member.toLowerCase() === oldLower ? newUsername : member)
    .sort();

  return `dm_${members.join('_')}`;
}

// ================================
// LOCAL DATABASE MOCK
// ================================
const localDbMock = {
  users: [],
  messages: [],
  groups: [],

  async findUser(username) {
    return this.users.find(
      u => u.username.toLowerCase() === username.toLowerCase()
    );
  },

  async getAllUsers() {
    return this.users.map(u => ({
      id: u._id,
      username: u.username,
      avatar: u.avatar,
      bio: u.bio || 'Infinity Chat user',
      lastSeen: u.lastSeen || u.createdAt,
      createdAt: u.createdAt
    }));
  },

  async createUser(username, hashedPassword, avatar) {
    const newUser = {
      _id: 'mock_u_' + Math.random().toString(36).substr(2, 9),
      username,
      password: hashedPassword,
      avatar:
        avatar ||
        `https://api.dicebear.com/7.x/bottts/svg?seed=${username}&backgroundColor=ff2da6,f472b6,c026d3`,
      bio: 'Infinity Chat user',
      lastSeen: new Date(),
      createdAt: new Date()
    };

    this.users.push(newUser);
    saveLocalDbData();
    return newUser;
  },

  async updateUserProfile(userId, avatar, bio, username) {
    const user = this.users.find(u => u._id === userId);

    if (!user) return null;

    const oldUsername = user.username;

    if (avatar) user.avatar = avatar;
    if (bio !== undefined) user.bio = bio || 'Infinity Chat user';
    if (username && username !== oldUsername) {
      user.username = username;

      this.messages.forEach(message => {
        if (message.username === oldUsername) message.username = username;
        if (message.sender === oldUsername) message.sender = username;

        if (message.roomId?.startsWith('dm_')) {
          message.roomId = getRenamedDmRoomId(message.roomId, oldUsername, username);
        }

        if (Array.isArray(message.deletedFor)) {
          message.deletedFor = message.deletedFor.map(member =>
            member === oldUsername ? username : member
          );
        }

        Object.keys(message.reactions || {}).forEach(emoji => {
          message.reactions[emoji] = message.reactions[emoji].map(member =>
            member === oldUsername ? username : member
          );
        });
      });

      this.groups.forEach(group => {
        if (group.creator === oldUsername) group.creator = username;
        group.members = group.members.map(member =>
          member === oldUsername ? username : member
        );
      });
    }

    saveLocalDbData();
    return user;
  },

  async updateLastSeen(username) {
    const user = this.users.find(u => u.username === username);

    if (user) {
      user.lastSeen = new Date();
      saveLocalDbData();
    }

    return user;
  },

  async getRecentMessages(roomId = 'lounge', limit = 50) {
    return this.messages.filter(m => m.roomId === roomId).slice(-limit);
  },

  async saveMessage(sender, content, type = 'chat', roomId = 'lounge') {
    const user = this.users.find(u => u.username === sender);

    const newMsg = {
      _id: 'mock_m_' + Math.random().toString(36).substr(2, 9),
      type,
      roomId,
      username: sender,
      sender,
      avatar: user?.avatar || '',
      message: content,
      content,
      reactions: {},
      deletedFor: [],
      deletedForEveryone: false,
      status: 'sent',
      timestamp: new Date()
    };

    this.messages.push(newMsg);
    saveLocalDbData();
    return newMsg;
  },

  async createGroup(name, creator, members = [], avatar) {
    const newGroup = {
      _id: 'mock_g_' + Math.random().toString(36).substr(2, 9),
      name,
      creator,
      members: Array.from(new Set([creator, ...members])),
      avatar:
        avatar ||
        `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
          name
        )}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`,
      createdAt: new Date()
    };

    this.groups.push(newGroup);
    saveLocalDbData();
    return newGroup;
  },

  async getGroupsForUser(username) {
    return this.groups.filter(g => g.members.includes(username));
  },

  async leaveGroup(groupId, username) {
    const group = this.groups.find(g => g._id === groupId);

    if (group) {
      group.members = group.members.filter(m => m !== username);
      saveLocalDbData();
    }

    return group;
  },

  async updateGroupMembers(groupId, members) {
    const group = this.groups.find(g => g._id === groupId);

    if (group) {
      group.members = Array.from(new Set([group.creator, ...members]));
      saveLocalDbData();
    }

    return group;
  },

  async updateGroupProfile(groupId, updates = {}) {
    const group = this.groups.find(g => g._id === groupId);

    if (!group) return null;

    if (updates.name !== undefined) group.name = updates.name;
    if (updates.avatar !== undefined) group.avatar = updates.avatar;

    saveLocalDbData();
    return group;
  },

  async deleteGroup(groupId) {
    const group = this.groups.find(g => g._id === groupId);

    if (!group) return null;

    this.groups = this.groups.filter(g => g._id !== groupId);
    this.messages = this.messages.filter(m => m.roomId !== `group_${groupId}`);
    saveLocalDbData();

    return group;
  },

  async setMessageReaction(messageId, username, emoji) {
    const message = this.messages.find(m => m._id === messageId);

    if (!message) return null;

    message.reactions = message.reactions || {};

    Object.keys(message.reactions).forEach(key => {
      message.reactions[key] = message.reactions[key].filter(
        member => member !== username
      );

      if (message.reactions[key].length === 0) {
        delete message.reactions[key];
      }
    });

    if (emoji) {
      if (!message.reactions[emoji]) message.reactions[emoji] = [];
      message.reactions[emoji].push(username);
    }

    saveLocalDbData();
    return message;
  }
};

loadLocalDbData();
loadSessionControl();

// ================================
// MONGODB SETUP
// ================================
const mongoUri = process.env.MONGODB_URI;
const hasMongoUri = Boolean(mongoUri);

let User;
let Message;
let Group;

if (hasMongoUri) {
  console.log('[SYSTEM] Attempting to connect to MongoDB Atlas...');

  mongoConnectPromise = mongoose
    .connect(mongoUri)
    .then(() => {
      isMongoConnected = true;
      console.log('[DATABASE] Successfully connected to MongoDB Atlas!');
    })
    .catch(err => {
      console.warn(
        '[DATABASE] MongoDB connection failed. Falling back to in-memory database.',
        err.message
      );
    });

  const userSchema = new mongoose.Schema({
    username: {
      type: String,
      required: true,
      unique: true,
      lowercase: true,
      trim: true
    },
    password: { type: String, required: true },
    avatar: { type: String },
    bio: { type: String, default: 'Infinity Chat user' },
    lastSeen: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now }
  });

  const messageSchema = new mongoose.Schema({
    sender: { type: String, required: true },
    content: { type: String, required: true },
    type: { type: String, default: 'chat' },
    roomId: { type: String, default: 'lounge', index: true },
    reactions: {
      type: Map,
      of: [String],
      default: {}
    },
    deletedFor: [{ type: String }],
    deletedForEveryone: { type: Boolean, default: false },
    status: { type: String, default: 'sent' },
    timestamp: { type: Date, default: Date.now }
  });

  const groupSchema = new mongoose.Schema({
    name: { type: String, required: true },
    creator: { type: String, required: true },
    members: [{ type: String }],
    avatar: { type: String },
    createdAt: { type: Date, default: Date.now }
  });

  User = mongoose.model('User', userSchema);
  Message = mongoose.model('Message', messageSchema);
  Group = mongoose.model('Group', groupSchema);
} else {
  console.warn(
    '[SYSTEM] MONGODB_URI is not defined. Falling back to IN-MEMORY DATABASE.'
  );
}

async function useMongoUserStore(context = 'request') {
  if (!hasMongoUri) return false;

  await mongoConnectPromise;

  if (!isMongoConnected) {
    throw new Error(
      `[DATABASE] MongoDB is required for ${context} because MONGODB_URI is configured.`
    );
  }

  return true;
}

// ================================
// REDIS MOCK / SETUP
// ================================
const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

let redis;

const localRedisMock = {
  onlineUsers: new Set(),
  cachedMessages: {},

  async sadd(key, member) {
    this.onlineUsers.add(member);
    return 1;
  },

  async srem(key, member) {
    return this.onlineUsers.delete(member) ? 1 : 0;
  },

  async smembers(key) {
    return Array.from(this.onlineUsers);
  },

  async lrange(key, start, stop) {
    if (!this.cachedMessages[key]) return [];

    return this.cachedMessages[key].slice(
      start,
      stop === -1 ? undefined : stop + 1
    );
  },

  async rpush(key, value) {
    if (!this.cachedMessages[key]) this.cachedMessages[key] = [];

    this.cachedMessages[key].push(value);

    return this.cachedMessages[key].length;
  },

  async ltrim(key, start, stop) {
    if (!this.cachedMessages[key]) return 'OK';

    this.cachedMessages[key] = this.cachedMessages[key].slice(
      start,
      stop === -1 ? undefined : stop + 1
    );

    return 'OK';
  },

  async del(key) {
    if (key === 'online_users') {
      const count = this.onlineUsers.size;
      this.onlineUsers.clear();
      return count;
    }

    if (this.cachedMessages[key]) {
      delete this.cachedMessages[key];
      return 1;
    }

    return 0;
  }
};

if (redisUrl && redisToken) {
  redis = new Redis({
    url: redisUrl,
    token: redisToken
  });

  redisConnectPromise = redis
    .ping()
    .then(() => {
      isRedisConnected = true;
      console.log('[CACHE] Successfully connected to Upstash Redis!');
    })
    .catch(err => {
      redis = null;
      console.warn('[CACHE] Redis failed. Using local mock.', err.message);
    });
} else {
  console.warn('[SYSTEM] Redis credentials not found. Using local Redis mock.');
}

const getRedisClient = () => (isRedisConnected ? redis : localRedisMock);

// ================================
// AUTH MIDDLEWARE
// ================================
const authenticateToken = async (req, res, next) => {
  let token = req.cookies.token;

  if (!token && req.headers.authorization) {
    const authHeader = req.headers.authorization;

    if (authHeader.startsWith('Bearer ')) {
      token = authHeader.split(' ')[1];
    }
  }

  if (!token) {
    return res.status(401).json({
      error: 'Access denied. No token provided.'
    });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);

    if (isTokenGloballyInvalidated(decoded)) {
      return res.status(403).json({
        error: 'Session was reset. Please log in again.'
      });
    }

    req.user = decoded;
    next();
  } catch (err) {
    return res.status(403).json({
      error: 'Invalid or expired authentication token.'
    });
  }
};

function createUserToken(user) {
  return jwt.sign(
    {
      id: user._id,
      username: user.username,
      authIssuedAtMs: Date.now()
    },
    JWT_SECRET,
    { expiresIn: '7d' }
  );
}

function userResponse(user) {
  return {
    id: user._id,
    username: user.username,
    avatar: user.avatar,
    bio: user.bio || 'Infinity Chat user',
    lastSeen: user.lastSeen || user.createdAt,
    createdAt: user.createdAt
  };
}

// ================================
// AUTH ROUTES
// ================================
app.post('/api/auth/signup', async (req, res) => {
  try {
    let { username, password, avatar } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        error: 'Username and password are required.'
      });
    }

    username = username.trim();

    if (username.length < 3 || username.length > 15) {
      return res.status(400).json({
        error: 'Username must be between 3 and 15 characters.'
      });
    }

    let existingUser;

    if (await useMongoUserStore('signup')) {
      existingUser = await User.findOne({
        username: username.toLowerCase()
      });
    } else {
      existingUser = await localDbMock.findUser(username);
    }

    if (existingUser) {
      return res.status(400).json({
        error: 'Username is already taken.'
      });
    }

    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(password, salt);

    if (!avatar) {
      avatar = `https://api.dicebear.com/7.x/bottts/svg?seed=${encodeURIComponent(
        username
      )}&backgroundColor=ff2da6,f472b6,c026d3`;
    }

    let user;

    if (await useMongoUserStore('signup')) {
      user = new User({
        username,
        password: hashedPassword,
        avatar,
        bio: 'Infinity Chat user',
        lastSeen: new Date()
      });

      await user.save();
    } else {
      user = await localDbMock.createUser(username, hashedPassword, avatar);
    }

    const token = createUserToken(user);

    res.cookie('token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    res.status(201).json({
      success: true,
      token,
      user: userResponse(user)
    });
  } catch (error) {
    console.error('[SIGNUP ERROR]', error);

    res.status(500).json({
      error: 'An internal server error occurred during registration.'
    });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    let { username, password } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        error: 'Username and password are required.'
      });
    }

    username = username.trim();

    let user;

    if (await useMongoUserStore('login')) {
      user = await User.findOne({
        username: username.toLowerCase()
      });
    } else {
      user = await localDbMock.findUser(username);
    }

    if (!user) {
      return res.status(400).json({
        error: 'Invalid username or password.'
      });
    }

    const isMatch = await bcrypt.compare(password, user.password);

    if (!isMatch) {
      return res.status(400).json({
        error: 'Invalid username or password.'
      });
    }

    user.lastSeen = new Date();

    if (await useMongoUserStore('login')) {
      await user.save();
    }

    const token = createUserToken(user);

    res.cookie('token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    res.status(200).json({
      success: true,
      token,
      user: userResponse(user)
    });
  } catch (error) {
    console.error('[LOGIN ERROR]', error);

    res.status(500).json({
      error: 'An internal server error occurred during login.'
    });
  }
});

app.get('/api/auth/me', authenticateToken, async (req, res) => {
  try {
    let user;

    if (await useMongoUserStore('auth/me')) {
      user = await User.findById(req.user.id);
    } else {
      user = localDbMock.users.find(u => u._id === req.user.id);
    }

    if (!user) {
      return res.status(404).json({
        error: 'User not found.'
      });
    }

    res.status(200).json({
      success: true,
      user: userResponse(user)
    });
  } catch (err) {
    res.status(500).json({
      error: 'Failed to get current user.'
    });
  }
});

app.post('/api/auth/logout', async (req, res) => {
  try {
    let token = req.cookies.token;

    if (!token && req.headers.authorization) {
      const authHeader = req.headers.authorization;

      if (authHeader.startsWith('Bearer ')) {
        token = authHeader.split(' ')[1];
      }
    }

    if (token) {
      const decoded = jwt.verify(token, JWT_SECRET);

      if (await useMongoUserStore('logout')) {
        await User.findByIdAndUpdate(decoded.id, {
          lastSeen: new Date()
        });
      } else {
        await localDbMock.updateLastSeen(decoded.username);
      }
    }
  } catch (err) {
    console.warn('[LOGOUT LASTSEEN ERROR]', err.message);
  }

  res.clearCookie('token');

  res.status(200).json({
    success: true,
    message: 'Logged out successfully.'
  });
});

function isGlobalLogoutAuthorized(req) {
  const expectedKey = process.env.ADMIN_LOGOUT_KEY || process.env.JWT_SECRET || JWT_SECRET;
  const providedKey = req.headers['x-admin-key'];

  return typeof providedKey === 'string' && providedKey === expectedKey;
}

app.post('/api/auth/logout-all', async (req, res) => {
  if (!isGlobalLogoutAuthorized(req)) {
    return res.status(403).json({
      error: 'Admin logout key is required.'
    });
  }

  globalLogoutAfter = Date.now();
  saveSessionControl();

  const logoutResult = await forceLogoutAllActiveClients();

  res.clearCookie('token');

  res.status(200).json({
    success: true,
    message: 'All users were logged out successfully.',
    invalidatedAt: new Date(globalLogoutAfter).toISOString(),
    ...logoutResult
  });
});

// ================================
// PROFILE ROUTE
// ================================
app.put('/api/profile', authenticateToken, async (req, res) => {
  try {
    const { avatar, bio } = req.body;
    const requestedUsername = req.body.username !== undefined
      ? normalizeUsername(req.body.username)
      : '';

    let user;
    let oldUsername = req.user.username;
    let usernameChanged = false;

    if (await useMongoUserStore('profile update')) {
      user = await User.findById(req.user.id);

      if (!user) {
        return res.status(404).json({
          error: 'User not found.'
        });
      }

      oldUsername = user.username;

      if (requestedUsername && requestedUsername !== user.username) {
        const usernameError = validateUsername(requestedUsername);

        if (usernameError) {
          return res.status(400).json({ error: usernameError });
        }

        const existingUser = await User.findOne({
          username: requestedUsername.toLowerCase(),
          _id: { $ne: user._id }
        });

        if (existingUser) {
          return res.status(409).json({
            error: 'Username is already taken.'
          });
        }

        user.username = requestedUsername;
        usernameChanged = true;
      }

      if (avatar) user.avatar = avatar;
      if (bio !== undefined) user.bio = bio || 'Infinity Chat user';

      await user.save();

      if (usernameChanged) {
        const newUsername = user.username;

        const messages = await Message.find({
          $or: [
            { sender: oldUsername },
            { roomId: { $regex: `^dm_` } },
            { deletedFor: oldUsername }
          ]
        });

        for (const message of messages) {
          if (message.sender === oldUsername) message.sender = newUsername;
          if (message.roomId?.startsWith('dm_')) {
            message.roomId = getRenamedDmRoomId(message.roomId, oldUsername, newUsername);
          }
          if (Array.isArray(message.deletedFor)) {
            message.deletedFor = message.deletedFor.map(member =>
              member === oldUsername ? newUsername : member
            );
          }

          const reactions = Object.fromEntries(message.reactions || []);
          let reactionsChanged = false;

          Object.keys(reactions).forEach(emoji => {
            reactions[emoji] = reactions[emoji].map(member => {
              if (member === oldUsername) {
                reactionsChanged = true;
                return newUsername;
              }
              return member;
            });
          });

          if (reactionsChanged) message.reactions = reactions;
          await message.save();
        }

        await Group.updateMany(
          { creator: oldUsername },
          { $set: { creator: newUsername } }
        );
        await Group.updateMany(
          { members: oldUsername },
          { $set: { 'members.$': newUsername } }
        );
      }
    } else {
      if (requestedUsername && requestedUsername !== req.user.username) {
        const usernameError = validateUsername(requestedUsername);

        if (usernameError) {
          return res.status(400).json({ error: usernameError });
        }

        const existingUser = await localDbMock.findUser(requestedUsername);

        if (existingUser && existingUser._id !== req.user.id) {
          return res.status(409).json({
            error: 'Username is already taken.'
          });
        }

        usernameChanged = true;
      }

      user = await localDbMock.updateUserProfile(
        req.user.id,
        avatar,
        bio,
        requestedUsername || ''
      );

      if (!user) {
        return res.status(404).json({
          error: 'User not found.'
        });
      }
    }

    if (usernameChanged) {
      const newUsername = user.username;
      const client = getRedisClient();

      await client.srem('online_users', oldUsername);
      await client.sadd('online_users', newUsername);
    }

    const token = createUserToken(user);

    res.cookie('token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    const updatedUser = userResponse(user);

    const profilePayload = JSON.stringify({
      type: 'profile_updated',
      user: updatedUser
    });

    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(profilePayload);
      }
    });

    if (usernameChanged) {
      await broadcastOnlineUsers();
    }

    res.json({
      success: true,
      token,
      user: updatedUser
    });
  } catch (err) {
    console.error('[PROFILE UPDATE ERROR]', err);

    res.status(500).json({
      error: 'Failed to update profile.'
    });
  }
});

// ================================
// USERS ROUTE
// ================================
app.get('/api/users', authenticateToken, async (req, res) => {
  try {
    console.log(
      `[GET /api/users] Called by ${req.user.username || req.user.id}. Source: ${
        hasMongoUri ? 'MongoDB' : 'local-data.json'
      }`
    );

    let usersList = [];

    if (await useMongoUserStore('/api/users')) {
      const dbUsers = await User.find(
        {
          username: { $ne: String(req.user.username || '').toLowerCase() }
        },
        'username avatar bio lastSeen createdAt'
      ).sort({ username: 1 });

      usersList = dbUsers.map(userResponse);
    } else {
      usersList = (await localDbMock.getAllUsers())
        .filter(user =>
          String(user.id) !== String(req.user.id) &&
          user.username.toLowerCase() !== String(req.user.username || '').toLowerCase()
        )
        .sort((a, b) => a.username.localeCompare(b.username));
    }

    console.log(`[GET /api/users] Found ${usersList.length} users.`);

    res.status(200).json({
      success: true,
      users: usersList
    });
  } catch (error) {
    console.error('[GET USERS ERROR]', error);

    res.status(500).json({
      error: 'Failed to retrieve users.'
    });
  }
});

// ================================
// GROUP ROUTES
// ================================
app.get('/api/groups', authenticateToken, async (req, res) => {
  try {
    let groupsList = [];

    if (isMongoConnected) {
      groupsList = await Group.find({
        members: req.user.username
      });
    } else {
      groupsList = await localDbMock.getGroupsForUser(req.user.username);
    }

    res.status(200).json({
      success: true,
      groups: groupsList
    });
  } catch (error) {
    console.error('[GET GROUPS ERROR]', error);

    res.status(500).json({
      error: 'Failed to retrieve groups.'
    });
  }
});

app.post('/api/groups', authenticateToken, async (req, res) => {
  try {
    const { name, members } = req.body;

    if (!name || !name.trim()) {
      return res.status(400).json({
        error: 'Group name is required.'
      });
    }

    if (!members || members.length === 0) {
      return res.status(400).json({
        error: 'At least one member must be selected to create a group.'
      });
    }

    const creator = req.user.username;

    const finalMembers = Array.from(
      new Set([creator, ...members])
    );

    const groupAvatar =
      `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
        name.trim()
      )}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`;

    let newGroup;

    if (isMongoConnected) {
      newGroup = new Group({
        name: name.trim(),
        creator,
        members: finalMembers,
        avatar: groupAvatar
      });

      await newGroup.save();
    } else {
      newGroup = await localDbMock.createGroup(
        name.trim(),
        creator,
        finalMembers,
        groupAvatar
      );
    }

    const groupCreatedPayload = JSON.stringify({
      type: 'group_created',
      group: newGroup
    });

    finalMembers.forEach(memberUsername => {
      const socketSet = userSockets.get(memberUsername);

      if (socketSet) {
        socketSet.forEach(s => {
          if (s.readyState === WebSocket.OPEN) {
            s.send(groupCreatedPayload);
          }
        });
      }
    });

    res.status(201).json({
      success: true,
      group: newGroup
    });
  } catch (error) {
    console.error('[CREATE GROUP ERROR]', error);

    res.status(500).json({
      error: 'Failed to create group.'
    });
  }
});

app.post('/api/groups/:id/leave', authenticateToken, async (req, res) => {
  try {
    const groupId = req.params.id;
    const username = req.user.username;

    let group;

    if (isMongoConnected) {
      group = await Group.findById(groupId);

      if (group) {
        if (group.creator === username) {
          return res.status(400).json({
            error: 'Group creator cannot leave.'
          });
        }

        group.members = group.members.filter(m => m !== username);

        await group.save();
      }
    } else {
      const tempGroup = localDbMock.groups.find(g => g._id === groupId);

      if (tempGroup && tempGroup.creator === username) {
        return res.status(400).json({
          error: 'Group creator cannot leave.'
        });
      }

      group = await localDbMock.leaveGroup(groupId, username);
    }

    if (!group) {
      return res.status(404).json({
        error: 'Group not found.'
      });
    }

    const leavePayload = JSON.stringify({
      type: 'group_updated',
      group
    });

    group.members.forEach(member => {
      const socketSet = userSockets.get(member);

      if (socketSet) {
        socketSet.forEach(s => {
          if (s.readyState === WebSocket.OPEN) {
            s.send(leavePayload);
          }
        });
      }
    });

    res.status(200).json({
      success: true,
      group
    });
  } catch (err) {
    console.error('[LEAVE GROUP ERROR]', err);

    res.status(500).json({
      error: 'Failed to leave group.'
    });
  }
});

app.put('/api/groups/:id/members', authenticateToken, async (req, res) => {
  try {
    const groupId = req.params.id;
    const { members } = req.body;
    const username = req.user.username;
    let previousMembers = [];

    if (!members || members.length === 0) {
      return res.status(400).json({
        error: 'Group must have at least one member.'
      });
    }

    let group;

    if (isMongoConnected) {
      group = await Group.findById(groupId);

      if (group) {
        previousMembers = [...group.members];

        if (group.creator !== username) {
          return res.status(403).json({
            error: 'Only the creator can manage members.'
          });
        }

        group.members = Array.from(
          new Set([group.creator, ...members])
        );

        await group.save();
      }
    } else {
      const tempGroup = localDbMock.groups.find(g => g._id === groupId);

      if (tempGroup && tempGroup.creator !== username) {
        return res.status(403).json({
          error: 'Only the creator can manage members.'
        });
      }

      previousMembers = tempGroup ? [...tempGroup.members] : [];
      group = await localDbMock.updateGroupMembers(groupId, members);
    }

    if (!group) {
      return res.status(404).json({
        error: 'Group not found.'
      });
    }

    const updatePayload = JSON.stringify({
      type: 'group_updated',
      group
    });

    Array.from(new Set([...previousMembers, ...group.members])).forEach(member => {
      const socketSet = userSockets.get(member);

      if (socketSet) {
        socketSet.forEach(s => {
          if (s.readyState === WebSocket.OPEN) {
            s.send(updatePayload);
          }
        });
      }
    });

    res.status(200).json({
      success: true,
      group
    });
  } catch (err) {
    console.error('[UPDATE MEMBERS ERROR]', err);

    res.status(500).json({
      error: 'Failed to update members.'
    });
  }
});

app.put('/api/groups/:id/profile', authenticateToken, async (req, res) => {
  try {
    const groupId = req.params.id;
    const { name, avatar } = req.body;
    const username = req.user.username;
    let previousMembers = [];

    let nextName = typeof name === 'string' ? name.trim() : '';
    let nextAvatar = typeof avatar === 'string' ? avatar.trim() : '';

    if (!nextName && !nextAvatar) {
      return res.status(400).json({
        error: 'Provide at least one profile field to update.'
      });
    }

    let group;

    if (isMongoConnected) {
      if (!mongoose.Types.ObjectId.isValid(groupId)) {
        return res.status(404).json({
          error: 'Group not found.'
        });
      }

      group = await Group.findById(groupId);

      if (group) {
        previousMembers = [...group.members];

        if (!group.members.includes(username)) {
          return res.status(403).json({
            error: 'Only group members can change the group profile.'
          });
        }

        if (nextName) {
          group.name = nextName;
          if (!nextAvatar && !group.avatar) {
            nextAvatar = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(nextName)}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`;
          }
        }

        if (nextAvatar) {
          group.avatar = nextAvatar;
        }

        await group.save();
      }
    } else {
      const tempGroup = localDbMock.groups.find(g => g._id === groupId);

      if (tempGroup) {
        previousMembers = [...tempGroup.members];

        if (!tempGroup.members.includes(username)) {
          return res.status(403).json({
            error: 'Only group members can change the group profile.'
          });
        }

        if (nextName) {
          tempGroup.name = nextName;
          if (!nextAvatar && !tempGroup.avatar) {
            nextAvatar = `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(nextName)}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`;
          }
        }

        if (nextAvatar) {
          tempGroup.avatar = nextAvatar;
        }

        group = await localDbMock.updateGroupProfile(groupId, {
          name: nextName || tempGroup.name,
          avatar: nextAvatar || tempGroup.avatar
        });
      }
    }

    if (!group) {
      return res.status(404).json({
        error: 'Group not found.'
      });
    }

    const updatePayload = JSON.stringify({
      type: 'group_updated',
      group
    });

    Array.from(new Set([...previousMembers, ...group.members])).forEach(member => {
      const socketSet = userSockets.get(member);

      if (socketSet) {
        socketSet.forEach(socket => {
          if (socket.readyState === WebSocket.OPEN) {
            socket.send(updatePayload);
          }
        });
      }
    });

    res.status(200).json({
      success: true,
      group
    });
  } catch (error) {
    console.error('[UPDATE GROUP PROFILE ERROR]', error);

    res.status(500).json({
      error: 'Failed to update group profile.'
    });
  }
});

app.delete('/api/groups/:id', authenticateToken, async (req, res) => {
  try {
    const groupId = req.params.id;
    const username = req.user.username;
    let group;
    let membersToNotify = [];

    if (isMongoConnected) {
      if (!mongoose.Types.ObjectId.isValid(groupId)) {
        return res.status(404).json({
          error: 'Group not found.'
        });
      }

      group = await Group.findById(groupId);

      if (group) {
        if (group.creator !== username) {
          return res.status(403).json({
            error: 'Only the group admin can delete this group.'
          });
        }

        membersToNotify = [...group.members];
        await Message.deleteMany({ roomId: `group_${groupId}` });
        await Group.deleteOne({ _id: groupId });
      }
    } else {
      const tempGroup = localDbMock.groups.find(g => g._id === groupId);

      if (tempGroup && tempGroup.creator !== username) {
        return res.status(403).json({
          error: 'Only the group admin can delete this group.'
        });
      }

      membersToNotify = tempGroup ? [...tempGroup.members] : [];
      group = await localDbMock.deleteGroup(groupId);
    }

    if (!group) {
      return res.status(404).json({
        error: 'Group not found.'
      });
    }

    const deletePayload = {
      type: 'group_deleted',
      groupId,
      group
    };

    await Promise.all(
      Array.from(new Set(membersToNotify)).map(member =>
        sendToUser(member, deletePayload)
      )
    );

    res.status(200).json({
      success: true,
      groupId
    });
  } catch (err) {
    console.error('[DELETE GROUP ERROR]', err);

    res.status(500).json({
      error: 'Failed to delete group.'
    });
  }
});

// ================================
// WEBSOCKET SERVER
// ================================
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const userSockets = new Map();

wss.on('error', err => {
  if (err.code === 'EADDRINUSE') {
    console.error(
      `[SYSTEM] WebSocket port ${PORT} is already in use. Run npm run restart, or stop the old server first.`
    );
    process.exit(1);
  }

  console.error('[WEBSOCKET SERVER ERROR]', err);
});

async function getUserAvatar(username) {
  try {
    if (isMongoConnected) {
      const user = await User.findOne({ username: username.toLowerCase() });
      return user?.avatar || '';
    }

    const user = await localDbMock.findUser(username);
    return user?.avatar || '';
  } catch {
    return '';
  }
}

async function getGroupMembersByRoom(roomId) {
  const groupId = roomId.replace('group_', '');

  if (isMongoConnected) {
    if (!mongoose.Types.ObjectId.isValid(groupId)) return [];

    const group = await Group.findById(groupId);
    return group ? group.members : [];
  }

  const group = localDbMock.groups.find(g => g._id === groupId);
  return group ? group.members : [];
}

function getDmMembers(roomId) {
  return roomId.replace('dm_', '').split('_');
}

async function sendToUser(username, payload) {
  const socketSet = userSockets.get(username);

  if (!socketSet) return;

  socketSet.forEach(socket => {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(payload));
    }
  });
}

async function broadcastOnlineUsers() {
  try {
    const client = getRedisClient();
    const onlineUsers = await client.smembers('online_users');

    const onlineUserList = [];

    for (const username of onlineUsers) {
      const avatar = await getUserAvatar(username);

      onlineUserList.push({
        username,
        avatar:
          avatar ||
          `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
            username
          )}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`
      });
    }

    const payload = JSON.stringify({
      type: 'online_list',
      users: onlineUserList,
      count: onlineUserList.length
    });

    wss.clients.forEach(clientSocket => {
      if (clientSocket.readyState === WebSocket.OPEN) {
        clientSocket.send(payload);
      }
    });
  } catch (err) {
    console.error('[PRESENCE BROADCAST ERROR]', err);
  }
}

async function updateLastSeenForOnlineUsers(usernames) {
  await Promise.all(
    usernames.map(async username => {
      try {
        if (isMongoConnected) {
          await User.findOneAndUpdate(
            { username: username.toLowerCase() },
            { lastSeen: new Date() }
          );
        } else {
          await localDbMock.updateLastSeen(username);
        }
      } catch (err) {
        console.warn(`[FORCE LOGOUT LASTSEEN ERROR] ${username}:`, err.message);
      }
    })
  );
}

async function clearOnlineUsersCache() {
  const client = getRedisClient();

  try {
    if (typeof client.del === 'function') {
      await client.del('online_users');
      return;
    }

    const onlineUsers = await client.smembers('online_users');
    await Promise.all(
      onlineUsers.map(username => client.srem('online_users', username))
    );
  } catch (err) {
    console.warn('[FORCE LOGOUT PRESENCE CLEAR ERROR]', err.message);
  }
}

async function forceLogoutAllActiveClients() {
  const activeUsernames = Array.from(userSockets.keys());
  const payload = JSON.stringify({
    type: 'force_logout',
    message: 'All sessions were reset. Please sign in again.'
  });

  let closedConnections = 0;

  wss.clients.forEach(clientSocket => {
    if (
      clientSocket.readyState !== WebSocket.OPEN &&
      clientSocket.readyState !== WebSocket.CONNECTING
    ) {
      return;
    }

    closedConnections += 1;

    if (clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.send(payload, () => {
        clientSocket.close(4000, 'Logged out by administrator');
      });
      return;
    }

    clientSocket.close(4000, 'Logged out by administrator');
  });

  await updateLastSeenForOnlineUsers(activeUsernames);
  userSockets.clear();
  await clearOnlineUsersCache();
  await broadcastOnlineUsers();

  return {
    activeUsersLoggedOut: activeUsernames.length,
    activeConnectionsClosed: closedConnections
  };
}

async function getHistory(roomId, limit = 50, viewerUsername = '') {
  if (isMongoConnected) {
    const dbMsgs = await Message.find({ roomId })
      .sort({ timestamp: -1 })
      .limit(limit);

    return dbMsgs.reverse().map(m => ({
      _id: m._id,
      type: 'chat',
      roomId: m.roomId,
      username: m.sender,
      sender: m.sender,
      message: m.content,
      content: m.content,
      reactions: Object.fromEntries(m.reactions || []),
      deletedForEveryone: Boolean(m.deletedForEveryone),
      deletedForMe: (m.deletedFor || []).includes(viewerUsername),
      timestamp: m.timestamp,
      status: m.status || 'sent',
      avatar: ''
    }));
  }

  const messages = await localDbMock.getRecentMessages(roomId, limit);

  return messages.map(message => ({
    ...message,
    deletedForEveryone: Boolean(message.deletedForEveryone),
    deletedForMe: (message.deletedFor || []).includes(viewerUsername)
  }));
}

async function saveChatMessage(username, content, roomId) {
  if (isMongoConnected) {
    const message = new Message({
      sender: username,
      content,
      roomId,
      status: 'sent'
    });

    await message.save();

    return {
      _id: message._id,
      type: 'chat',
      roomId,
      username,
      sender: username,
      avatar: await getUserAvatar(username),
      message: content,
      content,
      reactions: {},
      deletedFor: [],
      deletedForEveryone: false,
      timestamp: message.timestamp,
      status: 'sent'
    };
  }

  return await localDbMock.saveMessage(
    username,
    content,
    'chat',
    roomId
  );
}

async function setMessageReaction(messageId, username, emoji) {
  if (isMongoConnected) {
    const message = await Message.findById(messageId);

    if (!message) return null;

    const reactions = Object.fromEntries(message.reactions || []);

    Object.keys(reactions).forEach(key => {
      reactions[key] = reactions[key].filter(member => member !== username);

      if (reactions[key].length === 0) {
        delete reactions[key];
      }
    });

    if (emoji) {
      if (!reactions[emoji]) reactions[emoji] = [];
      reactions[emoji].push(username);
    }

    message.reactions = reactions;
    await message.save();

    return {
      _id: message._id,
      roomId: message.roomId,
      reactions
    };
  }

  const message = await localDbMock.setMessageReaction(
    messageId,
    username,
    emoji
  );

  return message
    ? {
        _id: message._id,
        roomId: message.roomId,
        reactions: message.reactions || {}
      }
    : null;
}

async function markMessageDeleted(messageId, username, mode) {
  if (isMongoConnected) {
    const message = await Message.findById(messageId);

    if (!message) return null;

    if (mode === 'everyone') {
      if (message.sender !== username) return null;

      message.deletedForEveryone = true;
      message.reactions = {};
    } else {
      message.deletedFor = Array.from(
        new Set([...(message.deletedFor || []), username])
      );
    }

    await message.save();

    return {
      _id: message._id,
      roomId: message.roomId,
      mode,
      username
    };
  }

  const message = localDbMock.messages.find(m => String(m._id) === String(messageId));

  if (!message) return null;

  if (mode === 'everyone') {
    if (message.sender !== username && message.username !== username) return null;

    message.deletedForEveryone = true;
    message.reactions = {};
  } else {
    message.deletedFor = Array.from(
      new Set([...(message.deletedFor || []), username])
    );
  }

  saveLocalDbData();

  return {
    _id: message._id,
    roomId: message.roomId,
    mode,
    username
  };
}

async function routePayloadToRoom(roomId, payload) {
  if (roomId === 'lounge') {
    wss.clients.forEach(clientSocket => {
      if (clientSocket.readyState === WebSocket.OPEN) {
        clientSocket.send(JSON.stringify(payload));
      }
    });

    return;
  }

  if (roomId.startsWith('dm_')) {
    const members = getDmMembers(roomId);

    members.forEach(username => {
      sendToUser(username, payload);
    });

    return;
  }

  if (roomId.startsWith('group_')) {
    const members = await getGroupMembersByRoom(roomId);

    members.forEach(username => {
      sendToUser(username, payload);
    });
  }
}

async function routeTypingToRoom(roomId, senderUsername) {
  const typingPayload = {
    type: 'typing',
    roomId,
    username: senderUsername
  };

  if (roomId === 'lounge') {
    wss.clients.forEach(clientSocket => {
      if (
        clientSocket.readyState === WebSocket.OPEN &&
        clientSocket.username !== senderUsername
      ) {
        clientSocket.send(JSON.stringify(typingPayload));
      }
    });

    return;
  }

  if (roomId.startsWith('dm_')) {
    const members = getDmMembers(roomId);

    members.forEach(username => {
      if (username !== senderUsername) {
        sendToUser(username, typingPayload);
      }
    });

    return;
  }

  if (roomId.startsWith('group_')) {
    const members = await getGroupMembersByRoom(roomId);

    members.forEach(username => {
      if (username !== senderUsername) {
        sendToUser(username, typingPayload);
      }
    });
  }
}

wss.on('connection', async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split('?')[1]);
  const token = urlParams.get('token');

  let decodedUser;

  try {
    if (!token) throw new Error('No authentication token provided.');
    decodedUser = jwt.verify(token, JWT_SECRET);

    if (isTokenGloballyInvalidated(decodedUser)) {
      throw new Error('Session was reset. Please log in again.');
    }
  } catch (err) {
    ws.send(
      JSON.stringify({
        type: 'system',
        message: 'Authentication failed. Closing connection.'
      })
    );

    ws.close(4001, 'Unauthorized');
    return;
  }

  const { username, avatar } = decodedUser;

  ws.username = username;
  ws.avatar = avatar;

  if (!userSockets.has(username)) {
    userSockets.set(username, new Set());
  }

  userSockets.get(username).add(ws);

  const client = getRedisClient();
  await client.sadd('online_users', username);

  ws.send(
    JSON.stringify({
      type: 'system',
      message: `Welcome to the secure chat, @${username}!`
    })
  );

  await broadcastOnlineUsers();

  ws.on('message', async rawData => {
    try {
      const parsedData = JSON.parse(rawData.toString());

      if (parsedData.type === 'typing') {
        const targetRoomId = parsedData.roomId || 'lounge';

        await routeTypingToRoom(targetRoomId, username);

        return;
      }

      if (parsedData.type === 'get_history') {
        const targetRoomId = parsedData.roomId || 'lounge';

        const history = await getHistory(targetRoomId, 50, username);

        const historyWithAvatars = [];

        for (const msg of history) {
          historyWithAvatars.push({
            ...msg,
            avatar:
              msg.avatar ||
              (await getUserAvatar(msg.username || msg.sender))
          });
        }

        ws.send(
          JSON.stringify({
            type: 'history',
            roomId: targetRoomId,
            messages: historyWithAvatars
          })
        );

        return;
      }

      if (parsedData.type === 'reaction') {
        const targetRoomId = parsedData.roomId || 'lounge';

        if (targetRoomId.startsWith('group_')) {
          const members = await getGroupMembersByRoom(targetRoomId);

          if (!members.includes(username)) return;
        }

        if (targetRoomId.startsWith('dm_')) {
          const members = getDmMembers(targetRoomId);

          if (!members.includes(username)) return;
        }

        const updatedMessage = await setMessageReaction(
          parsedData.messageId,
          username,
          parsedData.emoji
        );

        if (!updatedMessage || updatedMessage.roomId !== targetRoomId) {
          return;
        }

        await routePayloadToRoom(targetRoomId, {
          type: 'reaction',
          roomId: targetRoomId,
          messageId: String(updatedMessage._id),
          reactions: updatedMessage.reactions
        });

        return;
      }

      if (parsedData.type === 'delete_message') {
        const targetRoomId = parsedData.roomId || 'lounge';
        const mode = parsedData.mode === 'everyone' ? 'everyone' : 'me';

        if (targetRoomId.startsWith('group_')) {
          const members = await getGroupMembersByRoom(targetRoomId);

          if (!members.includes(username)) return;
        }

        if (targetRoomId.startsWith('dm_')) {
          const members = getDmMembers(targetRoomId);

          if (!members.includes(username)) return;
        }

        const deletedMessage = await markMessageDeleted(
          parsedData.messageId,
          username,
          mode
        );

        if (!deletedMessage || deletedMessage.roomId !== targetRoomId) {
          return;
        }

        const payload = {
          type: 'message_deleted',
          roomId: targetRoomId,
          messageId: String(deletedMessage._id),
          mode,
          username
        };

        if (mode === 'everyone') {
          await routePayloadToRoom(targetRoomId, payload);
        } else {
          sendToUser(username, payload);
        }

        return;
      }

      if (parsedData.type !== 'chat') return;

      const content = parsedData.message;

      if (!content || content.trim() === '') return;

      const targetRoomId = parsedData.roomId || 'lounge';

      if (targetRoomId.startsWith('group_')) {
        const members = await getGroupMembersByRoom(targetRoomId);

        if (!members.includes(username)) {
          ws.send(
            JSON.stringify({
              type: 'system',
              message: 'Access denied: You are not a member of this group.'
            })
          );

          return;
        }
      }

      if (targetRoomId.startsWith('dm_')) {
        const members = getDmMembers(targetRoomId);

        if (!members.includes(username)) {
          ws.send(
            JSON.stringify({
              type: 'system',
              message: 'Access denied: Invalid direct message room.'
            })
          );

          return;
        }
      }

      const payload = await saveChatMessage(
        username,
        content.trim(),
        targetRoomId
      );

      const cacheKey = `chat_history:${targetRoomId}`;

      try {
        await client.rpush(cacheKey, payload);
        await client.ltrim(cacheKey, -50, -1);
      } catch (cacheErr) {
        console.warn('[CACHE MESSAGE ERROR]', cacheErr.message);
      }

      await routePayloadToRoom(targetRoomId, payload);
    } catch (error) {
      console.error('[WS ERROR]', error.message);
    }
  });

  ws.on('close', async () => {
    const socketSet = userSockets.get(username);

    if (socketSet) {
      socketSet.delete(ws);

      if (socketSet.size === 0) {
        userSockets.delete(username);

        await client.srem('online_users', username);

        if (isMongoConnected) {
          await User.findOneAndUpdate(
            { username: username.toLowerCase() },
            { lastSeen: new Date() }
          );
        } else {
          await localDbMock.updateLastSeen(username);
        }

        await broadcastOnlineUsers();
      }
    }
  });
});

// REST notification route
app.post('/api/notify', authenticateToken, async (req, res) => {
  const { notification } = req.body;

  if (!notification) {
    return res.status(400).json({
      error: "Missing 'notification' parameter in request body."
    });
  }

  console.log(`[HTTP REST] Notification broadcast: ${notification}`);

  const notificationPayload = JSON.stringify({
    type: 'notification',
    message: notification,
    timestamp: new Date()
  });

  let activeReceivers = 0;

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(notificationPayload);
      activeReceivers++;
    }
  });

  res.status(200).json({
    success: true,
    message: `Notification pushed to ${activeReceivers} active client sessions.`
  });
});

// ================================
// START SERVER
// ================================
const PORT = process.env.PORT || 3000;

server.on('error', err => {
  if (err.code === 'EADDRINUSE') {
    console.error(
      `[SYSTEM] Port ${PORT} is already in use. Stop the other server first, or change PORT in .env.`
    );
    process.exit(1);
  }

  throw err;
});

const startupPromises = [
  mongoConnectPromise,
  redisConnectPromise
];

Promise.allSettled(startupPromises).finally(() => {
  server.listen(PORT, () => {
    console.log(`[SYSTEM] Server listening on http://localhost:${PORT}`);
    console.log('[SYSTEM] Ready for Local Sandbox connection!');
  });
});
