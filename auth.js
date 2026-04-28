'use strict';

const crypto = require('crypto');
const C      = require('./config');

const SIGNING_SECRET = (() => {
  if (!process.env.TOKEN_SECRET) {
    return crypto.randomBytes(64).toString('hex');
  }
  return process.env.TOKEN_SECRET;
})();

const SESSION_TTL = 8 * 60 * 60 * 1000;
const USER_TTL    = 30 * 24 * 60 * 60 * 1000;

function makeToken(payload) {
  const data = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig  = crypto.createHmac('sha256', SIGNING_SECRET).update(data).digest('base64url');
  return data + '.' + sig;
}

function verifyToken(token) {
  if (!token || typeof token !== 'string') return null;
  const dot  = token.lastIndexOf('.');
  if (dot < 0) return null;
  const data = token.slice(0, dot);
  const sig  = token.slice(dot + 1);
  const expected = crypto.createHmac('sha256', SIGNING_SECRET).update(data).digest('base64url');
  try {
    const sigBuf      = Buffer.from(sig,      'base64url');
    const expectedBuf = Buffer.from(expected, 'base64url');
    if (sigBuf.length !== expectedBuf.length || !crypto.timingSafeEqual(sigBuf, expectedBuf)) return null;
  } catch(e) { return null; }
  try {
    const p = JSON.parse(Buffer.from(data, 'base64url').toString());
    if (p.exp && Date.now() > p.exp) return null;
    return p;
  } catch(e) { return null; }
}

function makeAdminToken() {
  return makeToken({ role: 'admin', iat: Date.now(), exp: Date.now() + SESSION_TTL });
}

function makeUserToken(username) {
  return makeToken({ sub: username, role: 'user', iat: Date.now(), exp: Date.now() + USER_TTL });
}

const _rl = new Map();
var _rlLastClean = Date.now();

function _rlCleanup() {
  var now = Date.now();
  if (now - _rlLastClean < 5 * 60 * 1000) return;
  _rlLastClean = now;
  for (var _k of _rl.keys()) { var _v = _rl.get(_k); if (_v && now > _v.r) _rl.delete(_k); }
}
function rateLimit(key, maxHits, windowMs) {
  const now = Date.now();
  _rlCleanup();
  const e   = _rl.get(key) || { h: 0, r: now + windowMs };
  if (now > e.r) { e.h = 0; e.r = now + windowMs; }
  e.h++;
  _rl.set(key, e);
  return e.h <= maxHits;
}

function adminAuth(req, res, next) {
  const token   = req.headers['x-admin-token'];
  const payload = verifyToken(token);
  if (!payload || payload.role !== 'admin') {
    return res.status(401).json({ ok: false, message: 'Sesi tidak valid. Login ulang.' });
  }
  req.adminIp = req.ip || 'unknown';
  next();
}

module.exports = {
  makeToken, verifyToken, makeAdminToken, makeUserToken,
  rateLimit,
  adminAuth,

};
