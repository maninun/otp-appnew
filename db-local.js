'use strict';

const path   = require('path');
const fs     = require('fs');
const crypto = require('crypto');

const { DATA_DIR, RUNTIME } = require('./env-detect');
console.log('[db/local] Runtime:', RUNTIME.label, '| DATA_DIR:', DATA_DIR);
const DB_FILE     = path.join(DATA_DIR, 'file.json');
const USER_FILE   = path.join(DATA_DIR, 'user.json');
const PRODUK_FILE = path.join(DATA_DIR, 'produk.json');

fs.mkdirSync(DATA_DIR, { recursive: true });

var _store = {};

function _loadFile() {
  try {
    if (fs.existsSync(DB_FILE)) {
      var raw = fs.readFileSync(DB_FILE, 'utf8');
      _store = JSON.parse(raw);
      console.log('[DB] Loaded', Object.keys(_store).length, 'keys dari', DB_FILE);
    } else {
      _store = {};
      fs.writeFileSync(DB_FILE, '{}', 'utf8');
      console.log('[DB] File baru dibuat:', DB_FILE);
    }
  } catch (e) {
    console.error('[DB] Gagal load file.json:', e.message);
    _store = {};
  }
}

function _saveFile() {
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(_store, null, 2), 'utf8');
  } catch (e) {
    console.error('[DB] Gagal simpan file.json:', e.message);
  }
}

function _saveUserFile() {
  try {
    var out = {};
    Object.keys(_store).forEach(function(key) {
      var m = key.match(/^users\/(.+)\.json$/);
      if (m) out[m[1]] = _store[key].value;
    });
    fs.writeFileSync(USER_FILE, JSON.stringify(out, null, 2), 'utf8');
  } catch (e) {
    console.error('[DB] Gagal simpan user.json:', e.message);
  }
}

function _saveProdukFile() {
  try {
    var rec = _store['products.json'];
    var out = rec ? rec.value : [];
    fs.writeFileSync(PRODUK_FILE, JSON.stringify(out, null, 2), 'utf8');
  } catch (e) {
    console.error('[DB] Gagal simpan produk.json:', e.message);
  }
}

function _autoSave(key) {
  _saveFile();
  _saveUserFile();
  _saveProdukFile();

}

_loadFile();

_saveUserFile();
_saveProdukFile();

console.log('');
console.log('┌──────────────────────────────────────────────────┐');
console.log('│  🗄️  JSON File Backend                            │');
console.log('│  📁 Master  : data/file.json                     │');
console.log('│  👤 Users   : data/user.json    (auto-filter)    │');
console.log('│  📦 Produk  : data/produk.json  (auto-filter)    │');
console.log('│  ✅ Write   : Langsung ke disk setiap operasi    │');
console.log('│  ✅ Simple  : Bisa dibuka & edit manual          │');
console.log('│  ✅ Deps    : Zero external dependency           │');
console.log('└──────────────────────────────────────────────────┘');
console.log('');

function _hashValue(value) {
  return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex').slice(0, 40);
}

var _dbCache = new Map();
var DB_CACHE_TTL = {
  'products.json':        30 * 1000,
  'settings.json':        60 * 1000,
  'panel-templates.json': 60 * 1000,
};
var DB_CACHE_DEFAULT_TTL = 8 * 1000;

function _dbCacheTTL(fp) {
  for (var k of Object.keys(DB_CACHE_TTL)) {
    if (fp.endsWith(k)) return DB_CACHE_TTL[k];
  }
  return DB_CACHE_DEFAULT_TTL;
}
function _dbCacheInvalidate(fp) { if (fp === undefined) { _dbCache.clear(); } else { _dbCache.delete(fp); } }

var _gitTreeCache = new Map();
var GIT_TREE_TTL  = 30 * 1000;

function _invalidateDirCache(fp) {
  var parts = fp.split('/');
  if (parts.length >= 2) _gitTreeCache.delete(parts[0]);
}

function _kvRead(key) {
  var rec = _store[key];
  if (!rec) return { data: null, sha: null };
  return { data: rec.value, sha: rec.sha || null };
}

function _kvWrite(key, value, sha) {
  if (sha !== null && sha !== undefined) {
    var current = _kvRead(key);
    if (current.sha && current.sha !== sha) {
      throw Object.assign(
        new Error('SHA conflict — data sudah diubah oleh proses lain'),
        { status: 409 }
      );
    }
  }
  var newSha = _hashValue(value);
  _store[key] = { value: value, sha: newSha, updatedAt: new Date().toISOString() };
  _autoSave(key);
  return newSha;
}

function _kvDelete(key) {
  if (!_store[key]) return false;
  delete _store[key];
  _autoSave(key);
  return true;
}

function _kvListDir(prefix) {
  var normalPrefix = prefix.endsWith('/') ? prefix : prefix + '/';
  return Object.entries(_store)
    .filter(function(e) {
      var rest = e[0].startsWith(normalPrefix) ? e[0].slice(normalPrefix.length) : null;
      return rest && !rest.includes('/');
    })
    .map(function(e) {
      return { name: e[0].slice(normalPrefix.length), sha: e[1].sha || '', type: 'file' };
    })
    .sort(function(a, b) {
      var ta = (_store[normalPrefix + a.name] || {}).updatedAt || '';
      var tb = (_store[normalPrefix + b.name] || {}).updatedAt || '';
      return new Date(tb) - new Date(ta);
    });
}

async function dbRead(fp, bypassCache) {
  if (!bypassCache) {
    var hit = _dbCache.get(fp);
    if (hit && Date.now() < hit.exp) return hit.val;
  }
  var val = _kvRead(fp);
  _dbCache.set(fp, { val: val, exp: Date.now() + _dbCacheTTL(fp) });
  return val;
}

async function dbWrite(fp, data, sha, _msg) {
  _kvWrite(fp, data, sha);
  _dbCacheInvalidate(fp);
  _invalidateDirCache(fp);
}

async function dbDelete(fp) {
  _kvDelete(fp);
  _dbCacheInvalidate(fp);
  _invalidateDirCache(fp);
}

async function listDirCached(folderPath) {
  var hit = _gitTreeCache.get(folderPath);
  if (hit && Date.now() < hit.exp) return hit.data;
  var files = _kvListDir(folderPath);
  _gitTreeCache.set(folderPath, { data: files, exp: Date.now() + GIT_TREE_TTL });
  return files;
}

var _rlMap = new Map();

async function rateLimitDB(key, maxHits, windowMs) {
  var now   = Date.now();
  var entry = _rlMap.get(key) || { hits: [] };
  entry.hits = entry.hits.filter(function(ts){ return ts > now - windowMs; });
  entry.hits.push(now);
  _rlMap.set(key, entry);
  return entry.hits.length <= maxHits;
}

var _otpLocks    = new Map();
var OTP_LOCK_TTL = 7000;

async function _acquireOtpLockDB(username) {
  var now      = Date.now();
  var existing = _otpLocks.get(username);
  if (existing && existing.expiresAt > now) {
    throw Object.assign(
      new Error('User sedang memproses order lain. Tunggu sebentar.'),
      { status: 429 }
    );
  }
  _otpLocks.set(username, { lockedAt: now, expiresAt: now + OTP_LOCK_TTL });
  return username;
}

async function _releaseOtpLockDB(lockHandle) {
  if (lockHandle) _otpLocks.delete(lockHandle);
}

// ── BUG FIX: Hapus process.on SIGTERM/SIGINT dari sini.
// Sebelumnya db-local mendaftar handler yang langsung memanggil process.exit(0),
// menyebabkan race condition dengan graceful shutdown di index.js — server.close()
// tidak sempat selesai. Sekarang index.js mengontrol exit dan memanggil
// _saveAllData() sebelum keluar.
function _saveAllData(signal) {
  console.log('[DB] ' + (signal || 'shutdown') + ' — memastikan semua data tersimpan...');
  _saveFile();
  _saveUserFile();
  _saveProdukFile();
  console.log('[DB] ✅ file.json + user.json + produk.json tersimpan.');
}

var _db     = { query: function(){ return []; }, pager: { flush: function(){} }, close: function(){} };
var esc     = function(v){ return JSON.stringify(v); };
var q       = function(){ return null; };
var qSelect = function(){ return []; };

var DB_BACKEND = 'jsonfile';

module.exports = {
  DB_BACKEND,
  _dbCacheInvalidate,
  _gitTreeCache,
  dbRead, dbWrite, dbDelete, listDirCached,
  rateLimitDB,
  _acquireOtpLockDB, _releaseOtpLockDB,
  _db, esc, q, qSelect,
  _saveAllData,
};
