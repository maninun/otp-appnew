'use strict';

/**
 * db-external.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Database backend yang menggunakan DongtubeDB (dongtube.my.id) sebagai
 * penyimpanan eksternal via REST API KV.
 *
 * ENV yang diperlukan:
 *   DTDB_URL     = https://dongtube.my.id   (base URL DongtubeDB)
 *   DTDB_API_KEY = <admin API key dari DongtubeDB>
 *
 * API yang dipakai:
 *   GET    /api/kv/{key}              → { success, data, sha }
 *   PUT    /api/kv/{key}              → body JSON, header x-sha (optional)
 *   DELETE /api/kv/{key}
 *   GET    /api/kv?prefix=x&mode=dir  → { success, items: [{name,sha,type}] }
 * ─────────────────────────────────────────────────────────────────────────────
 */

const axios  = require('axios');
const crypto = require('crypto');

/* ─── Config ─────────────────────────────────────────────────────────────── */

const DTDB_URL     = (process.env.DTDB_URL || '').replace(/\/$/, '');
const DTDB_API_KEY = process.env.DTDB_API_KEY || '';
const DTDB_TIMEOUT = parseInt(process.env.DTDB_TIMEOUT_MS || '10000', 10);

if (!DTDB_URL) {
  console.error('[DB-EXT] ❌ DTDB_URL belum diset! Tambahkan di .env');
}
if (!DTDB_API_KEY) {
  console.error('[DB-EXT] ❌ DTDB_API_KEY belum diset! Tambahkan di .env');
}

/* ─── HTTP helper ─────────────────────────────────────────────────────────── */

function _headers(extraHeaders) {
  return Object.assign({ 'x-api-key': DTDB_API_KEY, 'Content-Type': 'application/json' }, extraHeaders || {});
}

/**
 * Encode key untuk path URL — forward slash DIPERTAHANKAN karena path-based API.
 * Contoh: "users/alice.json" → "/api/kv/users/alice.json"
 */
function _kvUrl(key) {
  return DTDB_URL + '/api/kv/' + key;
}

function _listUrl(prefix, mode) {
  return DTDB_URL + '/api/kv?prefix=' + encodeURIComponent(prefix) + '&mode=' + (mode || 'dir');
}

/* ─── In-memory cache (sama persis dengan db-local, agar read tidak selalu HTTP) */

var _dbCache        = new Map();
var DB_CACHE_TTL    = {
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

/* ─── Core KV operations ─────────────────────────────────────────────────── */

/**
 * dbRead(fp, bypassCache?)
 * → { data: <value|null>, sha: <string|null> }
 */
async function dbRead(fp, bypassCache) {
  if (!bypassCache) {
    var hit = _dbCache.get(fp);
    if (hit && Date.now() < hit.exp) return hit.val;
  }

  try {
    var resp = await axios.get(_kvUrl(fp), {
      headers : _headers(),
      timeout : DTDB_TIMEOUT,
      validateStatus: function(s) { return s < 500; },
    });

    var val;
    if (resp.status === 404 || !resp.data || !resp.data.success) {
      val = { data: null, sha: null };
    } else {
      val = { data: resp.data.data, sha: resp.data.sha || null };
    }

    _dbCache.set(fp, { val: val, exp: Date.now() + _dbCacheTTL(fp) });
    return val;

  } catch (e) {
    console.error('[DB-EXT] dbRead error:', fp, e.message);
    return { data: null, sha: null };
  }
}

/**
 * dbWrite(fp, data, sha?, msg?)
 * Throws { status: 409 } jika SHA mismatch (optimistic locking).
 */
async function dbWrite(fp, data, sha, _msg) {
  var headers = _headers();
  if (sha !== null && sha !== undefined) {
    headers['x-sha'] = sha;
  }

  try {
    var resp = await axios.put(_kvUrl(fp), data, {
      headers : headers,
      timeout : DTDB_TIMEOUT,
      validateStatus: function(s) { return s < 600; },
    });

    if (resp.status === 409) {
      throw Object.assign(
        new Error('SHA conflict — data sudah diubah oleh proses lain'),
        { status: 409 }
      );
    }
    if (!resp.data || !resp.data.success) {
      throw new Error('[DB-EXT] dbWrite gagal: ' + JSON.stringify(resp.data));
    }

    _dbCacheInvalidate(fp);
    _invalidateDirCache(fp);

  } catch (e) {
    if (e.status === 409) throw e;
    console.error('[DB-EXT] dbWrite error:', fp, e.message);
    throw e;
  }
}

/**
 * dbDelete(fp)
 */
async function dbDelete(fp) {
  try {
    await axios.delete(_kvUrl(fp), {
      headers : _headers(),
      timeout : DTDB_TIMEOUT,
      validateStatus: function(s) { return s < 500; },
    });
    _dbCacheInvalidate(fp);
    _invalidateDirCache(fp);
  } catch (e) {
    console.error('[DB-EXT] dbDelete error:', fp, e.message);
  }
}

/**
 * listDirCached(folderPath)
 * → [{ name, sha, type }]
 */
async function listDirCached(folderPath) {
  var hit = _gitTreeCache.get(folderPath);
  if (hit && Date.now() < hit.exp) return hit.data;

  try {
    var resp = await axios.get(_listUrl(folderPath, 'dir'), {
      headers : _headers(),
      timeout : DTDB_TIMEOUT,
      validateStatus: function(s) { return s < 500; },
    });

    var files = [];
    if (resp.data && resp.data.success && Array.isArray(resp.data.items)) {
      files = resp.data.items; // [{ name, sha, type }]
    }

    _gitTreeCache.set(folderPath, { data: files, exp: Date.now() + GIT_TREE_TTL });
    return files;

  } catch (e) {
    console.error('[DB-EXT] listDirCached error:', folderPath, e.message);
    return [];
  }
}

/* ─── Rate limiter — in-memory (sama dengan local) ──────────────────────── */

var _rlMap = new Map();

async function rateLimitDB(key, maxHits, windowMs) {
  var now   = Date.now();
  var entry = _rlMap.get(key) || { hits: [] };
  entry.hits = entry.hits.filter(function(ts) { return ts > now - windowMs; });
  entry.hits.push(now);
  _rlMap.set(key, entry);
  return entry.hits.length <= maxHits;
}

/* ─── OTP lock — in-memory mutex (sama dengan local) ────────────────────── */

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

/* ─── Stub untuk backward-compat (tidak dipakai di jsonfile mode) ────────── */

var _db     = { query: function() { return []; }, pager: { flush: function() {} }, close: function() {} };
var esc     = function(v) { return JSON.stringify(v); };
var q       = function() { return null; };
var qSelect = function() { return []; };

/* ─── Boot log ───────────────────────────────────────────────────────────── */

var DB_BACKEND = 'dongtube-external';

console.log('');
console.log('┌──────────────────────────────────────────────────────┐');
console.log('│  🌐  DongtubeDB External Backend                     │');
console.log('│  🔗  URL  : ' + DTDB_URL.padEnd(40) + '│');
console.log('│  🔑  Key  : ' + (DTDB_API_KEY ? '✅ Set' : '❌ Missing!').padEnd(40) + '│');
console.log('│  ⚡  TTL  : 8s default, 30s products, 60s settings   │');
console.log('│  🔒  Auth : x-api-key header (Admin role)            │');
console.log('└──────────────────────────────────────────────────────┘');
console.log('');

/* ─── Exports (interface identik dengan db-local.js) ─────────────────────── */

module.exports = {
  DB_BACKEND,
  _dbCacheInvalidate,
  _gitTreeCache,
  dbRead, dbWrite, dbDelete, listDirCached,
  rateLimitDB,
  _acquireOtpLockDB, _releaseOtpLockDB,
  _db, esc, q, qSelect,
};
