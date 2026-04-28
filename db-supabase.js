'use strict';

/**
 * lib/db-supabase.js — Supabase KV Backend
 * ------------------------------------------------------------
 * Backend ini mempertahankan interface yang sama dengan db-local.js:
 *   dbRead(fp) -> { data, sha }
 *   dbWrite(fp, data, sha)
 *   dbDelete(fp)
 *   listDirCached(folderPath)
 *
 * ENV wajib:
 *   SUPABASE_URL=https://xxxx.supabase.co
 *   SUPABASE_SERVICE_ROLE_KEY=<service_role key>
 *
 * ENV opsional:
 *   SUPABASE_KV_TABLE=dongtube_kv
 *   SUPABASE_TIMEOUT_MS=10000
 *
 * SQL tabel ada di supabase/schema.sql.
 */

const axios = require('axios');
const crypto = require('crypto');

const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY || '';
const SUPABASE_KV_TABLE = process.env.SUPABASE_KV_TABLE || 'dongtube_kv';
const SUPABASE_TIMEOUT = parseInt(process.env.SUPABASE_TIMEOUT_MS || '10000', 10);

if (!SUPABASE_URL) console.error('[DB-SUPABASE] SUPABASE_URL belum diset.');
if (!SUPABASE_SERVICE_ROLE_KEY) console.error('[DB-SUPABASE] SUPABASE_SERVICE_ROLE_KEY belum diset.');

function _headers(extra) {
  return Object.assign({
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: 'Bearer ' + SUPABASE_SERVICE_ROLE_KEY,
    'Content-Type': 'application/json',
    Prefer: 'return=representation',
  }, extra || {});
}

function _tableUrl(query) {
  return SUPABASE_URL + '/rest/v1/' + encodeURIComponent(SUPABASE_KV_TABLE) + (query || '');
}

function _hashValue(value) {
  return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex').slice(0, 40);
}

var _dbCache = new Map();
var DB_CACHE_TTL = {
  'products.json': 30 * 1000,
  'settings.json': 60 * 1000,
  'panel-templates.json': 60 * 1000,
};
var DB_CACHE_DEFAULT_TTL = 8 * 1000;
function _dbCacheTTL(fp) {
  for (var k of Object.keys(DB_CACHE_TTL)) if (fp.endsWith(k)) return DB_CACHE_TTL[k];
  return DB_CACHE_DEFAULT_TTL;
}
function _dbCacheInvalidate(fp) { if (fp === undefined) _dbCache.clear(); else _dbCache.delete(fp); }

var _gitTreeCache = new Map();
var GIT_TREE_TTL = 30 * 1000;
function _invalidateDirCache(fp) {
  var parts = fp.split('/');
  if (parts.length >= 2) _gitTreeCache.delete(parts[0]);
}

function _encodeEq(v) { return 'eq.' + encodeURIComponent(v); }

async function dbRead(fp, bypassCache) {
  if (!bypassCache) {
    var hit = _dbCache.get(fp);
    if (hit && Date.now() < hit.exp) return hit.val;
  }
  try {
    var resp = await axios.get(_tableUrl('?key=' + _encodeEq(fp) + '&select=value,sha&limit=1'), {
      headers: _headers(),
      timeout: SUPABASE_TIMEOUT,
      validateStatus: function(s) { return s < 500; },
    });
    var row = Array.isArray(resp.data) ? resp.data[0] : null;
    var val = row ? { data: row.value, sha: row.sha || null } : { data: null, sha: null };
    _dbCache.set(fp, { val: val, exp: Date.now() + _dbCacheTTL(fp) });
    return val;
  } catch (e) {
    console.error('[DB-SUPABASE] dbRead error:', fp, e.message);
    return { data: null, sha: null };
  }
}

async function dbWrite(fp, data, sha, _msg) {
  var current = await dbRead(fp, true);
  if (sha !== null && sha !== undefined && current.sha && current.sha !== sha) {
    throw Object.assign(new Error('SHA conflict — data sudah diubah oleh proses lain'), { status: 409 });
  }

  var newSha = _hashValue(data);
  var row = { key: fp, value: data, sha: newSha, updated_at: new Date().toISOString() };
  try {
    var resp = await axios.post(_tableUrl('?on_conflict=key'), [row], {
      headers: _headers({ Prefer: 'resolution=merge-duplicates,return=minimal' }),
      timeout: SUPABASE_TIMEOUT,
      validateStatus: function(s) { return s < 600; },
    });
    if (resp.status >= 400) throw new Error('Supabase upsert gagal: ' + JSON.stringify(resp.data));
    _dbCacheInvalidate(fp);
    _invalidateDirCache(fp);
    return newSha;
  } catch (e) {
    console.error('[DB-SUPABASE] dbWrite error:', fp, e.message);
    throw e;
  }
}

async function dbDelete(fp) {
  try {
    await axios.delete(_tableUrl('?key=' + _encodeEq(fp)), {
      headers: _headers(),
      timeout: SUPABASE_TIMEOUT,
      validateStatus: function(s) { return s < 500; },
    });
    _dbCacheInvalidate(fp);
    _invalidateDirCache(fp);
  } catch (e) {
    console.error('[DB-SUPABASE] dbDelete error:', fp, e.message);
  }
}

async function listDirCached(folderPath) {
  var hit = _gitTreeCache.get(folderPath);
  if (hit && Date.now() < hit.exp) return hit.data;
  var prefix = folderPath.endsWith('/') ? folderPath : folderPath + '/';
  try {
    var resp = await axios.get(_tableUrl('?key=like.' + encodeURIComponent(prefix + '*') + '&select=key,sha,updated_at&order=updated_at.desc'), {
      headers: _headers(),
      timeout: SUPABASE_TIMEOUT,
      validateStatus: function(s) { return s < 500; },
    });
    var files = (Array.isArray(resp.data) ? resp.data : [])
      .map(function(row) {
        var rest = row.key && row.key.startsWith(prefix) ? row.key.slice(prefix.length) : '';
        if (!rest || rest.includes('/')) return null;
        return { name: rest, sha: row.sha || '', type: 'file' };
      })
      .filter(Boolean);
    _gitTreeCache.set(folderPath, { data: files, exp: Date.now() + GIT_TREE_TTL });
    return files;
  } catch (e) {
    console.error('[DB-SUPABASE] listDirCached error:', folderPath, e.message);
    return [];
  }
}

var _rlMap = new Map();
async function rateLimitDB(key, maxHits, windowMs) {
  var now = Date.now();
  var entry = _rlMap.get(key) || { hits: [] };
  entry.hits = entry.hits.filter(function(ts) { return ts > now - windowMs; });
  entry.hits.push(now);
  _rlMap.set(key, entry);
  return entry.hits.length <= maxHits;
}

var _otpLocks = new Map();
var OTP_LOCK_TTL = 7000;
async function _acquireOtpLockDB(username) {
  var now = Date.now();
  var existing = _otpLocks.get(username);
  if (existing && existing.expiresAt > now) {
    throw Object.assign(new Error('User sedang memproses order lain. Tunggu sebentar.'), { status: 429 });
  }
  _otpLocks.set(username, { lockedAt: now, expiresAt: now + OTP_LOCK_TTL });
  return username;
}
async function _releaseOtpLockDB(lockHandle) { if (lockHandle) _otpLocks.delete(lockHandle); }

var _db = { query: function() { return []; }, pager: { flush: function() {} }, close: function() {} };
var esc = function(v) { return JSON.stringify(v); };
var q = function() { return null; };
var qSelect = function() { return []; };
var DB_BACKEND = 'supabase';

console.log('');
console.log('┌──────────────────────────────────────────────────────┐');
console.log('│  ⚡  Supabase Backend                                │');
console.log('│  🔗  URL   : ' + (SUPABASE_URL || '-').slice(0, 38).padEnd(38) + ' │');
console.log('│  🧾  Table : ' + SUPABASE_KV_TABLE.slice(0, 38).padEnd(38) + ' │');
console.log('│  🔑  Key   : ' + (SUPABASE_SERVICE_ROLE_KEY ? 'Set' : 'Missing').padEnd(38) + ' │');
console.log('└──────────────────────────────────────────────────────┘');
console.log('');

module.exports = {
  DB_BACKEND,
  _dbCacheInvalidate,
  _gitTreeCache,
  dbRead, dbWrite, dbDelete, listDirCached,
  rateLimitDB,
  _acquireOtpLockDB, _releaseOtpLockDB,
  _db, esc, q, qSelect,
};
