'use strict';

/**
 * lib/cdn-external.js
 * ─────────────────────────────────────────────────────────────────────────────
 * CDN backend menggunakan DongtubeDB (dongtube.my.id) sebagai storage via REST.
 *
 * ENV yang diperlukan:
 *   DTDB_URL     = https://dongtube.my.id
 *   DTDB_API_KEY = <admin API key dari DongtubeDB>
 *
 * API:
 *   POST   /api/upload          → upload file (multipart/form-data)
 *   GET    /api/files/{id}      → serve file langsung dari dongtube (no proxy)
 *   DELETE /api/files/{id}      → hapus file
 *
 * Mode: DIRECT (tanpa proxy)
 *   - Upload → file disimpan di dongtube
 *   - URL yang dikembalikan = URL dongtube langsung (bukan /cdn/:filename)
 *   - GET /cdn/:filename → redirect 302 ke dongtube URL
 *   - Tidak ada buffer/streaming melalui server ini
 * ─────────────────────────────────────────────────────────────────────────────
 */

const crypto   = require('crypto');
const FormData = require('form-data');

/* ─── Config ─────────────────────────────────────────────────────────────── */

const DTDB_URL     = (process.env.DTDB_URL || '').replace(/\/$/, '');
const DTDB_API_KEY = process.env.DTDB_API_KEY || '';
const DTDB_TIMEOUT = parseInt(process.env.DTDB_TIMEOUT_MS || '30000', 10);

if (!DTDB_URL) {
  console.error('[CDN-EXT] ❌ DTDB_URL belum diset! Tambahkan DTDB_URL=https://dongtube.my.id di .env');
}
if (!DTDB_API_KEY) {
  console.error('[CDN-EXT] ❌ DTDB_API_KEY belum diset! Upload CDN tidak akan berfungsi.');
}

/* ─── Mime / ext constants (sama dengan cdn-local.js) ───────────────────── */

const CDN_ALLOWED_EXT = /^\.(jpg|jpeg|jfif|png|gif|webp|bmp|svg|ico|tiff|avif|heic|heif|psd|eps|raw|cr2|nef|arw|dng|ai|mp4|mov|mkv|avi|webm|flv|wmv|m4v|ts|mts|ogv|3gp|3g2|f4v|rm|rmvb|vob|mpg|mpeg|m2ts|divx|xvid|asf|mp3|wav|aac|flac|opus|ogg|m4a|wma|aiff|alac|amr|mid|midi|ape|dsf|dff|wv|mka|weba|pdf|doc|docx|xls|xlsx|ppt|pptx|odt|ods|odp|rtf|epub|txt|csv|json|xml|md|html|htm|css|js|jsx|mjs|cjs|tsx|scss|sass|less|py|java|cpp|c|h|hpp|go|rs|php|rb|swift|kt|sh|bash|bat|cmd|ps1|yml|yaml|toml|ini|cfg|conf|env|log|sql|graphql|vue|svelte|zip|rar|7z|tar|gz|bz2|xz|lz|lzma|zst|cab|iso|img|apk|ipa|exe|dmg|deb|rpm|msi|pkg|m3u|m3u8|pls|srt|vtt|ass|ssa|sub|idx|woff|woff2|ttf|otf|eot|bin|dat|db|sqlite|backup|torrent)$/i;

const CDN_MIME = {
  jpg:'image/jpeg',jpeg:'image/jpeg',jfif:'image/jpeg',
  png:'image/png',gif:'image/gif',webp:'image/webp',bmp:'image/bmp',
  svg:'image/svg+xml',ico:'image/x-icon',tiff:'image/tiff',
  avif:'image/avif',heic:'image/heic',heif:'image/heif',
  psd:'image/vnd.adobe.photoshop',eps:'application/postscript',ai:'application/postscript',
  raw:'image/x-raw',cr2:'image/x-canon-cr2',nef:'image/x-nikon-nef',
  arw:'image/x-sony-arw',dng:'image/x-adobe-dng',
  mp4:'video/mp4',mov:'video/quicktime',mkv:'video/x-matroska',
  avi:'video/x-msvideo',webm:'video/webm',flv:'video/x-flv',
  wmv:'video/x-ms-wmv',m4v:'video/x-m4v',ts:'video/mp2t',mts:'video/mp2t',
  ogv:'video/ogg','3gp':'video/3gpp','3g2':'video/3gpp2',
  f4v:'video/mp4',rm:'application/vnd.rn-realmedia',
  rmvb:'application/vnd.rn-realmedia-vbr',vob:'video/dvd',
  mpg:'video/mpeg',mpeg:'video/mpeg',m2ts:'video/mp2t',
  divx:'video/x-divx',xvid:'video/x-xvid',asf:'video/x-ms-asf',
  mp3:'audio/mpeg',wav:'audio/wav',aac:'audio/aac',flac:'audio/flac',
  opus:'audio/opus',ogg:'audio/ogg',m4a:'audio/mp4',wma:'audio/x-ms-wma',
  aiff:'audio/aiff',alac:'audio/x-alac',amr:'audio/amr',
  mid:'audio/midi',midi:'audio/midi',ape:'audio/x-ape',
  dsf:'audio/x-dsf',dff:'audio/x-dff',wv:'audio/x-wavpack',
  mka:'audio/x-matroska',weba:'audio/webm',
  pdf:'application/pdf',doc:'application/msword',
  docx:'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  xls:'application/vnd.ms-excel',
  xlsx:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  ppt:'application/vnd.ms-powerpoint',
  pptx:'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  odt:'application/vnd.oasis.opendocument.text',
  ods:'application/vnd.oasis.opendocument.spreadsheet',
  odp:'application/vnd.oasis.opendocument.presentation',
  rtf:'application/rtf',epub:'application/epub+zip',
  txt:'text/plain',csv:'text/csv',json:'application/json',
  xml:'application/xml',md:'text/markdown',html:'text/html',htm:'text/html',
  css:'text/css',js:'text/javascript',jsx:'text/javascript',
  mjs:'text/javascript',cjs:'text/javascript',
  tsx:'text/plain',scss:'text/x-scss',sass:'text/x-sass',less:'text/css',
  py:'text/x-python',java:'text/x-java',cpp:'text/plain',c:'text/plain',
  h:'text/plain',hpp:'text/plain',php:'text/plain',rb:'text/plain',
  go:'text/plain',rs:'text/plain',swift:'text/plain',kt:'text/plain',
  sh:'text/plain',bash:'text/plain',bat:'text/plain',cmd:'text/plain',ps1:'text/plain',
  yml:'text/plain',yaml:'text/plain',toml:'text/plain',ini:'text/plain',
  cfg:'text/plain',conf:'text/plain',env:'text/plain',log:'text/plain',
  sql:'text/plain',graphql:'text/plain',vue:'text/plain',svelte:'text/plain',
  zip:'application/zip',rar:'application/x-rar-compressed',
  '7z':'application/x-7z-compressed',tar:'application/x-tar',
  gz:'application/gzip',bz2:'application/x-bzip2',
  xz:'application/x-xz',lz:'application/x-lzip',lzma:'application/x-lzma',
  zst:'application/zstd',cab:'application/vnd.ms-cab-compressed',
  iso:'application/x-iso9660-image',img:'application/x-raw-disk-image',
  apk:'application/vnd.android.package-archive',ipa:'application/octet-stream',
  exe:'application/vnd.microsoft.portable-executable',
  dmg:'application/x-apple-diskimage',
  deb:'application/vnd.debian.binary-package',rpm:'application/x-rpm',
  msi:'application/x-msi',pkg:'application/x-newton-compatible-pkg',
  m3u:'audio/x-mpegurl',m3u8:'application/vnd.apple.mpegurl',pls:'audio/x-scpls',
  srt:'text/plain',vtt:'text/vtt',ass:'text/plain',ssa:'text/plain',
  sub:'text/plain',idx:'text/plain',
  woff:'font/woff',woff2:'font/woff2',ttf:'font/ttf',otf:'font/otf',
  eot:'application/vnd.ms-fontobject',
  bin:'application/octet-stream',dat:'application/octet-stream',
  db:'application/x-sqlite3',sqlite:'application/x-sqlite3',
  backup:'application/octet-stream',torrent:'application/x-bittorrent',
};

const CDN_TEXT_EXTS = new Set([
  'txt','html','htm','json','xml','csv','md','js','jsx','mjs','cjs','tsx',
  'css','scss','sass','less','py','java','cpp','c','h','hpp','php','rb',
  'go','rs','swift','kt','sh','bash','bat','cmd','ps1',
  'yml','yaml','toml','ini','cfg','conf','env','log','sql','graphql','vue','svelte',
  'srt','vtt','ass','ssa','sub','idx',
]);

const CDN_DOWNLOAD_EXTS = new Set([
  'apk','ipa','zip','rar','7z','tar','gz','bz2','xz','lz','lzma','zst','cab',
  'exe','dmg','msi','pkg','deb','rpm','iso','img',
  'db','sqlite','backup','bin','dat','torrent',
]);

const CDN_DANGEROUS_EXTS = new Set([
  'html','htm','js','mjs','jsx','tsx','cjs','svg','xml',
  'php','sh','bash','bat','cmd','ps1','py','rb','vue','svelte',
]);

const CDN_MAX_FOLDERS          = 10;
const CDN_MAX_FILES_PER_FOLDER = 500;

/* ─── HTTP helpers (native fetch) ───────────────────────────────────────── */

function _headers(extra) {
  return Object.assign({ 'X-API-Key': DTDB_API_KEY }, extra || {});
}

function _kvUrl(key) { return DTDB_URL + '/api/kv/' + key; }

/* ─── In-memory list cache ───────────────────────────────────────────────── */

var _listCache    = null;
var _listCacheAt  = 0;
const LIST_TTL    = 10 * 1000; // 10 detik

function _invalidateListCache() {
  _listCache   = null;
  _listCacheAt = 0;
}

/* ─── KV mapping helpers (filename → dtdbId) ─────────────────────────────── */
// Disimpan di DongtubeDB KV sebagai: cdn-map/{filename} → { dtdbId, size, uploadedAt }

async function _mapGet(filename) {
  var ac = new AbortController();
  var tid = setTimeout(function() { ac.abort(); }, DTDB_TIMEOUT);
  try {
    var r = await fetch(_kvUrl('cdn-map/' + filename), {
      headers: _headers(),
      signal: ac.signal,
    });
    clearTimeout(tid);
    if (r.status === 404) return null;
    var body = await r.json();
    if (!body || !body.success) return null;
    return body.data || null;
  } catch(e) {
    clearTimeout(tid);
    console.error('[cdn-ext] _mapGet error:', filename, e.message);
    return null;
  }
}

async function _mapSet(filename, record) {
  var ac = new AbortController();
  var tid = setTimeout(function() { ac.abort(); }, DTDB_TIMEOUT);
  try {
    await fetch(_kvUrl('cdn-map/' + filename), {
      method : 'PUT',
      headers: _headers({ 'Content-Type': 'application/json' }),
      body   : JSON.stringify(record),
      signal : ac.signal,
    });
    clearTimeout(tid);
  } catch(e) {
    clearTimeout(tid);
    console.error('[cdn-ext] _mapSet error:', filename, e.message);
  }
}

async function _mapDelete(filename) {
  var ac = new AbortController();
  var tid = setTimeout(function() { ac.abort(); }, DTDB_TIMEOUT);
  try {
    await fetch(_kvUrl('cdn-map/' + filename), {
      method: 'DELETE',
      headers: _headers(),
      signal: ac.signal,
    });
    clearTimeout(tid);
  } catch(e) {
    clearTimeout(tid);
    console.error('[cdn-ext] _mapDelete error:', filename, e.message);
  }
}

async function _mapListAll() {
  var ac = new AbortController();
  var tid = setTimeout(function() { ac.abort(); }, DTDB_TIMEOUT);
  try {
    var r = await fetch(DTDB_URL + '/api/kv?prefix=cdn-map%2F&mode=all', {
      headers: _headers(),
      signal: ac.signal,
    });
    clearTimeout(tid);
    var body = await r.json();
    if (!body || !body.success) return [];
    return Array.isArray(body.items) ? body.items : [];
  } catch(e) {
    clearTimeout(tid);
    console.error('[cdn-ext] _mapListAll error:', e.message);
    return [];
  }
}

/* ─── _cdnFolderCache stub (agar routes/cdn.js tidak error) ─────────────── */

var _cdnFolderCache = new Map();

/* ─── Error sanitizer ───────────────────────────────────────────────────── */

function _cdnSanitizeError(err) {
  var msg = (err && err.message) || String(err);
  if (/quota|storage full/i.test(msg))     return 'Storage DongtubeDB penuh, hubungi admin.';
  if (/timeout|etimedout/i.test(msg))      return 'Koneksi ke DongtubeDB timeout, coba lagi.';
  if (/401|403|forbidden|unauthorized/i.test(msg)) return 'API key DongtubeDB tidak valid, hubungi admin.';
  if (/413|too large/i.test(msg))          return 'File terlalu besar.';
  return 'Upload gagal, coba lagi.';
}

/* ─── Core CDN functions ─────────────────────────────────────────────────── */

/**
 * Upload file ke DongtubeDB.
 * @returns {string} URL dongtube langsung, contoh: https://dongtube.my.id/api/files/2gCX58HU.png
 */
async function cdnUploadFile(filename, buffer) {
  var axios = require('axios');
  var form  = new FormData();
  form.append('file', buffer, {
    filename   : filename,
    contentType: cdnGetMime('.' + filename.split('.').pop()),
  });
  form.append('public', 'true');

  var res;
  try {
    res = await axios.post(DTDB_URL + '/api/upload', form, {
      headers: Object.assign({}, form.getHeaders(), { 'X-API-Key': DTDB_API_KEY }),
      maxContentLength: Infinity,
      maxBodyLength   : Infinity,
      timeout         : DTDB_TIMEOUT,
    });
  } catch(e) {
    console.error('[cdn-ext] upload axios error:', e.message);
    throw new Error((e.response && e.response.data && e.response.data.error) || e.message);
  }

  var data = res.data;
  if (!data || !data.success || !data.file) {
    throw new Error('DongtubeDB upload gagal: ' + JSON.stringify(data));
  }

  var dtdbId    = data.file.id;
  var directUrl = data.file.url || (DTDB_URL + '/api/files/' + dtdbId);
  var size      = data.file.size || buffer.length;

  // Simpan mapping filename → dtdbId + directUrl di KV
  await _mapSet(filename, {
    dtdbId      : dtdbId,
    directUrl   : directUrl,
    originalName: filename,
    size        : size,
    uploadedAt  : new Date().toISOString(),
  });

  _invalidateListCache();

  console.log('[cdn-ext] uploaded:', filename, '→', directUrl,
    '|', (size / 1024 / 1024).toFixed(2) + 'MB');

  // Kembalikan URL dongtube langsung (bukan /cdn/:filename)
  return directUrl;
}

/**
 * Ambil URL direct dongtube untuk filename tertentu.
 * Dipakai oleh routes/cdn.js untuk redirect 302 (tanpa proxy buffer).
 * @returns {string|null} URL dongtube langsung, atau null jika tidak ditemukan
 */
async function cdnGetDirectUrl(filename) {
  var record = await _mapGet(filename);
  if (!record) return null;
  if (record.directUrl) return record.directUrl;
  if (record.dtdbId)    return DTDB_URL + '/api/files/' + record.dtdbId;
  return null;
}

/**
 * cdnReadFile — tidak dipakai di mode external (redirect langsung ke dongtube).
 * Tetap di-export agar interface sama dengan cdn-local.js.
 * @returns {null}
 */
function cdnReadFile() {
  return null;
}

/**
 * Hapus file dari DongtubeDB.
 * @returns {boolean}
 */
async function cdnDeleteFile(filename) {
  var record = await _mapGet(filename);
  if (!record || !record.dtdbId) return false;

  var ac  = new AbortController();
  var tid = setTimeout(function() { ac.abort(); }, DTDB_TIMEOUT);
  try {
    await fetch(DTDB_URL + '/api/files/' + record.dtdbId, {
      method : 'DELETE',
      headers: _headers(),
      signal : ac.signal,
    });
    clearTimeout(tid);
  } catch(e) {
    clearTimeout(tid);
    console.error('[cdn-ext] cdnDeleteFile error:', filename, e.message);
    return false;
  }

  await _mapDelete(filename);
  _invalidateListCache();
  return true;
}

/**
 * List semua file CDN.
 * @returns {Array<{name, url, size, path, folder, sha, download}>}
 */
async function cdnListFiles() {
  if (_listCache && Date.now() - _listCacheAt < LIST_TTL) return _listCache;

  var items = await _mapListAll();
  var result = [];

  for (var i = 0; i < items.length; i++) {
    var item = items[i];
    // item.key = "cdn-map/filename.ext" atau item.name = "filename.ext"
    var filename = item.key
      ? item.key.replace(/^cdn-map\//, '')
      : (item.name || '');

    if (!filename) continue;

    var rec = null;
    try {
      // Data ada di item langsung (mode=all) atau perlu fetch
      rec = item.data || item.value || null;
      // Jika rec masih string (KV menyimpan JSON string), parse dulu
      if (typeof rec === 'string') { try { rec = JSON.parse(rec); } catch(_) { rec = null; } }
      if (!rec) {
        rec = await _mapGet(filename);
      }
    } catch(e) {}

    var size = (rec && rec.size) ? rec.size : 0;
    var directUrl = (rec && rec.directUrl)
      ? rec.directUrl
      : (rec && rec.dtdbId ? DTDB_URL + '/api/files/' + rec.dtdbId : '/cdn/' + filename);

    result.push({
      name    : filename,
      url     : directUrl,
      size    : size,
      path    : 'dtdb:cdn-map/' + filename,
      folder  : 'files',
      sha     : crypto.createHash('md5').update(filename + size).digest('hex').slice(0, 8),
      download: directUrl,
    });
  }

  _listCache   = result;
  _listCacheAt = Date.now();
  return result;
}

/**
 * Storage stats.
 */
async function cdnStorageStats() {
  var files     = await cdnListFiles();
  var totalSize = files.reduce(function(s, f) { return s + (f.size || 0); }, 0);
  return {
    backend        : 'dongtube-external',
    dataDir        : DTDB_URL + ' (DongtubeDB)',
    totalFiles     : files.length,
    totalSizeBytes : totalSize,
    folders        : { files: { files: files.length, sizeBytes: totalSize } },
  };
}

/* ─── Helpers ────────────────────────────────────────────────────────────── */

function _cdnAccounts() {
  return [{ name: 'dongtube-external', backend: 'dongtube-external', active: true }];
}

function _cdnInvalidateCache() {
  _invalidateListCache();
  _cdnFolderCache.clear();
}

function cdnGetMime(ext) {
  var e    = ext.replace(/^\./, '').toLowerCase();
  var mime = CDN_MIME[e] || 'application/octet-stream';
  if (CDN_TEXT_EXTS.has(e)) mime += '; charset=utf-8';
  return mime;
}

/* ─── Boot log ───────────────────────────────────────────────────────────── */

console.log('');
console.log('┌──────────────────────────────────────────────────────┐');
console.log('│  🌐  DongtubeDB External CDN  (Direct / No Proxy)    │');
console.log('│  🔗  URL  : ' + DTDB_URL.padEnd(40) + '│');
console.log('│  🔑  Key  : ' + (DTDB_API_KEY ? '✅ Set' : '❌ Missing!').padEnd(40) + '│');
console.log('│  📁  Map  : KV cdn-map/{filename} → dtdbId+directUrl │');
console.log('│  🚀  Mode : Direct — GET /cdn/:f → 302 ke dongtube   │');
console.log('└──────────────────────────────────────────────────────┘');
console.log('');

/* ─── Exports ────────────────────────────────────────────────────────────── */

module.exports = {
  _cdnAccounts, _cdnInvalidateCache, _cdnSanitizeError, _cdnFolderCache,
  cdnUploadFile, cdnReadFile, cdnGetDirectUrl,
  cdnDeleteFile, cdnListFiles, cdnStorageStats,
  cdnGetMime,
  CDN_ALLOWED_EXT, CDN_MIME, CDN_TEXT_EXTS, CDN_DOWNLOAD_EXTS, CDN_DANGEROUS_EXTS,
  CDN_MAX_FOLDERS, CDN_MAX_FILES_PER_FOLDER,
};
