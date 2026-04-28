'use strict';

const path   = require('path');
const fs     = require('fs');
const crypto = require('crypto');

const { DATA_DIR, RUNTIME } = require('./env-detect');
console.log('[cdn/local] Runtime:', RUNTIME.label, '| DATA_DIR:', DATA_DIR);
const CDN_DIR  = path.join(DATA_DIR, 'cdn');

const CDN_MAX_FILES_PER_FOLDER = 500;
const CDN_MAX_FOLDERS          = 10;

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

fs.mkdirSync(CDN_DIR, { recursive: true });

console.log('[cdn] Local CDN aktif → data/cdn/');

var _cdnFolderCache = new Map();
const CACHE_TTL = 5 * 60 * 1000;

function _getCacheKey(folder)  { return 'count_' + folder; }

function _getCachedCount(folder) {
  var key    = _getCacheKey(folder);
  var cached = _cdnFolderCache.get(key);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) return cached.count;
  _cdnFolderCache.delete(key);
  return null;
}

function _setCachedCount(folder, count) {
  _cdnFolderCache.set(_getCacheKey(folder), { count: count, timestamp: Date.now() });
}

function _invalidateFolder(folder) {
  _cdnFolderCache.delete(_getCacheKey(folder));
  _cdnFolderCache.delete('list');
}

function _folderName(i) { return i === 1 ? 'files' : 'files' + i; }
function _folderPath(i) { return path.join(CDN_DIR, _folderName(i)); }

function _findAvailableFolder() {
  for (var i = 1; i <= CDN_MAX_FOLDERS; i++) {
    var fn  = _folderName(i);
    var fp  = _folderPath(i);
    var cached = _getCachedCount(fn);
    var count;

    if (cached !== null) {
      count = cached;
    } else {
      try {
        fs.mkdirSync(fp, { recursive: true });
        count = fs.readdirSync(fp).filter(function(f) {
          try { return fs.statSync(path.join(fp, f)).isFile(); } catch(e) { return false; }
        }).length;
      } catch(e) { count = 0; }
      _setCachedCount(fn, count);
    }

    if (count < CDN_MAX_FILES_PER_FOLDER) return fn;
  }
  return null;
}

function _findFile(filename) {
  for (var i = 1; i <= CDN_MAX_FOLDERS; i++) {
    var fp = path.join(_folderPath(i), filename);
    if (fs.existsSync(fp)) return { folder: _folderName(i), filePath: fp };
  }
  return null;
}

function _cdnSanitizeError(err) {
  var msg = (err && err.message) || String(err);
  if (/enospc|no space/i.test(msg))   return 'Disk penuh, hubungi admin.';
  if (/eacces|permission/i.test(msg)) return 'Tidak ada izin tulis, hubungi admin.';
  if (/enoent/i.test(msg))            return 'File tidak ditemukan.';
  if (/etimedout|timeout/i.test(msg)) return 'Request timeout, coba lagi.';
  return 'Upload gagal, coba lagi.';
}

function _cdnAccounts()        { return [{ name: 'local', backend: 'local', active: true }]; }
function _cdnInvalidateCache() { _cdnFolderCache.clear(); }

async function cdnUploadFile(filename, buffer) {
  var folderName = _findAvailableFolder();
  if (!folderName) {
    throw new Error('Semua folder CDN penuh (maks ' + (CDN_MAX_FOLDERS * CDN_MAX_FILES_PER_FOLDER) + ' file).');
  }

  var fp = path.join(CDN_DIR, folderName);
  fs.mkdirSync(fp, { recursive: true });
  fs.writeFileSync(path.join(fp, filename), buffer);

  _invalidateFolder(folderName);

  console.log('[cdn/local] uploaded:', filename, '|',
    (buffer.length / 1024 / 1024).toFixed(2) + 'MB',
    '→ data/cdn/' + folderName + '/' + filename);

  return '/cdn/' + filename;
}

function cdnReadFile(filename) {
  var found = _findFile(filename);
  if (!found) return null;
  try { return fs.readFileSync(found.filePath); } catch(e) { return null; }
}

async function cdnDeleteFile(filename) {
  var found = _findFile(filename);
  if (!found) return false;
  try {
    fs.unlinkSync(found.filePath);
    _invalidateFolder(found.folder);
    return true;
  } catch(e) {
    console.error('[cdn/local] delete error:', e.message);
    return false;
  }
}

function cdnListFiles() {
  var cached = _cdnFolderCache.get('list');
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) return cached.data;

  var all = [];
  for (var i = 1; i <= CDN_MAX_FOLDERS; i++) {
    var fn = _folderName(i);
    var fp = _folderPath(i);
    if (!fs.existsSync(fp)) continue;
    try {
      fs.readdirSync(fp).forEach(function(name) {
        var filePath = path.join(fp, name);
        var stat;
        try { stat = fs.statSync(filePath); } catch(e) { return; }
        if (!stat.isFile()) return;
        all.push({
          name    : name,
          url     : '/cdn/' + name,
          size    : stat.size,
          path    : 'data/cdn/' + fn + '/' + name,
          folder  : fn,
          sha     : crypto.createHash('md5').update(name + stat.size).digest('hex').slice(0, 8),
          download: '/cdn/' + name,
        });
      });
    } catch(e) {  }
  }

  _cdnFolderCache.set('list', { data: all, timestamp: Date.now() });
  return all;
}

function cdnStorageStats() {
  var files = cdnListFiles(), folders = {}, totalSize = 0;
  files.forEach(function(f) {
    if (!folders[f.folder]) folders[f.folder] = { files: 0, sizeBytes: 0 };
    folders[f.folder].files++;
    folders[f.folder].sizeBytes += f.size || 0;
    totalSize += f.size || 0;
  });
  return { backend:'local', dataDir:CDN_DIR, totalFiles:files.length, totalSizeBytes:totalSize, folders:folders };
}

function cdnGetMime(ext) {
  var e    = ext.replace(/^\./, '').toLowerCase();
  var mime = CDN_MIME[e] || 'application/octet-stream';
  if (CDN_TEXT_EXTS.has(e)) mime += '; charset=utf-8';
  return mime;
}

module.exports = {
  _cdnAccounts, _cdnInvalidateCache, _cdnSanitizeError, _cdnFolderCache,
  cdnUploadFile, cdnReadFile, cdnGetDirectUrl: null,
  cdnDeleteFile, cdnListFiles, cdnStorageStats,
  cdnGetMime,
  CDN_ALLOWED_EXT, CDN_MIME, CDN_TEXT_EXTS, CDN_DOWNLOAD_EXTS, CDN_DANGEROUS_EXTS,
  CDN_MAX_FOLDERS, CDN_MAX_FILES_PER_FOLDER, CDN_DIR,
};
