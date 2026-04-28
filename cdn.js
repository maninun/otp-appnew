'use strict';

/**
 * lib/cdn.js — CDN Backend Selector
 * ─────────────────────────────────────────────────────────────────────────────
 * Pilih backend CDN berdasarkan env CDN_MODE:
 *
 *   CDN_MODE=local      → cdn-local.js  (file di disk, default)
 *   CDN_MODE=external   → cdn-external.js (DongtubeDB via REST API)
 *
 * Jika DB_MODE=external dan CDN_MODE tidak diset, otomatis pakai external.
 *
 * Semua file lain (routes/cdn.js, dll) import dari sini —
 * TIDAK perlu diubah sama sekali.
 * ─────────────────────────────────────────────────────────────────────────────
 */

// BUG FIX: Hapus require('dotenv').config() dari sini — sudah dipanggil di
// index.js. Redundant dan menyesatkan.

var cdnMode = (process.env.CDN_MODE || '').toLowerCase().trim();

// Auto-detect: kalau DB_MODE=external dan CDN_MODE tidak diset → pakai external
if (!cdnMode) {
  var dbMode = (process.env.DB_MODE || 'local').toLowerCase().trim();
  cdnMode = dbMode === 'external' ? 'external' : 'local';
}

if (cdnMode !== 'local' && cdnMode !== 'external') {
  console.warn('[CDN] ⚠️  CDN_MODE tidak dikenali ("' + cdnMode + '"), fallback ke local');
  cdnMode = 'local';
}

console.log('[CDN] 🔧 Mode CDN: ' + cdnMode.toUpperCase());

if (cdnMode === 'external') {
  module.exports = require('./cdn-external');
} else {
  module.exports = require('./cdn-local');
}
