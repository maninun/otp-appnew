'use strict';

/**
 * lib/db.js — Database Selector
 * ─────────────────────────────────────────────────────────────────────────────
 * Pilih backend database berdasarkan env DB_MODE:
 *
 *   DB_MODE=local      → db-local.js  (JSON file di disk, default)
 *   DB_MODE=external   → db-external.js (DongtubeDB via REST API)
 *
 * Tambahan env untuk mode external:
 *   DTDB_URL     = https://dongtube.my.id
 *   DTDB_API_KEY = <admin API key dari panel DongtubeDB>
 *
 * Semua file lain (routes, lib/models, dll) import dari sini —
 * TIDAK perlu diubah sama sekali.
 * ─────────────────────────────────────────────────────────────────────────────
 */

// BUG FIX: Hapus require('dotenv').config() dari sini — sudah dipanggil di
// index.js sebelum modul apapun di-require. Pemanggilan ulang tidak crash
// (no-op) tapi redundant dan menyesatkan.

var mode = (process.env.DB_MODE || 'local').toLowerCase().trim();

if (mode !== 'local' && mode !== 'external' && mode !== 'supabase') {
  console.warn('[DB] ⚠️  DB_MODE tidak dikenali ("' + mode + '"), fallback ke local');
  mode = 'local';
}

console.log('[DB] 🔧 Mode database: ' + mode.toUpperCase());

if (mode === 'external') {
  module.exports = require('./db-external');
} else if (mode === 'supabase') {
  module.exports = require('./db-supabase');
} else {
  module.exports = require('./db-local');
}
