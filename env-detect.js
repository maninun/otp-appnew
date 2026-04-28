'use strict';

/**
 * env-detect.js — Auto-detect runtime environment
 *
 * Mendukung:
 *   • VPS / bare-metal  → DATA_DIR default: <app_root>/data
 *   • Pterodactyl Panel → DATA_DIR default: /home/container/data
 *   • Vercel            → read-only fs, DATA_DIR tidak berlaku (skip)
 *
 * Prioritas:
 *   1. process.env.DATA_DIR  (manual override — selalu menang)
 *   2. Auto-detect Pterodactyl  → /home/container/data
 *   3. Fallback                 → <app_root>/data  (VPS / lokal)
 */

const path = require('path');
const os   = require('os');

// ── Deteksi Pterodactyl ───────────────────────────────────────────────────────
// Pterodactyl selalu inject env vars ini ke setiap container:
//   P_SERVER_UUID, P_SERVER_LOCATION, P_SERVER_ALLOCATION
// Atau cek homedir langsung → /home/container
function _isPterodactyl() {
  if (process.env.P_SERVER_UUID)       return true;
  if (process.env.P_SERVER_LOCATION)   return true;
  if (process.env.P_SERVER_ALLOCATION) return true;
  try {
    return os.homedir() === '/home/container';
  } catch (_) {
    return false;
  }
}

// ── Deteksi Vercel (read-only fs) ────────────────────────────────────────────
function _isVercel() {
  return !!(process.env.VERCEL || process.env.VERCEL_ENV || process.env.NOW_REGION);
}

// ── Resolve DATA_DIR ─────────────────────────────────────────────────────────
function resolveDataDir() {
  // 1. Manual override
  if (process.env.DATA_DIR) return process.env.DATA_DIR;

  // 2. Vercel — kembalikan path tmp yang bisa tulis (atau biarkan caller skip)
  if (_isVercel()) return '/tmp/data';

  // 3. Pterodactyl → /home/container/data  (persistent storage)
  if (_isPterodactyl()) return '/home/container/data';

  // 4. VPS / lokal → <app_root>/data
  return path.join(__dirname, '../data');
}

// ── Info environment (untuk log startup) ────────────────────────────────────
function getRuntimeInfo() {
  if (_isVercel())      return { env: 'vercel',      label: 'Vercel' };
  if (_isPterodactyl()) return { env: 'pterodactyl', label: 'Pterodactyl Panel' };
  return                       { env: 'vps',         label: 'VPS / Lokal' };
}

const DATA_DIR    = resolveDataDir();
const RUNTIME     = getRuntimeInfo();

module.exports = { DATA_DIR, RUNTIME, resolveDataDir, getRuntimeInfo };
