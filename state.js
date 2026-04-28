'use strict';

/**
 * state.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Shared in-memory state yang digunakan lintas modul tanpa circular dependency.
 *
 * Sebelumnya routes/user.js dan routes/otp.js mengakses piggybackState via
 * require('../index').piggybackState — anti-pattern circular dependency.
 * Sekarang semua modul cukup require('./state') atau require('../lib/state').
 */

const piggybackState = {
  _lastPiggybackRun : 0,
  _piggybackRunning : false,
  _lastPanelCronRun : 0,
};

module.exports = { piggybackState };
