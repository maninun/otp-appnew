'use strict';

/**
 * event-trigger.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Menggantikan cron global dengan background watch per-item.
 * Setiap watch hidup sendiri, bersih sendiri saat item sudah terminal.
 *
 * Trigger dipanggil tepat setelah user melakukan aksi:
 *   - triggerTrxWatch(trxId)         → dipanggil setelah order dibuat
 *   - triggerOtpOrderWatch(orderId)  → dipanggil setelah OTP order dibuat
 *   - recoverOnStartup()             → dipanggil saat server boot
 */

// ── State registry (in-memory) ────────────────────────────────────────────────
const _activeTrxWatches = new Map();   // trxId  → intervalId
const _activeOtpWatches = new Map();   // ordId  → intervalId

// ─────────────────────────────────────────────────────────────────────────────
// 1.  TRX WATCH
//     Polling per-transaksi setiap 20 detik.
//     - Cek apakah sudah dibayar via payment gateway
//     - Auto-expire jika melebihi waktu + 2 menit buffer
//     - Berhenti otomatis saat status terminal
// ─────────────────────────────────────────────────────────────────────────────
function triggerTrxWatch(trxId) {
  if (_activeTrxWatches.has(trxId)) return; // sudah diawasi

  var tries    = 0;
  var pgwTick  = 0;
  const MAX_TRIES = 100; // 100 × 20s ≈ 33 menit (lebih dari expiry maks 30 menit)

  const iv = setInterval(async function() {
    tries++;
    if (tries >= MAX_TRIES) { _clearTrx(trxId, iv); return; }

    try {
      const { getTrx, saveTrx, decrementStock } = require('./models');
      const { pgw, _pgwConfigured }             = require('./payment');
      const { processProductDelivery }           = require('./delivery');
      const { broadcastAdmin }                   = require('./broadcast');

      const r = await getTrx(trxId);
      if (!r || !r.data) { _clearTrx(trxId, iv); return; }

      const trx = r.data;
      const sha = r.sha;

      // ── Terminal: hentikan watcher ────────────────────────────────────────
      if (['COMPLETED', 'PAID_ERROR', 'FAILED', 'EXPIRED'].includes(trx.status)) {
        _clearTrx(trxId, iv); return;
      }

      // ── Masih PROCESSING: tunggu saja ─────────────────────────────────────
      if (trx.status === 'PROCESSING') return;

      // ── Auto-expire ───────────────────────────────────────────────────────
      if (trx.expiryAt && Date.now() > trx.expiryAt + 2 * 60 * 1000) {
        if (!trx.demo) {
          try { await pgw.cancel(trxId, trx.unitPrice, trx.totalBayar, trx.pakData); } catch(e) {}
        }
        await saveTrx(trxId,
          Object.assign({}, trx, { status: 'EXPIRED', expiredAutoAt: Date.now() }), sha);
        _clearTrx(trxId, iv); return;
      }

      // ── Demo / tidak ada PGW: skip cek bayar ─────────────────────────────
      if (trx.demo || !_pgwConfigured()) return;

      // ── Cek PGW setiap 2 tick (≈40s) agar tidak overload ─────────────────
      pgwTick++;
      if (pgwTick % 2 !== 0) return;

      try {
        const pakRes    = await pgw.check(trxId, trx.unitPrice, trx.totalBayar, trx.createdAt, trx.pakData);
        const pakStatus = _extractPgwStatus(pakRes);

        if (pakStatus === 'completed' || pakStatus === 'paid' || pakStatus === 'success') {
          // ── Lock dulu untuk cegah race dengan SSE stream ──────────────────
          try {
            await saveTrx(trxId,
              Object.assign({}, trx, { status: 'PROCESSING', processingAt: Date.now() }), sha);
          } catch(lockErr) { return; } // SSE mungkin sudah lock — skip tick ini

          try {
            const result = await processProductDelivery(trx, trxId);
            const freshR = await getTrx(trxId);
            await saveTrx(trxId,
              Object.assign({}, freshR.data || trx, { status: 'COMPLETED', result, completedAt: Date.now() }),
              freshR.sha || null);
            decrementStock(trx.productId, trx.variantId).catch(function(){});
            broadcastAdmin({
              type: 'trx_completed', id: trxId,
              productName: trx.productName, variantName: trx.variantName,
              totalBayar: trx.totalBayar || trx.unitPrice,
              productType: trx.productType, phone: trx.phone || null, ts: Date.now(),
            });
          } catch(procErr) {
            const errResult = {
              type: 'error',
              message: 'Pembayaran diterima tapi proses gagal. Hubungi admin. ID: ' + trxId,
            };
            const freshR2 = await getTrx(trxId);
            await saveTrx(trxId,
              Object.assign({}, freshR2.data || trx, {
                status: 'PAID_ERROR', error: procErr.message, result: errResult,
              }), freshR2.sha || null);
          }
          _clearTrx(trxId, iv); return;

        } else if (pakStatus === 'failed' || pakStatus === 'canceled' || pakStatus === 'cancelled') {
          await saveTrx(trxId,
            Object.assign({}, trx, { status: 'FAILED', expiredAutoAt: Date.now() }), sha);
          _clearTrx(trxId, iv); return;
        }
      } catch(pgwErr) { /* gateway sementara error — retry tick berikutnya */ }

    } catch(e) { /* silent — jangan crash interval */ }
  }, 20 * 1000);
  iv.unref(); // jangan blokir graceful shutdown

  _activeTrxWatches.set(trxId, iv);
}

function _clearTrx(trxId, iv) {
  clearInterval(iv);
  _activeTrxWatches.delete(trxId);
}

function _extractPgwStatus(pakRes) {
  return (
    (pakRes && pakRes.transaction && pakRes.transaction.status) ||
    (pakRes && pakRes.data && pakRes.data.status) ||
    (pakRes && pakRes.status) || ''
  ).toLowerCase();
}

// ─────────────────────────────────────────────────────────────────────────────
// 2.  OTP ORDER WATCH
//     Monitor per-order OTP. Setelah expiresAt, trigger refund otomatis.
//     Interval 15 detik, max 80 tick (≈20 menit).
// ─────────────────────────────────────────────────────────────────────────────
const _OTP_REFUND_BUFFER_MS = 30 * 1000; // 30s setelah expiresAt baru proses

function triggerOtpOrderWatch(orderId) {
  if (_activeOtpWatches.has(orderId)) return;

  var tries = 0;
  const MAX_TRIES = 80;

  const iv = setInterval(async function() {
    tries++;
    if (tries >= MAX_TRIES) { _clearOtp(orderId, iv); return; }

    try {
      const { getOtpOrder, saveOtpOrder, updateBalance } = require('./models');
      const rotp = require('./rotp');

      const d = await getOtpOrder(orderId);
      if (!d || !d.data) { _clearOtp(orderId, iv); return; }
      const ord = d.data;

      // ── Terminal & sudah di-refund ────────────────────────────────────────
      if (['completed', 'canceled', 'expired'].includes(ord.status) && ord.refunded) {
        _clearOtp(orderId, iv); return;
      }

      // ── Completed tanpa perlu refund ──────────────────────────────────────
      if (ord.status === 'completed') { _clearOtp(orderId, iv); return; }

      // ── Masih menunggu & belum expire ─────────────────────────────────────
      const isOverdue = ord.createdAt && ord.createdAt < Date.now() - (7.5 * 60 * 1000);
      const isExpired = ord.expiresAt && Date.now() > ord.expiresAt + _OTP_REFUND_BUFFER_MS;
      if (!isOverdue && !isExpired) return;

      // ── Sudah expired — proses refund ─────────────────────────────────────
      if (ord.status !== 'waiting' && ord.status !== 'expired') return; // state lain diurus di tempat lain

      // Tandai expiring dulu untuk lock
      try {
        await saveOtpOrder(orderId,
          Object.assign({}, ord, { status: 'expiring', expiringAt: Date.now() }), d.sha);
      } catch(lockErr) { return; } // ada proses lain yang lock — skip

      // Cancel di provider
      try { await rotp.cancelOrder(ord.rotpOrderId); } catch(e) {}

      // Refund saldo
      let refunded = false;
      try {
        if (!ord.balanceRefunded) {
          const preFr = await getOtpOrder(orderId).catch(function(){ return { data: ord, sha: null }; });
          await saveOtpOrder(orderId,
            Object.assign({}, preFr.data || ord, { balanceRefunded: true, _refundingAt: Date.now() }),
            preFr.sha || null).catch(function(){});
          await updateBalance(ord.username, ord.price);
        }
        refunded = true;
      } catch(balErr) {
        console.error('[otp-watch] refund GAGAL:', orderId, balErr.message);
      }

      const freshFr = await getOtpOrder(orderId).catch(function(){ return { data: ord, sha: null }; });
      await saveOtpOrder(orderId,
        Object.assign({}, freshFr.data || ord, {
          status: 'expired', refunded, balanceRefunded: true,
          refundedAt: refunded ? Date.now() : null, expiredAutoAt: Date.now(),
        }), freshFr.sha || null);

      _clearOtp(orderId, iv);
    } catch(e) { /* silent */ }
  }, 15 * 1000);
  iv.unref(); // jangan blokir graceful shutdown

  _activeOtpWatches.set(orderId, iv);
}

function _clearOtp(orderId, iv) {
  clearInterval(iv);
  _activeOtpWatches.delete(orderId);
}


// ─────────────────────────────────────────────────────────────────────────────
// 4.  STARTUP RECOVERY
//     Saat server restart, re-register semua item pending agar tidak
//     ada yang "terlupakan". Hanya berjalan di non-Vercel (Node server).
// ─────────────────────────────────────────────────────────────────────────────
async function recoverOnStartup() {
  // Di Vercel (serverless), setTimeout/setInterval tidak persist → skip
  if (process.env.VERCEL) return;

  // Delay 5 detik agar DB/modul sudah siap
  await new Promise(function(resolve){ setTimeout(resolve, 5000); });

  var trxRecovered   = 0;
  var otpRecovered   = 0;

  try {
    const { listTrx, getTrx, listDirCached, getOtpOrder, getDeposit, saveDeposit, updateBalance } = require('./models');
    const { broadcastAdmin } = require('./broadcast');
    const C = require('./config');

    // ── Recover pending TRX watches ─────────────────────────────────────────
    try {
      const trxFiles = await listTrx();
      for (const f of trxFiles.filter(function(f){ return f.name.endsWith('.json'); })) {
        try {
          const r = await getTrx(f.name.replace('.json', ''));
          if (!r || !r.data) continue;
          if (r.data.status === 'PENDING') {
            triggerTrxWatch(r.data.id);
            trxRecovered++;
          }
        } catch(e) {}
      }
    } catch(e) {}

    // ── Recover pending OTP watches ─────────────────────────────────────────
    try {
      const otpFiles = (await listDirCached('otp-orders'))
        .filter(function(f){ return f.name.endsWith('.json'); });
      for (const f of otpFiles) {
        try {
          const d = await getOtpOrder(f.name.replace('.json', ''));
          if (!d || !d.data) continue;
          const s = d.data.status;
          if (s === 'waiting' || s === 'expiring' || s === 'canceling') {
            triggerOtpOrderWatch(d.data.id);
            otpRecovered++;
          }
        } catch(e) {}
      }
    } catch(e) {}


    // ── Recover pending deposit watches ─────────────────────────────────────
    // Deposit yang dibuat saat server mati tidak punya watcher aktif.
    // Re-register watcher per-item tanpa sweep global.
    try {
      const depFiles = (await listDirCached('deposits'))
        .filter(function(f){ return f.name.endsWith('.json'); });
      var depRecovered = 0;
      for (const f of depFiles) {
        try {
          const { getDeposit } = require('./models');
          const d = await getDeposit(f.name.replace('.json', ''));
          if (!d || !d.data) continue;
          if (d.data.status === 'pending' || d.data.status === 'crediting') {
            triggerDepositWatch(d.data.id);
            depRecovered++;
          }
        } catch(e) {}
      }
      if (depRecovered > 0) console.log('[event-trigger] deposit watches recovered:', depRecovered);
    } catch(e) {}

    // ── Recover reseller server timers ──────────────────────────────────────
    var rsScheduled = 0;
    if (C.ptero && C.ptero.domain && C.ptero.apikey) {
      try {
        const { listDirCached: ldc2, getRsServer } = require('./models');
        const rsFiles = (await ldc2('reseller-servers'))
          .filter(function(f){ return f.name.endsWith('.json'); });
        for (const f of rsFiles) {
          try {
            const r = await getRsServer(f.name.replace('.json', ''));
            if (!r || !r.data) continue;
            const s = r.data;
            if (s.status !== 'active' || !s.expiresAt || !s.serverId) continue;
            triggerRsServerExpiry(s.id || f.name.replace('.json',''), s.serverId, s.userId || null, s.expiresAt);
            rsScheduled++;
          } catch(e) {}
        }
      } catch(e) {}
    }

    // ── Lazy deposit cleanup sekali saat startup ─────────────────────────────
    setTimeout(function() {
      runLazyDepositCleanup().catch(function(){});
    }, 15000);

    console.log(
      '[event-trigger] recovery selesai | trx:', trxRecovered,
      '| otp:', otpRecovered,       '| rs-timers:', rsScheduled
    );
  } catch(e) {
    console.warn('[event-trigger] recovery error:', e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Exports (base — diperluas oleh Object.assign di bawah untuk fungsi tambahan)
// ─────────────────────────────────────────────────────────────────────────────
module.exports = {
  triggerTrxWatch,
  triggerOtpOrderWatch,
  recoverOnStartup,
};

// ─────────────────────────────────────────────────────────────────────────────
// 5.  RESELLER SERVER EXPIRY
//     Sama seperti triggerPanelExpiry tapi untuk reseller-servers.
//     Dipanggil dari routes/reseller.js setelah server dibuat / diperpanjang.
// ─────────────────────────────────────────────────────────────────────────────
const _rsServerTimers = new Map(); // rsServerId → timeoutId

function triggerRsServerExpiry(rsServerId, serverId, userId, expiresAt) {
  // Hapus timer lama jika ada (misalnya setelah perpanjangan)
  if (_rsServerTimers.has(rsServerId)) {
    clearTimeout(_rsServerTimers.get(rsServerId));
    _rsServerTimers.delete(rsServerId);
  }

  const msUntilExpiry = expiresAt - Date.now();
  if (msUntilExpiry < 0) {
    _doSuspendRsServer(rsServerId, serverId, userId);
    return;
  }

  const t = setTimeout(function() {
    _rsServerTimers.delete(rsServerId);
    _doSuspendRsServer(rsServerId, serverId, userId);
  }, msUntilExpiry + 60 * 1000);
  t.unref();

  _rsServerTimers.set(rsServerId, t);
}

async function _doSuspendRsServer(rsServerId, serverId, userId) {
  try {
    const C       = require('./config');
    const axios   = require('axios');
    const { ptH } = require('./panel');
    const { getRsServer, saveRsServer } = require('./models');
    const { _gitTreeCache } = require('./db');

    if (!C.ptero || !C.ptero.domain || !C.ptero.apikey) return;
    const dom     = C.ptero.domain;
    const headers = ptH();

    const r = await getRsServer(rsServerId);
    if (!r || !r.data || r.data.status === 'suspended' || r.data.status === 'deleted') return;

    // Suspend
    try {
      await axios.post(dom + '/api/application/servers/' + serverId + '/suspend', {}, { headers });
      await saveRsServer(rsServerId,
        Object.assign({}, r.data, { status: 'suspended', _autoSuspendedAt: Date.now() }), r.sha);
      _gitTreeCache.delete('reseller-servers');
      console.log('[rs-expiry] suspended:', rsServerId, 'server:', serverId);
    } catch(e) {
      if (e.response && e.response.status === 404) {
        await saveRsServer(rsServerId,
          Object.assign({}, r.data, { status: 'deleted', _deletedAt: Date.now(), _deletedBy: 'expiry-404' }),
          r.sha).catch(function(){});
        _gitTreeCache.delete('reseller-servers');
      }
      return;
    }

    // Delete 3 hari setelah suspended
    var _rsDelTimer = setTimeout(async function() {
      try {
        const r2 = await getRsServer(rsServerId);
        if (!r2 || !r2.data || r2.data.status === 'deleted') return;
        try { await axios.delete(dom + '/api/application/servers/' + serverId, { headers }); } catch(e) {}
        try { if (userId) await axios.delete(dom + '/api/application/users/' + userId, { headers }); } catch(e) {}
        const fresh = await getRsServer(rsServerId).catch(function(){ return r2; });
        await saveRsServer(rsServerId,
          Object.assign({}, fresh.data || r2.data, { status: 'deleted', _deletedAt: Date.now(), _deletedBy: 'rs-expiry' }),
          fresh.sha || r2.sha || null);
        _gitTreeCache.delete('reseller-servers');
        console.log('[rs-expiry] deleted:', rsServerId, 'server:', serverId);
      } catch(e) {}
    }, 3 * 24 * 60 * 60 * 1000);
    _rsDelTimer.unref(); // jangan blokir graceful shutdown

  } catch(e) {
    console.warn('[rs-expiry] suspend error:', rsServerId, e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6.  DEPOSIT WATCH
//     Per-item deposit reconcile. Dipanggil tepat setelah deposit dibuat.
//     Polling setiap 30 detik, max 42 tick (≈21 menit).
//     Otomatis berhenti saat status terminal (success/cancel).
// ─────────────────────────────────────────────────────────────────────────────
const _activeDepWatches = new Map(); // depId → intervalId

function triggerDepositWatch(depId) {
  if (_activeDepWatches.has(depId)) return; // sudah diawasi

  var tries = 0;
  const MAX_TRIES = 42; // 42 × 30s ≈ 21 menit

  const iv = setInterval(async function() {
    tries++;
    if (tries >= MAX_TRIES) { _clearDep(depId, iv); return; }

    try {
      const { getDeposit, saveDeposit, updateBalance } = require('./models');
      const { broadcastAdmin } = require('./broadcast');
      const rotp = require('./rotp');

      const d = await getDeposit(depId);
      if (!d || !d.data) { _clearDep(depId, iv); return; }
      const dep = d.data;

      // ── Terminal — hentikan watcher ───────────────────────────────────────
      if (dep.status === 'success' || dep.status === 'cancel') {
        _clearDep(depId, iv); return;
      }

      // ── Crediting: saldo sudah dikreditkan, finalkan ──────────────────────
      if (dep.status === 'crediting') {
        if (dep.balanceCredited) {
          const fr = await getDeposit(depId).catch(function(){ return { data: dep, sha: d.sha }; });
          await saveDeposit(depId,
            Object.assign({}, fr.data || dep, { status: 'success', reconciledAt: Date.now() }),
            fr.sha || null).catch(function(){});
          _clearDep(depId, iv); return;
        }
        return; // tunggu balance credited
      }

      // ── Expired + buffer 3 menit ──────────────────────────────────────────
      if (dep.expiredAt && Date.now() > dep.expiredAt + 3 * 60 * 1000) {
        _clearDep(depId, iv); return;
      }

      // ── Cek status di provider ────────────────────────────────────────────
      var srRes = await rotp.depositStatus(dep.rotpId).catch(function(){ return null; });
      var rStatus = (srRes && srRes.success && srRes.data) ? srRes.data.status : null;

      if (rStatus === 'success') {
        // Lock dulu untuk cegah race condition
        try {
          await saveDeposit(depId,
            Object.assign({}, dep, { status: 'crediting', creditingAt: Date.now(), balanceCredited: true }),
            d.sha);
        } catch(lockErr) { return; } // ada conflict — coba lagi tick berikutnya

        var newBal;
        try {
          newBal = await updateBalance(dep.username, dep.amount);
        } catch(balErr) {
          const fr2 = await getDeposit(depId).catch(function(){ return { data: dep, sha: null }; });
          await saveDeposit(depId,
            Object.assign({}, fr2.data || dep, { status: 'pending', creditingAt: null, balanceCredited: false }),
            fr2.sha || null).catch(function(){});
          return;
        }

        const frOk = await getDeposit(depId).catch(function(){ return { data: dep, sha: null }; });
        await saveDeposit(depId,
          Object.assign({}, frOk.data || dep, {
            status: 'success', paidAt: Date.now(), creditedBalance: newBal, reconciledAt: Date.now(),
          }), frOk.sha || null);
        broadcastAdmin({ type: 'deposit_success', id: depId, username: dep.username, amount: dep.amount, ts: Date.now() });
        _clearDep(depId, iv);

      } else if (rStatus === 'cancel') {
        await saveDeposit(depId,
          Object.assign({}, dep, { status: 'cancel', cancelledAt: Date.now() }), d.sha).catch(function(){});
        _clearDep(depId, iv);
      }
    } catch(e) { /* silent — retry tick berikutnya */ }
  }, 30 * 1000);
  iv.unref();

  _activeDepWatches.set(depId, iv);
}

function _clearDep(depId, iv) {
  clearInterval(iv);
  _activeDepWatches.delete(depId);
}

// Batch: trigger watch untuk semua deposit pending (dipanggil dari admin atau recovery)
async function reconcileAllPendingDeposits() {
  try {
    const { listDirCached, getDeposit } = require('./models');
    const files = (await listDirCached('deposits'))
      .filter(function(f){ return f.name.endsWith('.json'); });
    var triggered = 0;
    for (const f of files) {
      try {
        const d = await getDeposit(f.name.replace('.json', ''));
        if (!d || !d.data) continue;
        if (d.data.status === 'pending' || d.data.status === 'crediting') {
          triggerDepositWatch(d.data.id);
          triggered++;
        }
      } catch(e) {}
    }
    if (triggered > 0) console.log('[deposit-watch] triggered watches:', triggered);
  } catch(e) { console.warn('[deposit-reconcile] error:', e.message); }
}

// ─────────────────────────────────────────────────────────────────────────────
// 7.  LAZY DEPOSIT CLEANUP
//     Hapus deposit lama (> 7 hari, status final) tanpa butuh cron.
//     Dipanggil sekali saat startup dan sesekali via admin.
// ─────────────────────────────────────────────────────────────────────────────
var _lastCleanupRun = 0;
const _CLEANUP_INTERVAL_MS = 6 * 60 * 60 * 1000; // max sekali per 6 jam

async function runLazyDepositCleanup() {
  if (Date.now() - _lastCleanupRun < _CLEANUP_INTERVAL_MS) return;
  _lastCleanupRun = Date.now();
  try {
    const { listDirCached, getDeposit } = require('./models');
    const { dbDelete, _gitTreeCache } = require('./db');
    const { _sleep } = require('./utils');
    const r = await listDirCached('deposits');
    var files = Array.isArray(r) ? r.filter(function(f){ return f.name.endsWith('.json'); }) : [];
    var WEEK = 7 * 24 * 60 * 60 * 1000; var deleted = 0;
    for (var i = 0; i < files.length; i++) {
      await _sleep(600);
      try {
        var d = await getDeposit(files[i].name.replace('.json', ''));
        if (!d.data) continue;
        var finalStatus = d.data.status === 'success' || d.data.status === 'cancel';
        var old = d.data.createdAt && (Date.now() - d.data.createdAt) > WEEK;
        if (finalStatus && old) {
          await dbDelete('deposits/' + files[i].name);
          _gitTreeCache.delete('deposits');
          deleted++;
        }
      } catch(e) {}
    }
    if (deleted > 0) console.log('[deposit/cleanup] dihapus:', deleted, 'file lama');
  } catch(e) { if (!e.response || e.response.status !== 404) console.warn('[deposit/cleanup]', e.message); }
}

// Patch module.exports di bawah — tambah fungsi-fungsi tambahan
Object.assign(module.exports, {
  triggerRsServerExpiry,
  triggerDepositWatch,
  reconcileAllPendingDeposits,
  runLazyDepositCleanup,
  _status: function() {
    return {
      activeTrxWatches : _activeTrxWatches.size,
      activeOtpWatches : _activeOtpWatches.size,
      activeDepWatches : _activeDepWatches.size,
      rsServerTimers   : _rsServerTimers.size,
    };
  },
});
