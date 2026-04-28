'use strict';

const crypto = require('crypto');
const { dbRead, dbWrite, dbDelete, listDirCached, _gitTreeCache } = require('./db');
const C = require('./config');
const { TRX_RE, DEP_RE, OTPORD_RE, isValidId, isValidDepId, isValidOtpId, newId, _sleep } = require('./utils');

function idrFormat(n) { return 'Rp' + Number(n).toLocaleString('id-ID'); }
function maskPhone(phone) {
  var p = String(phone || '').replace(/\D/g, '');
  var len = p.length;
  if (!len) return '';
  if (len <= 8) return p.slice(0,2) + '***' + p.slice(-2);
  if (len <= 10) return p.slice(0,3) + '***' + p.slice(-3);
  return p.slice(0,5) + '***' + p.slice(-5);
}

const USER_RE = /^[a-z0-9_]{3,20}$/;
function isValidUsername(u) { return USER_RE.test(u); }

var _analyticsCache = null, _analyticsCacheAt = 0;
const ANALYTICS_TTL = 5 * 60 * 1000;
function invalidateAnalyticsCache() { _analyticsCache = null; _analyticsCacheAt = 0; }

async function getProducts()       { const r = await dbRead('products.json'); return r.data || []; }

async function decrementStock(productId, variantId) {
  const MAX_RETRY = 5;
  for (var _dsi = 0; _dsi < MAX_RETRY; _dsi++) {
    try {
      const r = await dbRead('products.json', true);
      if (!r.data) return;
      const products = r.data;
      const pi = products.findIndex(function(p){ return p.id === productId; });
      if (pi < 0) return;
      const vi = products[pi].variants ? products[pi].variants.findIndex(function(v){ return v.id === variantId; }) : -1;
      if (vi < 0) return;
      const cur = products[pi].variants[vi].stock;
      if (cur === undefined || cur === null || cur < 0) return;
      if (cur === 0) return;
      products[pi].variants[vi].stock = cur - 1;
      await dbWrite('products.json', products, r.sha, 'stock-decrement:' + variantId);
      console.log('[stock] decremented:', variantId, '|', cur, '->', cur - 1);
      return;
    } catch(e) {
      if (_dsi < MAX_RETRY - 1 && (e.status === 409 || (e.message && e.message.includes('conflict')))) {
        await _sleep(150 * (_dsi + 1));
        continue;
      }
      console.warn('[stock] decrement failed (non-critical):', e.message);
      return;
    }
  }
}

function _acctPath(productId, variantId) {
  var pid = String(productId||'').replace(/[^a-zA-Z0-9_\-]/g,'').slice(0,64);
  var vid = String(variantId||'').replace(/[^a-zA-Z0-9_\-]/g,'').slice(0,64);
  if (!pid || !vid) throw new Error('Invalid product/variant ID');
  return 'accounts/' + pid + '_' + vid + '.json';
}
async function getAccounts(productId, variantId) { return dbRead(_acctPath(productId, variantId), true); }
async function saveAccounts(productId, variantId, arr, sha) {
  return dbWrite(_acctPath(productId, variantId), arr, sha, 'accounts:' + variantId);
}
async function popAccount(productId, variantId) {
  for (var _i = 0; _i < 3; _i++) {
    try {
      const r = await getAccounts(productId, variantId);
      if (!r.data || !Array.isArray(r.data) || r.data.length === 0) return null;
      const account = r.data[0];
      await saveAccounts(productId, variantId, r.data.slice(1), r.sha);
      return account;
    } catch(e) {
      if (_i < 2 && (e.status === 409 || (e.response && e.response.status === 409))) { await _sleep(400 * (_i + 1)); continue; }
      throw e;
    }
  }
  return null;
}

async function getSettings()       { const r = await dbRead('settings.json', true); return r.data || {}; }
async function getPanelTemplates() {
  try { const r = await dbRead('panel-templates.json'); return Array.isArray(r.data) ? r.data : []; }
  catch(e) { return []; }
}
async function getEffectiveSettings() {
  const s = await getSettings();
  return {
    storeName   : s.storeName    || C.store.name,
    wa          : s.wa           || C.store.wa,
    channelWa   : s.channelWa    || C.store.channel,
    expiryMin   : s.expiryMin    || C.store.expiry,
    logoUrl     : s.logoUrl      || C.store.logoUrl,
    appLogoUrl  : s.appLogoUrl   || C.store.appLogoUrl,
    announcement: s.announcement || '',
    footerText  : s.footerText   || '',
    primaryColor: s.primaryColor || C.store.primaryColor || '#34d399',
    tiktok      : s.tiktok       || C.store.tiktok    || '',
    instagram   : s.instagram    || C.store.instagram  || '',
    description : s.description  || C.store.description || '',
    otpEnabled  : s.otpEnabled   !== false,
    panelEnabled: s.panelEnabled !== false,
    maintenanceMode: s.maintenanceMode || false,
    maintenanceMsg : s.maintenanceMsg  || 'Sedang dalam maintenance.',
    musicUrl    : s.musicUrl    || '',
    musicEnabled: s.musicEnabled === true,
    captchaEnabled: s.captchaEnabled === true,
    depositFeeType: s.depositFeeType || 'flat',
    depositFee    : s.depositFee  != null ? parseFloat(s.depositFee)  : 0,
    depositMin    : s.depositMin  != null ? parseInt(s.depositMin, 10)    : 1000,
    otpMarkup     : s.otpMarkup   != null ? parseFloat(s.otpMarkup)   : 0,
    bgUrl     : s.bgUrl      || '',
    bgType    : s.bgType     || 'image',
    bgOpacity : s.bgOpacity  != null ? parseFloat(s.bgOpacity) : 0.15,
    customFields: Array.isArray(s.customFields) ? s.customFields : [],
    phoneRequired: s.phoneRequired === true,
    phoneEnabled : s.phoneEnabled  !== false,
  };
}

async function getTrx(id)          { return dbRead('transactions/' + id + '.json'); }
async function saveTrx(id, d, sha) {
  const clean = Object.assign({}, d);
  if (clean.pakData && clean.pakData.api_key) clean.pakData = { _ok: true, amount: clean.pakData.amount || null };
  invalidateAnalyticsCache();
  return dbWrite('transactions/' + id + '.json', clean, sha, 'trx:' + id + ':' + d.status);
}
async function listTrx() {
  try { return await listDirCached('transactions'); } catch(e) { return []; }
}

async function getUser(username)         { return dbRead('users/' + username + '.json'); }
async function saveUser(username, d, sha){ return dbWrite('users/' + username + '.json', d, sha, 'user:' + username); }
async function listUsers() {
  try { return await listDirCached('users'); } catch(e) { return []; }
}

async function updateBalance(username, delta, maxRetries) {
  var retries = maxRetries || 3;
  for (var i = 0; i < retries; i++) {
    try {
      const r = await getUser(username);
      if (!r.data) throw new Error('User tidak ditemukan: ' + username);
      const newBal = (r.data.balance || 0) + delta;
      if (newBal < 0) throw new Error('Saldo tidak cukup. Saldo: ' + idrFormat(r.data.balance || 0));
      await saveUser(username, Object.assign({}, r.data, { balance: newBal, updatedAt: Date.now() }), r.sha);
      return newBal;
    } catch(e) {
      if (i === retries - 1) throw e;
      var is409 = (e.status === 409) || (e.response && e.response.status === 409) ||
                  (e.message && e.message.toLowerCase().includes('conflict'));
      if (!is409) throw e;
      await new Promise(function(r){ setTimeout(r, 300 * (i+1)); });
    }
  }
}

async function getDeposit(id)           { return dbRead('deposits/' + id + '.json'); }
async function saveDeposit(id, d, sha)  { invalidateAnalyticsCache(); return dbWrite('deposits/' + id + '.json', d, sha, 'deposit:' + id + ':' + (d.status||'?')); }

async function getOtpOrder(id)          { return dbRead('otp-orders/' + id + '.json'); }
async function saveOtpOrder(id, d, sha) { return dbWrite('otp-orders/' + id + '.json', d, sha, 'otporder:' + id + ':' + (d.status||'?')); }
async function listOtpOrders(username) {
  try {
    const files = (await listDirCached('otp-orders')).filter(function(f){ return f.name.endsWith('.json'); });
    const results = [];
    await Promise.all(files.map(async function(f) {
      try {
        const o = await getOtpOrder(f.name.replace('.json',''));

        if (o.data && (username === null || o.data.username === username)) results.push(o.data);
      } catch(e) {}
    }));
    return results.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
  } catch(e) { return []; }
}

async function getSewabotOrder(id)          { return dbRead('sewabot-orders/' + id + '.json'); }
async function saveSewabotOrder(id, d, sha) { return dbWrite('sewabot-orders/' + id + '.json', d, sha, 'sewabot:' + id + ':' + (d.status||'?')); }
async function listSewabotOrders() {
  try {
    const files = (await listDirCached('sewabot-orders')).filter(function(f){ return f.name.endsWith('.json'); });
    const results = [];
    await Promise.all(files.map(async function(f) {
      try { const o = await getSewabotOrder(f.name.replace('.json','')); if(o.data) results.push(o.data); } catch(e) {}
    }));
    return results.sort(function(a,b){ return (b.createdAt||0)-(a.createdAt||0); });
  } catch(e) { return []; }
}

async function getRsServer(id)          { return dbRead('reseller-servers/' + id + '.json'); }
async function saveRsServer(id, d, sha) { return dbWrite('reseller-servers/' + id + '.json', d, sha, 'rsserver:' + id); }

async function getVouchers()       { const r = await dbRead('vouchers.json'); return Array.isArray(r.data) ? r.data : []; }
async function getReviews()        { return dbRead('reviews.json'); }
async function saveReviews(arr, sha){ return dbWrite('reviews.json', arr, sha, 'reviews'); }
async function getChatMessages()   { return dbRead('chat-messages.json'); }
async function saveChatMessages(arr, sha){ return dbWrite('chat-messages.json', arr, sha, 'chat-messages'); }

async function auditLog(action, detail, ip) {
  try {
    const r = await dbRead('audit.json');
    const log = Array.isArray(r.data) ? r.data : [];
    log.unshift({ ts: Date.now(), action, detail: String(detail || '').slice(0, 300), ip });
    if (log.length > 500) log.length = 500;
    await dbWrite('audit.json', log, r.sha || null, 'audit:' + action);
  } catch(e) { console.warn('[audit]', e.message); }
}

async function resolveContent(trx) {
  const products = await getProducts();
  const prod     = products.find(function(p) { return p.id === trx.productId; });
  const vari     = prod && prod.variants.find(function(v) { return v.id === trx.variantId; });
  if (!vari) return { type: 'digital', contentType: 'text', message: 'Produk ditemukan. Hubungi admin untuk detail. ID: ' + trx.id };
  const ct = vari.contentType || 'text';
  return {
    type       : 'digital',
    contentType: ct,
    contentUrl : vari.contentUrl || vari.fileUrl || '',
    contentText: vari.contentText || vari.content || '',
    title      : vari.name || trx.variantName,
    description: vari.description || '',
    filename   : vari.filename || '',
  };
}

async function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  return new Promise(function(resolve, reject) {
    crypto.scrypt(password, salt, 32, function(err, dk) {
      if (err) reject(err);
      else resolve(salt + ':' + dk.toString('hex'));
    });
  });
}
async function verifyPassword(password, stored) {
  if (!stored || !stored.includes(':')) return false;
  const [salt, hash] = stored.split(':');
  if (!salt || !hash) return false;
  return new Promise(function(resolve, reject) {
    crypto.scrypt(password, salt, 32, function(err, dk) {
      if (err) { resolve(false); return; }
      try { resolve(crypto.timingSafeEqual(Buffer.from(hash, 'hex'), dk)); }
      catch(e) { resolve(false); }
    });
  });
}

module.exports = {
  isValidId, isValidDepId, isValidOtpId, newId, _sleep,
  idrFormat, maskPhone, isValidUsername,
  invalidateAnalyticsCache,
  getProducts, decrementStock, getAccounts, saveAccounts, popAccount,
  getSettings, getPanelTemplates, getEffectiveSettings,
  getTrx, saveTrx, listTrx,
  getUser, saveUser, listUsers, updateBalance,
  getDeposit, saveDeposit, listDirCached,
  getOtpOrder, saveOtpOrder, listOtpOrders,
  getSewabotOrder, saveSewabotOrder, listSewabotOrders,
  getRsServer, saveRsServer,
  getVouchers, getReviews, saveReviews, getChatMessages, saveChatMessages,
  auditLog, resolveContent, hashPassword, verifyPassword,
};
