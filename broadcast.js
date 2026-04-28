'use strict';

const axios = require('axios');
const { dbRead, dbWrite } = require('./db');
const { maskPhone } = require('./models');

var _storeClients = new Set();
var _adminClients = new Set();
var _feedClients  = new Set();

var _feedBuf = [];
const FEED_MAX = 50;

function maskFeedId(id) {
  if (!id || typeof id !== 'string') return '???';
  var dash = id.indexOf('-');
  var prefix = dash > 0 ? id.slice(0, dash) : id.slice(0, 3);
  return prefix + '-***-' + id.slice(-4);
}

function formatFeedEvent(raw) {
  var base = { id: maskFeedId(raw.id) + ':' + (raw.type || 'evt'), type: raw.type, ts: raw.ts, label: raw.label || '', amount: raw.amount || 0 };
  if (raw.phone)    base.phone    = raw.phone;
  if (raw.username) base.username = raw.username;
  if (raw.otp)      base.otp      = raw.otp;
  return base;
}

function formatWebhookPayload(event) {
  var payload = { id: event.id, type: event.type, ts: event.ts, label: event.label || '', amount: event.amount || 0 };
  if (event.type === 'order') {
    if (event.productName)  payload.productName  = event.productName;
    if (event.variantName)  payload.variantName  = event.variantName;
    if (event.productType)  payload.productType  = event.productType;
    if (event.phone)        payload.phone        = event.phone;
    if (event.free)         payload.free         = event.free;
  }
  if (event.type === 'deposit')  { if (event.username) payload.username = event.username; }
  if (event.type === 'sewabot') {
    if (event.groupUrl)   payload.groupUrl   = event.groupUrl;
    if (event.days)       payload.days       = event.days;
    if (event.buyerName)  payload.buyerName  = event.buyerName;
  }
  if (event.type === 'otp') {
    if (event.username) payload.username = event.username;
    if (event.service)  payload.service  = event.service;
    if (event.country)  payload.country  = event.country;
    if (event.otp)      payload.otp      = event.otp;
  }
  return payload;
}

function toBroadcastFeedEvent(d) {
  if (d.type === 'new_order') return { id: d.id, type: 'new_order', ts: d.ts, label: (d.productName || '') + (d.variantName ? ' — ' + d.variantName : ''), amount: d.totalBayar || 0, productType: d.productType || 'digital' };
  if (d.type === 'trx_completed') return { id: d.id, type: 'order', ts: d.ts, label: (d.productName || '') + (d.variantName ? ' — ' + d.variantName : ''), amount: d.totalBayar || 0, productType: d.productType || 'digital', free: !!d.free, phone: d.phone ? maskPhone(d.phone) : '' };
  if (d.type === 'deposit_success') return { id: d.id, type: 'deposit', ts: d.ts, label: 'Deposit saldo — ' + (d.username || ''), amount: d.amount || 0, username: d.username || '' };
  if (d.type === 'sewabot_completed') return { id: d.id, type: 'sewabot', ts: d.ts, label: 'Sewa Bot ' + (d.days || '?') + ' hari', amount: d.totalBayar || 0, days: d.days || null, buyerName: d.buyerName || '', groupUrl: d.groupUrl || '' };
  if (d.type === 'new_otp_order') return { id: d.id, type: 'new_otp_order', ts: d.ts, label: 'Order OTP — ' + (d.service || '') + (d.country ? ' (' + d.country + ')' : ''), amount: d.price || 0, phone: d.phone ? maskPhone(d.phone) : '', username: d.username || '', service: d.service || '', country: d.country || '' };
  if (d.type === 'otp_completed') return { id: d.id, type: 'otp', ts: d.ts, label: 'OTP — ' + (d.service || '') + (d.country ? ' (' + d.country + ')' : ''), amount: d.price || 0, username: d.username || '', service: d.service || '', country: d.country || '', phone: d.phone ? maskPhone(d.phone) : '', otp: d.otp || '' };
  return null;
}

var _webhookCache   = null;
var _webhookCacheAt = 0;

async function getWebhookConfig() {
  if (_webhookCache && Date.now() - _webhookCacheAt < 10 * 60 * 1000) return _webhookCache;
  try {
    const r = await dbRead('webhook-config.json');
    _webhookCache = r.data || { url: '', secret: '', enabled: false };
    _webhookCacheAt = Date.now();
  } catch(e) { _webhookCache = { url: '', secret: '', enabled: false }; }
  return _webhookCache;
}

async function fireWebhook(event) {
  try {
    const cfg = await getWebhookConfig();
    if (!cfg || !cfg.url || cfg.enabled === false) return;
    const payload = formatWebhookPayload(event);
    await axios.post(cfg.url, payload, { timeout: 8000, headers: { 'Content-Type': 'application/json', 'X-Dongtube-Event': event.type, 'X-Dongtube-Secret': cfg.secret || '' } });
  } catch(e) { console.warn('[webhook]', e.message); }
}

async function persistFeedEvent(event) {
  try {
    const r = await dbRead('feed-cache.json');
    const current = Array.isArray(r.data) ? r.data : [];
    current.unshift(formatFeedEvent(event));
    if (current.length > 30) current.length = 30;
    await dbWrite('feed-cache.json', current, r.sha || null, 'feed-cache');
  } catch(e) {}
}

function _sseWrite(res, msg) {
  try {
    res.write(msg);
    // BUGFIX: flush gzip/compression buffer agar event SSE langsung dikirim ke client
    if (typeof res.flush === 'function') res.flush();
    return true;
  } catch(e) {
    return false;
  }
}

function broadcastStore(data) {
  var msg = 'data: ' + JSON.stringify(data) + '\n\n';
  _storeClients.forEach(function(res) {
    if (!_sseWrite(res, msg)) _storeClients.delete(res);
  });
}

function broadcastFeed(event) {
  var msg = 'data: ' + JSON.stringify(formatFeedEvent(event)) + '\n\n';
  _feedClients.forEach(function(res) {
    if (!_sseWrite(res, msg)) _feedClients.delete(res);
  });
}

function feedPush(event) {
  _feedBuf.unshift(event);
  if (_feedBuf.length > FEED_MAX) _feedBuf.length = FEED_MAX;
  broadcastFeed(event);
  fireWebhook(event).catch(function(){});
  persistFeedEvent(event).catch(function(){});
}

function broadcastAdmin(data) {
  var msg = 'data: ' + JSON.stringify(data) + '\n\n';
  _adminClients.forEach(function(res) {
    if (!_sseWrite(res, msg)) _adminClients.delete(res);
  });
  var feedEvent = toBroadcastFeedEvent(data);
  if (feedEvent) feedPush(feedEvent);
}

module.exports = {
  _storeClients, _adminClients, _feedClients,
  _feedBuf, _webhookCache,
  getWebhookConfig, fireWebhook, formatFeedEvent, formatWebhookPayload,
  broadcastStore, broadcastFeed, broadcastAdmin, feedPush,
};
