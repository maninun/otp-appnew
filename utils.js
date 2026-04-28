'use strict';

const crypto = require('crypto');
const axios = require('axios');
const { rateLimit } = require('./auth');

const TRX_RE     = /^(TRX|RNW|BOT)-\d{13}-[a-f0-9]{8}$/;
const DEP_RE     = /^DEP-\d{13}-[a-f0-9]{8}$/;
const OTPORD_RE  = /^OTP-\d{13}-[a-f0-9]{8}$/;

function isValidId(id)     { return typeof id === 'string' && TRX_RE.test(id); }
function isValidDepId(id)  { return typeof id === 'string' && DEP_RE.test(id); }
function isValidOtpId(id)  { return typeof id === 'string' && OTPORD_RE.test(id); }

function newId(prefix) {
  return (prefix || 'TRX') + '-' + Date.now() + '-' + crypto.randomBytes(4).toString('hex');
}

function _sleep(ms) {
  return new Promise(function(resolve){ setTimeout(resolve, ms); });
}

const _otpOrderLocks = new Map();
const _OTP_ORDER_COOLDOWN_MS = 3000;
const _otpLastOrderTime = new Map();

async function _acquireOtpLock(username) {
  while (_otpOrderLocks.has(username)) {
    await _otpOrderLocks.get(username);
  }
  let _resolve;
  const lockPromise = new Promise(function(resolve) { _resolve = resolve; });
  _otpOrderLocks.set(username, lockPromise);
  return _resolve;
}

function _releaseOtpLock(username, resolveFn) {
  _otpOrderLocks.delete(username);
  if (resolveFn) resolveFn();
}

function getClientIp(req) {
  return (req.headers['x-forwarded-for'] || req.socket?.remoteAddress || '').split(',')[0].trim();
}

async function httpRequest(method, url, data, headers = {}) {
  try {
    const response = await axios({
      method,
      url,
      data: method !== 'GET' ? data : undefined,
      headers: {
        'User-Agent': 'Dongtube-Store/1.0',
        ...headers
      },
      timeout: 30000
    });
    return response.data;
  } catch (error) {
    const status  = error.response && error.response.status;
    const body    = error.response && error.response.data;
    const message = (body && (body.message || body.error)) || error.message || 'HTTP request failed';
    const err     = new Error('[' + method + ' ' + url + '] ' + message);
    if (status) err.status = status;
    if (body)   err.responseData = body;
    throw err;
  }
}

function safeStringify(obj, depth = 10) {
  if (depth === 0) return '[Object]';
  if (obj === null) return null;
  if (obj === undefined) return undefined;
  if (typeof obj !== 'object') return obj;
  if (obj instanceof Date) return obj.toISOString();
  if (obj instanceof Error) return { message: obj.message, stack: obj.stack };
  if (Array.isArray(obj)) return obj.map(item => safeStringify(item, depth - 1));
  return Object.keys(obj).reduce((acc, key) => {
    acc[key] = safeStringify(obj[key], depth - 1);
    return acc;
  }, {});
}

module.exports = {

  isValidId,
  isValidDepId,
  isValidOtpId,
  newId,
  TRX_RE,
  DEP_RE,
  OTPORD_RE,

  _sleep,
  rateLimit,
  getClientIp,
  safeStringify,
  httpRequest,

  _acquireOtpLock,
  _releaseOtpLock,
  _otpOrderLocks,
  _otpLastOrderTime,
  _OTP_ORDER_COOLDOWN_MS,
};
