'use strict';

const CFG = require('../config');

const C = {
  gh: {
    token  : process.env.GH_TOKEN   || '',
    owner  : process.env.GH_OWNER   || '',
    repos  : (process.env.GH_REPO   || '').split(',').map(function(r){ return r.trim(); }).filter(Boolean),
    get repo() { return this.repos[0] || ''; },
    branch : process.env.GH_BRANCH  || 'main',
    private: process.env.GH_PRIVATE === 'true' || process.env.GH_PRIVATE === '1',

    token2 : process.env.GH_TOKEN2  || '',
    owner2 : process.env.GH_OWNER2  || '',
    repos2 : (process.env.GH_REPO2  || '').split(',').map(function(r){ return r.trim(); }).filter(Boolean),
    get repo2() { return this.repos2[0] || ''; },
    branch2: process.env.GH_BRANCH2 || process.env.GH_BRANCH || 'main',
    private2: process.env.GH_PRIVATE2 === 'true' || process.env.GH_PRIVATE2 === '1',
  },
  pak: {
    slug   : process.env.PAKASIR_SLUG   || '',
    apikey : process.env.PAKASIR_APIKEY || '',
  },
  ok: {
    authUsername: process.env.OK_AUTH_USERNAME || '',
    authToken   : process.env.OK_AUTH_TOKEN    || '',
    baseQris    : process.env.OK_BASE_QRIS     || '',
    randomMin   : parseInt(process.env.OK_RANDOM_MIN, 10) || 1,
    randomMax   : parseInt(process.env.OK_RANDOM_MAX, 10) || 97,
  },
  atl: {
    apikey  : process.env.ATLANTYC_API_KEY || '',
    baseUrl : (process.env.ATLANTYC_BASE_URL || 'https://atlantich2h.com').replace(/\/$/, ''),
  },
  zakki: {
    token   : process.env.ZAKKI_TOKEN    || '',
    baseUrl : (process.env.ZAKKI_BASE_URL || 'https://qris.zakki.store').replace(/\/$/, ''),
  },
  ptero: {
    domain   : (process.env.PTERO_DOMAIN || '').replace(/\/$/, ''),
    apikey   : process.env.PTERO_APIKEY  || '',
    capikey  : process.env.PTERO_CAPIKEY || '',
    egg      : parseInt(process.env.PTERO_EGG, 10)      || 15,
    nest     : parseInt(process.env.PTERO_NEST, 10)     || 5,
    location : parseInt(process.env.PTERO_LOCATION, 10) || 1,
  },
  store: {
    name        : process.env.STORE_NAME          || CFG.STORE_NAME          || 'Dongtube',
    description : process.env.STORE_DESCRIPTION   || CFG.STORE_DESCRIPTION   || '',
    wa          : process.env.STORE_WA            || CFG.STORE_WA            || '',
    channel     : process.env.STORE_CHANNEL       || CFG.STORE_CHANNEL       || '',
    instagram   : process.env.STORE_INSTAGRAM     || CFG.STORE_INSTAGRAM     || '',
    tiktok      : process.env.STORE_TIKTOK        || CFG.STORE_TIKTOK        || '',
    logoUrl     : process.env.STORE_LOGO          || CFG.STORE_LOGO          || '',
    appLogoUrl  : process.env.STORE_ICON          || CFG.STORE_ICON          || '',
    primaryColor: process.env.STORE_PRIMARY_COLOR || CFG.STORE_PRIMARY_COLOR || '#34d399',
    expiry      : parseInt(process.env.EXPIRY_MIN || CFG.STORE_EXPIRY_MIN, 10)   || 15,
    adminPass   : process.env.ADMIN_PASS || 'admin123',
    adminPath   : process.env.ADMIN_PATH || 'admin',
  },
  port: parseInt(process.env.PORT, 10) || 3000,
  cdn: {
    maxSize: parseInt(process.env.CDN_MAX_SIZE_MB || '100', 10) * 1024 * 1024,
  },
  otp: {
    apikey : process.env.RUMAHOTP_APIKEY || '',
  },
};

if (!process.env.ADMIN_PASS || process.env.ADMIN_PASS === 'admin123') {
  console.warn('[WARNING] ADMIN_PASS belum diset atau masih default. Segera ubah di .env!');
}
if (!process.env.RUMAHOTP_APIKEY) {
  console.warn('[WARNING] RUMAHOTP_APIKEY belum diset. Fitur OTP tidak akan berfungsi.');
}
if (!process.env.TOKEN_SECRET) {
  console.warn('[WARNING] TOKEN_SECRET belum diset. Session user akan reset setiap restart.');
}

module.exports = C;
