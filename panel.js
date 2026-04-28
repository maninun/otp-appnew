'use strict';

const axios  = require('axios');
const crypto = require('crypto');
const C      = require('./config');
const { dbRead, listDirCached } = require('./db');
const { getRsServer }           = require('./models');

const SPEC = {
  '1gb':       { ram:  1024, disk:  2048, cpu:  40 },
  '2gb':       { ram:  2048, disk:  3072, cpu:  60 },
  '3gb':       { ram:  3072, disk:  4096, cpu:  80 },
  '4gb':       { ram:  4096, disk:  5120, cpu: 100 },
  '5gb':       { ram:  5120, disk:  6144, cpu: 120 },
  '6gb':       { ram:  6144, disk:  7168, cpu: 140 },
  '7gb':       { ram:  7168, disk:  8192, cpu: 160 },
  '8gb':       { ram:  8192, disk:  9216, cpu: 180 },
  '9gb':       { ram:  9216, disk: 10240, cpu: 200 },
  '10gb':      { ram: 10240, disk: 11264, cpu: 220 },
  '11gb':      { ram: 11264, disk: 12288, cpu: 240 },
  '12gb':      { ram: 12288, disk: 13312, cpu: 260 },
  '13gb':      { ram: 13312, disk: 14336, cpu: 280 },
  '14gb':      { ram: 14336, disk: 15360, cpu: 300 },
  '15gb':      { ram: 15360, disk: 16384, cpu: 320 },
  '16gb':      { ram: 16384, disk: 17408, cpu: 340 },
  '17gb':      { ram: 17408, disk: 18432, cpu: 360 },
  '18gb':      { ram: 18432, disk: 19456, cpu: 380 },
  '19gb':      { ram: 19456, disk: 20480, cpu: 400 },
  '20gb':      { ram: 20480, disk: 21504, cpu: 420 },
  'unlimited': { ram:     0, disk:     0, cpu:   0 },
};

function sanitizeUsername(u) { return String(u).replace(/[^a-z0-9_]/gi, '').toLowerCase().slice(0, 20); }
function ptH()               { return { Authorization: 'Bearer ' + C.ptero.apikey, Accept: 'application/json', 'Content-Type': 'application/json' }; }
const PT_TIMEOUT = { timeout: 15000 };
function ptCfg(extra) { return Object.assign({ headers: ptH() }, PT_TIMEOUT, extra || {}); }

async function resolveSpec(plan) {
  const templates = await require('./models').getPanelTemplates();
  const t = templates.find(function(x){ return x.id === plan; });
  if (t) return t;
  return SPEC[plan] ? Object.assign({ id: plan, name: plan }, SPEC[plan]) : Object.assign({ id: '1gb', name: '1GB' }, SPEC['1gb']);
}

async function createPanelServer(plan, days, orderId, customUser, customPass, eggOverride) {
  const spec     = await resolveSpec(plan);
  const domain   = C.ptero.domain;
  const username = sanitizeUsername(customUser) || ('prem' + Math.random().toString(36).slice(2, 8));
  const password = customPass || crypto.randomBytes(10).toString('base64').replace(/[^a-zA-Z0-9]/g, '').slice(0, 12);
  const email    = username + '@Dongtube.local';
  const expiresAt = Date.now() + days * 86400000;
  const purchaseDate = new Date().toLocaleDateString('id-ID');
  const expireDate = new Date(expiresAt).toLocaleDateString('id-ID');

  const checkR = await axios.get(domain + '/api/application/users?filter[username]=' + username, ptCfg());
  if (checkR.data && checkR.data.data && checkR.data.data.length > 0) throw new Error('Username "' + username + '" sudah terdaftar.');

  const uRes = await axios.post(domain + '/api/application/users', { email, username, first_name: username, last_name: 'Panel', language: 'en', password }, ptCfg());
  if (uRes.data.errors) throw new Error((uRes.data.errors[0] && uRes.data.errors[0].detail) || 'Gagal buat user panel');
  if (!uRes.data.attributes || !uRes.data.attributes.id) throw new Error('Respons tidak valid dari Pterodactyl saat buat user');
  const userId = uRes.data.attributes.id;

  const ov     = eggOverride || {};
  const tEgg   = ov.egg   || spec.egg   || C.ptero.egg;
  const tNest  = ov.nest  || spec.nest  || C.ptero.nest;
  const tLoc   = spec.location || C.ptero.location;
  const tDock  = ov.docker_image || spec.docker_image || 'ghcr.io/parkervcp/yolks:nodejs_20';
  const tFeat  = spec.feature_limits || { databases: 5, backups: 5, allocations: 5 };
  const tIO    = spec.io !== undefined ? spec.io : 500;
  const tSwap  = spec.swap !== undefined ? spec.swap : 0;

  const eRes    = await axios.get(domain + '/api/application/nests/' + tNest + '/eggs/' + tEgg + '?include=variables', ptCfg());
  const eggAttr = eRes.data.attributes || {};
  // Build environment: start from egg defaults, overlay spec env, then overlay override env
  const eggDefEnv = {};
  const eggVars = (eggAttr.relationships && eggAttr.relationships.variables && eggAttr.relationships.variables.data) || [];
  eggVars.forEach(function(v) { const va = v.attributes; if (va.default_value !== undefined && va.default_value !== null) eggDefEnv[va.env_variable] = va.default_value; });
  const specEnv  = spec.environment || { INST: 'npm', USER_UPLOAD: '0', AUTO_UPDATE: '0', CMD_RUN: 'npm start' };
  const ovEnv    = ov.environment || {};
  const tEnv     = Object.assign({}, eggDefEnv, specEnv, ovEnv);
  const startup  = ov.startup || spec.startup || eggAttr.startup;

  const description = username + ' | Pembelian: ' + purchaseDate + ' | Exp: ' + expireDate;
  let sRes;
  try {
    sRes = await axios.post(domain + '/api/application/servers', {
      name: username,
      description: description,
      user: userId, egg: parseInt(tEgg, 10), docker_image: tDock, startup,
      environment: tEnv,
      limits: { memory: spec.ram, swap: tSwap, disk: spec.disk, io: tIO, cpu: spec.cpu },
      feature_limits: tFeat,
      deploy: { locations: [parseInt(tLoc, 10)], dedicated_ip: false, port_range: [] },
    }, ptCfg());
  } catch(srvErr) {

    try { await axios.delete(domain + '/api/application/users/' + userId, ptCfg()); } catch(e) {}
    const detail = srvErr.response && srvErr.response.data && srvErr.response.data.errors && srvErr.response.data.errors[0];
    throw new Error('Gagal buat server: ' + (detail ? detail.detail : srvErr.message));
  }
  if (sRes.data.errors) {
    try { await axios.delete(domain + '/api/application/users/' + userId, ptCfg()); } catch(e) {}
    throw new Error((sRes.data.errors[0] && sRes.data.errors[0].detail) || 'Gagal buat server');
  }
  if (!sRes.data.attributes) {
    try { await axios.delete(domain + '/api/application/users/' + userId, ptCfg()); } catch(e) {}
    throw new Error('Respons tidak valid dari Pterodactyl saat buat server');
  }
  const server = sRes.data.attributes;
  return {
    serverId: server.id, userId, username, password, email, domain: C.ptero.domain,
    ram:  spec.ram  === 0 ? 'Unlimited' : (spec.ram  / 1024).toFixed(1) + 'GB',
    disk: spec.disk === 0 ? 'Unlimited' : (spec.disk / 1024).toFixed(1) + 'GB',
    cpu:  spec.cpu  === 0 ? 'Unlimited' : spec.cpu + '%',
    days, expiresAt, plan, purchaseDate, expireDate,
  };
}

async function deleteServer(serverId) {
  const domain = C.ptero.domain;
  try {
    const sRes = await axios.get(domain + '/api/application/servers/' + serverId, ptCfg());
    if (!sRes.data.attributes) throw new Error('Server tidak ditemukan');
    const userId = sRes.data.attributes.user;

    await axios.delete(domain + '/api/application/servers/' + serverId, ptCfg());

    try { await axios.delete(domain + '/api/application/users/' + userId, ptCfg()); } catch(e) {}

    return { ok: true };
  } catch (e) {
    throw new Error('Gagal hapus server: ' + (e.message || e));
  }
}

async function suspendServer(serverId) {
  const domain = C.ptero.domain;
  try {
    const res = await axios.post(domain + '/api/application/servers/' + serverId + '/suspend', {}, ptCfg());
    return res.status === 204 || res.status === 200;
  } catch (e) {
    throw new Error('Gagal suspend server: ' + (e.message || e));
  }
}

async function unsuspendServer(serverId) {
  const domain = C.ptero.domain;
  try {
    const res = await axios.post(domain + '/api/application/servers/' + serverId + '/unsuspend', {}, ptCfg());
    return res.status === 204 || res.status === 200;
  } catch (e) {
    throw new Error('Gagal unsuspend server: ' + (e.message || e));
  }
}

async function getServerDetails(serverId) {
  const domain = C.ptero.domain;
  try {
    const res = await axios.get(domain + '/api/application/servers/' + serverId, ptCfg());
    if (!res.data.attributes) throw new Error('Server tidak ditemukan');

    const srv = res.data.attributes;
    const limits = srv.limits || {};

    return {
      serverId: srv.id,
      name: srv.name,
      description: srv.description,
      userId: srv.user,
      suspended: srv.suspended,
      ram: limits.memory === 0 ? 'Unlimited' : (limits.memory / 1024).toFixed(1) + 'GB',
      disk: limits.disk === 0 ? 'Unlimited' : (limits.disk / 1024).toFixed(1) + 'GB',
      cpu: limits.cpu === 0 ? 'Unlimited' : limits.cpu + '%',
      status: srv.status,
      nodeId: srv.node,
      eggId: srv.egg,
      nestId: srv.nest,
      dockerImage: srv.docker_image,
      raw: srv,
    };
  } catch (e) {
    throw new Error('Gagal ambil detail server: ' + (e.message || e));
  }
}

async function updateServerDescription(serverId, description) {
  const domain = C.ptero.domain;
  try {
    const getRes = await axios.get(domain + '/api/application/servers/' + serverId, ptCfg());
    const srv = getRes.data.attributes;

    const res = await axios.patch(domain + '/api/application/servers/' + serverId + '/details', {
      name: srv.name,
      user: srv.user,
      external_id: srv.external_id || null,
      description: description,
    }, ptCfg());

    return res.status === 200;
  } catch (e) {
    throw new Error('Gagal update deskripsi: ' + (e.message || e));
  }
}

async function updateExpiryDescription(serverId, expiresAt) {
  const domain = C.ptero.domain;
  try {
    const getRes = await axios.get(domain + '/api/application/servers/' + serverId, ptCfg());
    const srv = getRes.data.attributes;
    const desc = srv.description || '';

    let username = srv.name || '';
    let purchaseDate = new Date().toLocaleDateString('id-ID');
    let newExpireDate = new Date(expiresAt).toLocaleDateString('id-ID');

    if (desc) {
      const parts = desc.split('|');
      if (parts[0]) username = parts[0].trim();
      if (parts[1]) purchaseDate = parts[1].replace('Pembelian:', '').trim();
    }

    const newDescription = username + ' | Pembelian: ' + purchaseDate + ' | Exp: ' + newExpireDate;

    const res = await axios.patch(domain + '/api/application/servers/' + serverId + '/details', {
      name: srv.name,
      user: srv.user,
      external_id: srv.external_id || null,
      description: newDescription,
    }, ptCfg());

    return res.status === 200;
  } catch (e) {
    throw new Error('Gagal update tanggal exp: ' + (e.message || e));
  }
}

async function extendServerExpiry(serverId, addDays, expiryData) {
  const domain = C.ptero.domain;
  try {
    const getRes = await axios.get(domain + '/api/application/servers/' + serverId, ptCfg());
    const srv = getRes.data.attributes;

    const base = expiryData && expiryData.expiresAt > Date.now() ? expiryData.expiresAt : Date.now();
    const newExpiry = base + addDays * 86400000;
    const newExpireDate = new Date(newExpiry).toLocaleDateString('id-ID');

    const desc = srv.description || '';
    let username = srv.name || '';
    let purchaseDate = new Date().toLocaleDateString('id-ID');

    if (desc) {
      const parts = desc.split('|');
      if (parts[0]) username = parts[0].trim();
      if (parts[1]) purchaseDate = parts[1].replace('Pembelian:', '').trim();
    }

    const newDescription = username + ' | Pembelian: ' + purchaseDate + ' | Exp: ' + newExpireDate;

    await axios.patch(domain + '/api/application/servers/' + serverId + '/details', {
      name: srv.name,
      user: srv.user,
      external_id: srv.external_id || null,
      description: newDescription,
    }, ptCfg());

    return { newExpiry, newExpireDate };
  } catch (e) {
    throw new Error('Gagal extend server: ' + (e.message || e));
  }
}


// ─── Pterodactyl Data Fetchers ────────────────────────────────────────────────

async function getPteroNests() {
  const domain = C.ptero.domain;
  if (!domain || !C.ptero.apikey) throw new Error('Pterodactyl belum dikonfigurasi.');
  const res = await axios.get(domain + '/api/application/nests?per_page=100', ptCfg());
  return (res.data && res.data.data) ? res.data.data.map(function(n) {
    return { id: n.attributes.id, name: n.attributes.name, description: n.attributes.description };
  }) : [];
}

async function getPteroEggsForNest(nestId) {
  const domain = C.ptero.domain;
  if (!domain || !C.ptero.apikey) throw new Error('Pterodactyl belum dikonfigurasi.');
  const res = await axios.get(domain + '/api/application/nests/' + nestId + '/eggs?include=variables&per_page=100', ptCfg());
  return (res.data && res.data.data) ? res.data.data.map(function(e) {
    const a = e.attributes;
    const vars = (a.relationships && a.relationships.variables && a.relationships.variables.data) || [];
    return {
      id: a.id, nestId: a.nest, name: a.name, author: a.author,
      startup: a.startup,
      docker_images: a.docker_images || (a.docker_image ? { default: a.docker_image } : {}),
      variables: vars.map(function(v) {
        const va = v.attributes;
        return { name: va.name, env: va.env_variable, default: va.default_value, description: va.description, rules: va.rules };
      }),
    };
  }) : [];
}

async function getEggDetail(nestId, eggId) {
  const domain = C.ptero.domain;
  if (!domain || !C.ptero.apikey) throw new Error('Pterodactyl belum dikonfigurasi.');
  const res = await axios.get(domain + '/api/application/nests/' + nestId + '/eggs/' + eggId + '?include=variables', ptCfg());
  if (!res.data || !res.data.attributes) throw new Error('Egg tidak ditemukan');
  const a = res.data.attributes;
  const vars = (a.relationships && a.relationships.variables && a.relationships.variables.data) || [];
  return {
    id: a.id, nestId: a.nest, name: a.name, author: a.author,
    startup: a.startup,
    docker_images: a.docker_images || (a.docker_image ? { default: a.docker_image } : {}),
    variables: vars.map(function(v) {
      const va = v.attributes;
      return { name: va.name, env: va.env_variable, default: va.default_value, description: va.description, rules: va.rules };
    }),
  };
}

module.exports = {
  SPEC, sanitizeUsername, ptH, ptCfg,
  resolveSpec, createPanelServer,
  getPteroNests, getPteroEggsForNest, getEggDetail,
  deleteServer, suspendServer, unsuspendServer,
  getServerDetails, updateServerDescription,
  updateExpiryDescription, extendServerExpiry,
};
