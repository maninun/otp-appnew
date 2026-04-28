'use strict';

const axios  = require('axios');
const C      = require('./config');
const { getProducts, getTrx, saveTrx, getSewabotOrder, saveSewabotOrder, popAccount, _sleep, getRsServer, saveRsServer, listDirCached } = require('./models');
const { broadcastAdmin } = require('./broadcast');
const { _gitTreeCache } = require('./db');

async function processRenewal(trx, orderId) {
  const { ptH, ptCfg } = require('./panel');
  const addDays = trx.variantDays || 30;
  const headers = ptH();

  let origTrx = null;
  let lookupId = trx.origTrxId;
  for (var _chain = 0; _chain < 10 && lookupId; _chain++) {
    try {
      const r = await getTrx(lookupId);
      if (!r.data) break;
      origTrx = r.data;
      if (origTrx.result && origTrx.result.serverId) break;
      lookupId = origTrx.origTrxId || null;
    } catch(e) { break; }
  }

  if ((!origTrx || !origTrx.result) && trx.panelUsername) {
    try {
      const rsFiles = (await listDirCached('reseller-servers')).filter(function(f){ return f.name.endsWith('.json'); });
      for (const f of rsFiles) {
        try {
          const r = await getRsServer(f.name.replace('.json',''));
          if (r.data && r.data.panelUsername === trx.panelUsername && r.data.status !== 'deleted') {
            origTrx = { id: r.data.id, _rsvId: r.data.id, _isReseller: true, result: { serverId: r.data.serverId, username: r.data.panelUsername, userId: r.data.userId, domain: r.data.domain, ram: r.data.ram, disk: r.data.disk, cpu: r.data.cpu, expiresAt: r.data.expiresAt } };
            break;
          }
        } catch(e) {}
      }
    } catch(e) {}
  }

  if (!origTrx || !origTrx.result) throw new Error('Transaksi asli tidak ditemukan.');

  const orig      = origTrx.result;
  const baseTime  = (orig.expiresAt && orig.expiresAt > Date.now()) ? orig.expiresAt : Date.now();
  const newExpiry = baseTime + addDays * 86400000;

  if (C.ptero.domain && C.ptero.apikey && orig.serverId) {
    try {
      const sRes = await axios.get(C.ptero.domain + '/api/application/servers/' + orig.serverId, ptCfg());
      const srv  = sRes.data.attributes;
      await axios.patch(C.ptero.domain + '/api/application/servers/' + orig.serverId + '/details', { name: srv.name, user: srv.user, email: srv.user, external_id: srv.external_id || null, description: 'Dongtube ' + (trx.origTrxId || orderId) + ' | exp: ' + new Date(newExpiry).toLocaleDateString('id-ID') + ' [renewed:' + orderId + ']' }, ptCfg());
    } catch(e) { console.warn('[renewal] ptero update:', e.message); }
  }

  if (origTrx._isReseller && origTrx._rsvId) {
    const rsvFresh = await getRsServer(origTrx._rsvId);
    await saveRsServer(origTrx._rsvId, Object.assign({}, rsvFresh.data || {}, { expiresAt: newExpiry, status: 'active', renewedAt: Date.now(), renewedBy: orderId }), rsvFresh.sha || null);
    _gitTreeCache.delete('reseller-servers');
  } else {
    const orig_r = await getTrx(origTrx.id);
    if (!orig_r || !orig_r.data) throw new Error('Data transaksi asli tidak dapat dibaca saat renewal: ' + origTrx.id);
    await saveTrx(origTrx.id, Object.assign({}, orig_r.data, { result: Object.assign({}, orig, { expiresAt: newExpiry }), renewedAt: Date.now(), renewedBy: orderId }), orig_r.sha);
  }
  return { type: 'renewal', username: trx.panelUsername, domain: orig.domain, ram: orig.ram, disk: orig.disk, cpu: orig.cpu, addedDays: addDays, expiresAt: newExpiry, serverId: orig.serverId };
}

async function processProductDelivery(trx, id) {
  if (trx.productType === 'panel' && trx.type === 'renewal') return await processRenewal(trx, id);

  if (trx.productType === 'panel') {
    const { createPanelServer } = require('./panel');
    const _eggOv = trx.panelEgg && typeof trx.panelEgg === 'object' ? trx.panelEgg : null;
    const pd = await createPanelServer(trx.variantPlan, trx.variantDays, id, trx.panelUsername, trx.panelPassword, _eggOv);
    try { const pTrxR = await getTrx(id); if (pTrxR.data) { await saveTrx(id, Object.assign({}, pTrxR.data, { panelPassword: null, _pwClearedAt: Date.now() }), pTrxR.sha); } } catch(e) {}


    return { type: 'panel', username: pd.username, password: pd.password, domain: pd.domain, ram: pd.ram, disk: pd.disk, cpu: pd.cpu, days: pd.days, expiresAt: pd.expiresAt, serverId: pd.serverId, userId: pd.userId };
  }

  if (trx.productType === 'sewabot') {
    const sbId = 'BOT-' + id;
    var sbSaved = false;
    for (var _sbTry = 0; _sbTry < 3 && !sbSaved; _sbTry++) {
      try {
        if (_sbTry > 0) await _sleep(400 * _sbTry);
        await saveSewabotOrder(sbId, { id: sbId, trxId: id, type: 'sewabot', groupUrl: trx.groupUrl || '', days: trx.variantDays || 30, buyerName: trx.buyerName || null, phone: trx.phone || null, price: trx.unitPrice, adminFee: trx.adminFee || 0, totalBayar: trx.totalBayar, status: 'PROCESSING', createdAt: trx.createdAt, completedAt: null }, null);
        sbSaved = true;
      } catch(sbErr) { console.warn('[sewabot-mirror] attempt ' + (_sbTry+1) + ' gagal:', sbErr.message); }
    }
    if (!sbSaved) console.error('[sewabot-mirror] GAGAL simpan setelah 3 percobaan — trxId:', id);
    broadcastAdmin({ type: 'new_sewabot', id: sbId, groupUrl: trx.groupUrl, days: trx.variantDays, buyerName: trx.buyerName, totalBayar: trx.totalBayar, ts: Date.now() });
    return { type: 'sewabot', groupUrl: trx.groupUrl || '', days: trx.variantDays || 30 };
  }

  const products = await getProducts();
  const prod     = products.find(function(p) { return p.id === trx.productId; });
  const vari     = prod && prod.variants.find(function(v) { return v.id === trx.variantId; });
  const ct       = (vari && vari.contentType) || (trx.productType === 'download' ? 'file' : 'text');
  const contentUrl  = (vari && (vari.contentUrl || vari.fileUrl)) || trx.variantFile || '';
  const contentText = (vari && (vari.contentText || vari.content)) || '';

  if (ct === 'account') {
    if (trx.deliveredAccount) {
      return { type: 'content', contentType: 'text', contentText: trx.deliveredAccount, title: trx.variantName || trx.productName, description: (vari && vari.description) || '', filename: '', productName: trx.productName };
    }
    const account = await popAccount(trx.productId, trx.variantId);
    if (!account) return { type: 'content', contentType: 'text', contentText: 'Stok akun habis. Hubungi admin.\nID Order: ' + trx.id, title: trx.variantName || trx.productName, description: '', filename: '', productName: trx.productName };
    try {
      const freshTrxAcc = await getTrx(trx.id);
      await saveTrx(trx.id, Object.assign({}, freshTrxAcc.data || trx, { deliveredAccount: account, _accountSavedAt: Date.now() }), freshTrxAcc.sha || null);
      trx.deliveredAccount = account;
    } catch(saveAccErr) { trx.deliveredAccount = account; }
    return { type: 'content', contentType: 'text', contentText: account, title: trx.variantName || trx.productName, description: (vari && vari.description) || '', filename: '', productName: trx.productName };
  }

  if (!contentUrl && !contentText && ct !== 'text') {
    return { type: 'content', contentType: 'text', contentText: trx.productName + '\nID: ' + trx.id + '\nHubungi admin untuk detail produk.', title: trx.variantName || trx.productName, description: (vari && vari.description) || '', filename: '', productName: trx.productName };
  }
  return {
    type: 'content', contentType: ct, contentUrl,
    contentText: contentText || (ct === 'text' && !contentUrl ? trx.productName + ' berhasil!\nID: ' + trx.id : ''),
    title: trx.variantName || trx.productName,
    description: (vari && vari.description) || '',
    instructions: (vari && vari.instructions) || '',
    contentLinks: (vari && Array.isArray(vari.links) ? vari.links : []),
    filename: (vari && vari.filename) || (trx.variantName || 'produk').replace(/[^a-zA-Z0-9]/g, '_'),
    productName: trx.productName,
  };
}

module.exports = { processProductDelivery };
