'use strict';

const axios = require('axios');
const C     = require('./config');

const pak = {
  async create(orderId, amount) {
    const r = await axios.post('https://app.pakasir.com/api/transactioncreate/qris', {
      project: C.pak.slug, order_id: orderId, amount, api_key: C.pak.apikey,
    }, { timeout: 15000 });
    const _safeLog = JSON.stringify(r.data || {}).replace(/"api_key":"[^"]*"/g, '"api_key":"***"');
    console.log('[Pakasir/create] raw:', _safeLog.slice(0, 500));
    if (!r.data) throw new Error('Pakasir: empty response');
    if (!r.data.payment && !r.data.data) throw new Error((r.data && r.data.message) || 'Pakasir error: ' + _safeLog.slice(0, 100));
    const pay = r.data.payment || r.data.data || r.data;
    const _qrisString = pay.payment_number || pay.qr_string || pay.qris_string || pay.qr
      || pay.emv || pay.qr_code || pay.qrcode || pay.qris || pay.emv_qr || pay.emv_code
      || pay.nmid_qr || pay.acquirer_data || '';
    if (!_qrisString) {
      console.warn('[Pakasir/create] QRIS string kosong! Semua field:', Object.keys(pay).join(', '));
    }
    pay._qrisString = _qrisString;
    pay._totalPayment = pay.total_payment || pay.total || pay.amount_total || amount;
    pay._fee = pay.fee || pay.admin_fee || pay.biaya_admin || pay.service_fee || 0;
    return pay;
  },
  async check(orderId, amount) {
    const r = await axios.get('https://app.pakasir.com/api/transactiondetail', {
      params: { project: C.pak.slug, order_id: orderId, amount, api_key: C.pak.apikey }, timeout: 12000,
    });
    const _safeChk = JSON.stringify(r.data || {}).replace(/"api_key":"[^"]*"/g, '"api_key":"***"');
    console.log('[Pakasir/check]', orderId, '| response:', _safeChk.slice(0, 400));
    return r.data;
  },
  async cancel(orderId, amount) {
    await axios.post('https://app.pakasir.com/api/transactioncancel', {
      project: C.pak.slug, order_id: orderId, amount, api_key: C.pak.apikey,
    }, { timeout: 10000 }).catch(function(e) {
      console.error('[pakasir/cancel]', e.message);
    });
  },
};

var _OK_CONSTANTS = {

  app_reg_id            : process.env.OK_APP_REG_ID  || '',
  phone_uuid            : process.env.OK_PHONE_UUID  || '',
  phone_model           : process.env.OK_PHONE_MODEL || 'sdk_gphone64_x86_64',
  phone_android_version : process.env.OK_ANDROID_VER || '16',
  app_version_code      : process.env.OK_APP_VER_CODE || '250811',
  app_version_name      : process.env.OK_APP_VER_NAME || '25.08.11',
  ui_mode               : 'light',
};
var _OK_HEADERS = {
  'User-Agent'   : 'okhttp/4.12.0',
  'Host'         : 'app.orderkuota.com',
  'Content-Type' : 'application/x-www-form-urlencoded',
};
function _okEncode(obj) {
  return Object.keys(obj).map(function(k) {
    return encodeURIComponent(k) + '=' + encodeURIComponent(obj[k] == null ? '' : obj[k]);
  }).join('&');
}
function _crc16(str) {
  var crc = 0xFFFF;
  for (var i = 0; i < str.length; i++) {
    crc ^= str.charCodeAt(i) << 8;
    for (var j = 0; j < 8; j++) crc = (crc & 0x8000) ? ((crc << 1) ^ 0x1021) : (crc << 1);
    crc &= 0xFFFF;
  }
  return crc.toString(16).toUpperCase().padStart(4, '0');
}
function _qrisEmbed(baseQris, amount) {
  if (!baseQris || typeof baseQris !== 'string' || baseQris.length < 20) throw new Error('OK_BASE_QRIS tidak valid.');
  var q = baseQris.trim().toUpperCase();
  if (q.slice(-8, -4) === '6304') q = q.slice(0, -8);
  q = q.replace('010212', '010211');
  var a = String(Math.round(amount)), t = '54' + String(a.length).padStart(2,'0') + a;
  var i53 = q.indexOf('5303'), e53 = i53 >= 0 ? i53+4+(parseInt(q.slice(i53+2,i53+4), 10)||3) : 0;
  var p58 = q.indexOf('5802'); if (p58 < 0) { var _m = q.search(/59\d{2}/); p58 = _m>=0?_m:q.length; }
  var sf  = (e53 > 0 && e53 < p58) ? e53 : 0;
  if (sf < p58) { var bt=q.slice(sf,p58); if (bt.startsWith('54')) { var ol=parseInt(bt.slice(2,4)); if(!isNaN(ol)){bt=bt.slice(4+ol);q=q.slice(0,sf)+bt+q.slice(p58);p58=q.indexOf('5802');if(p58<0)p58=q.length;} } }
  q = q.slice(0,p58) + t + q.slice(p58);
  var s = q + '6304'; return s + _crc16(s);
}
async function _okGenerateQrisAjaib(authUsername, authToken, amount) {
  var payload = Object.assign({}, _OK_CONSTANTS, {
    auth_username                 : authUsername,
    auth_token                    : authToken,
    request_time                  : Date.now().toString(),
    'requests[qris_ajaib][amount]': String(amount),
  });
  var resp = await axios.post('https://app.orderkuota.com/api/v2/get', _okEncode(payload), { headers: _OK_HEADERS, timeout: 15000 });
  var d = resp && resp.data;
  var qr = (d && d.qris_ajaib && (d.qris_ajaib.qr_string || d.qris_ajaib.qrstring || d.qris_ajaib.content)) || null;
  if (!qr) {
    console.warn('[ok/ajaib] raw:', JSON.stringify(d).slice(0, 300));
    throw new Error('QRIS Ajaib: response tidak berisi QR string. ' + JSON.stringify(d).slice(0,150));
  }
  return qr;
}
async function _okCheckPayment(authUsername, authToken, amount, createdAt) {
  var target   = Math.round(amount);
  var anchor   = createdAt || Date.now();
  var tokenId  = String(authToken).split(':')[0];
  var timestamp = Date.now().toString();
  var payload = {
    app_reg_id                          : _OK_CONSTANTS.app_reg_id,
    phone_uuid                          : _OK_CONSTANTS.phone_uuid,
    phone_model                         : _OK_CONSTANTS.phone_model,
    'requests[qris_history][keterangan]': '',
    'requests[qris_history][jumlah]'    : '',
    request_time                        : timestamp,
    phone_android_version               : _OK_CONSTANTS.phone_android_version,
    app_version_code                    : _OK_CONSTANTS.app_version_code,
    auth_username                       : authUsername,
    'requests[qris_history][page]'      : '1',
    auth_token                          : authToken,
    app_version_name                    : _OK_CONSTANTS.app_version_name,
    ui_mode                             : _OK_CONSTANTS.ui_mode,
    'requests[qris_history][dari_tanggal]': '',
    'requests[0]'                       : 'account',
    'requests[qris_history][ke_tanggal]': '',
  };
  var resp = await axios.post('https://app.orderkuota.com/api/v2/qris/mutasi/' + tokenId, _okEncode(payload), { headers: _OK_HEADERS, timeout: 12000 });
  var data = resp && resp.data;
  console.log('[ok/mutasi] raw:', JSON.stringify(data).slice(0, 600));
  var results = (data && data.qris_history && data.qris_history.results) || [];
  if (!Array.isArray(results)) results = [];
  for (var i = 0; i < results.length; i++) {
    var tx = results[i];
    var txAmt = parseInt((tx.jumlah || '').replace(/\D/g, ''), 10) || 0;
    var txTs  = tx.waktu ? new Date(tx.waktu).getTime() : 0;
    if (txAmt === target && txTs >= anchor - 5 * 60 * 1000) return true;
  }
  return false;
}

const ok_gw = {
  async create(orderId, amount) {
    if (!(C.ok.authUsername && C.ok.authToken)) {
      throw new Error('Order Kuota belum dikonfigurasi. Set OK_AUTH_USERNAME dan OK_AUTH_TOKEN.');
    }
    var fee   = Math.floor(Math.random() * (C.ok.randomMax - C.ok.randomMin + 1)) + C.ok.randomMin;
    var total = amount + fee;
    var qr;
    if (C.ok.baseQris) {
      qr = _qrisEmbed(C.ok.baseQris, total);
    } else {
      qr = await _okGenerateQrisAjaib(C.ok.authUsername, C.ok.authToken, total);
    }
    if (!qr || qr.length < 50) throw new Error('Order Kuota: gagal generate QRIS. Cek OK_BASE_QRIS atau OK_AUTH_TOKEN.');
    console.log('[ok/create]', orderId, '| base:', amount, '| fee:', fee, '| total:', total, '| qr_len:', qr.length);
    return { _qrisString: qr, _totalPayment: total, _fee: fee, _createdAt: Date.now() };
  },
  async check(orderId, unitPrice, totalBayar, createdAt) {
    var checkAmount = totalBayar || unitPrice;
    var _createdAt  = createdAt || (Date.now() - 10 * 60 * 1000);
    try {
      var paid = await _okCheckPayment(C.ok.authUsername, C.ok.authToken, checkAmount, _createdAt);
      console.log('[ok/check]', orderId, '| amount:', checkAmount, '| paid:', paid);
      return paid ? { status: 'completed' } : { status: 'pending' };
    } catch(e) {
      console.warn('[ok/check] error:', orderId, e.message);
      return { status: 'pending' };
    }
  },
  async cancel(orderId) {
    console.log('[ok/cancel] skip — OrderKuota tidak punya cancel API:', orderId);
  },
};

const pak_wrapped = {
  create : pak.create.bind(pak),
  // Bug fix: harus pakai effectivePrice (= harga setelah voucher), bukan unitPrice.
  // effectivePrice = totalBayar - pakFee, karena:
  //   pak.create dipanggil dengan effectivePrice
  //   totalBayar  = pakData._totalPayment = effectivePrice + pakFee
  //   pakFee      = pakData._fee (tersimpan di trx.pakData._fee)
  // Jika totalBayar tidak ada (order lama / free), fallback ke unitPrice.
  async check(orderId, unitPrice, totalBayar, createdAt, pakData) {
    var pakFee = (pakData && (pakData._fee || pakData.fee || pakData.admin_fee || 0)) || 0;
    var checkAmount = (totalBayar != null && totalBayar > 0) ? (totalBayar - pakFee) : unitPrice;
    return pak.check(orderId, checkAmount);
  },
  async cancel(orderId, unitPrice, totalBayar) { return pak.cancel(orderId, unitPrice); },
};

var _atlMap = new Map();

function _atlEncode(obj) {
  return Object.keys(obj).map(function(k) {
    return encodeURIComponent(k) + '=' + encodeURIComponent(obj[k] == null ? '' : obj[k]);
  }).join('&');
}

var _ATL_HEADERS = { 'Content-Type': 'application/x-www-form-urlencoded' };

const atl = {
  async create(orderId, amount) {
    if (!C.atl || !C.atl.apikey) throw new Error('Atlantyc belum dikonfigurasi. Set ATLANTYC_API_KEY di .env');
    const payload = _atlEncode({
      api_key: C.atl.apikey,
      reff_id: orderId,
      nominal: String(Math.round(amount)),
      type   : 'ewallet',
      metode : 'qris',
    });
    const r = await axios.post(C.atl.baseUrl + '/deposit/create', payload, {
      headers: _ATL_HEADERS, timeout: 15000,
    });
    const _safeLog = JSON.stringify(r.data || {}).replace(/api_key=[^&]*/g, 'api_key=***');
    console.log('[Atlantyc/create] raw:', _safeLog.slice(0, 500));
    if (!r.data || r.data.status === false) {
      throw new Error('Atlantyc create gagal: ' + ((r.data && r.data.message) || _safeLog.slice(0, 120)));
    }
    const d = r.data.data;
    if (!d || !d.id) throw new Error('Atlantyc: deposit ID tidak ada di response. Raw: ' + _safeLog.slice(0, 200));
    _atlMap.set(orderId, d.id);
    const qr = d.qr_string || d.qrstring || d.qr || '';
    if (!qr) console.warn('[Atlantyc/create] QR string kosong. Fields:', Object.keys(d).join(', '));
    return {
      _qrisString   : qr,
      _totalPayment : d.nominal || amount,
      _fee          : 0,
      _atlantycId   : d.id,
      _qrImage      : d.qr_image || '',
    };
  },

  async check(orderId, unitPrice, totalBayar, createdAt, pakData) {
    if (!C.atl || !C.atl.apikey) return { status: 'pending' };
    var atlId = (pakData && pakData._atlantycId) || _atlMap.get(orderId);
    if (!atlId) {
      console.warn('[Atlantyc/check] deposit ID tidak ditemukan untuk order:', orderId, '— anggap pending');
      return { status: 'pending' };
    }
    const payload = _atlEncode({ api_key: C.atl.apikey, id: atlId });
    const r = await axios.post(C.atl.baseUrl + '/deposit/status', payload, {
      headers: _ATL_HEADERS, timeout: 12000,
    });
    const status = ((r.data && r.data.data && r.data.data.status) || 'pending').toLowerCase();
    console.log('[Atlantyc/check]', orderId, '| atl_id:', atlId, '| status:', status);
    if (status === 'success')                        return { status: 'completed' };
    if (status === 'cancel' || status === 'expired') return { status: 'canceled' };
    return { status: 'pending' };
  },

  async cancel(orderId, unitPrice, totalBayar, pakData) {
    if (!C.atl || !C.atl.apikey) return;
    var atlId = (pakData && pakData._atlantycId) || _atlMap.get(orderId);
    if (!atlId) { console.log('[Atlantyc/cancel] skip — deposit ID tidak diketahui:', orderId); return; }
    const payload = _atlEncode({ api_key: C.atl.apikey, id: atlId });
    await axios.post(C.atl.baseUrl + '/deposit/cancel', payload, {
      headers: _ATL_HEADERS, timeout: 10000,
    }).catch(function(e) { console.error('[Atlantyc/cancel]', e.message); });
    _atlMap.delete(orderId);
  },
};

const atl_wrapped = {
  create: atl.create.bind(atl),
  async check(orderId, unitPrice, totalBayar, createdAt, pakData)  { return atl.check(orderId, unitPrice, totalBayar, createdAt, pakData); },
  async cancel(orderId, unitPrice, totalBayar, pakData)            { return atl.cancel(orderId, unitPrice, totalBayar, pakData); },
};

var _zakkiMap = new Map();

const zakki = {
  async create(orderId, amount) {
    if (!C.zakki || !C.zakki.token) throw new Error('Zakki belum dikonfigurasi. Set ZAKKI_TOKEN di .env');
    const payload = { token: C.zakki.token, nominal: Math.round(amount) };
    const r = await axios.post(C.zakki.baseUrl + '/topup', payload, {
      headers: { 'Content-Type': 'application/json' }, timeout: 15000,
      validateStatus: function(s) { return s < 500; },
    });
    if (r.status === 401) throw new Error('Zakki: token tidak valid atau tidak diizinkan (HTTP 401). Pastikan ZAKKI_TOKEN di .env sudah benar.');
    if (r.status === 403) throw new Error('Zakki: akses ditolak (HTTP 403). Periksa kembali token dan izin akun zakki kamu.');
    const _safeLog = JSON.stringify(r.data || {}).replace(/"token":"[^"]*"/g, '"token":"***"');
    console.log('[Zakki/create] raw:', _safeLog.slice(0, 800));
    const _code  = r.data && (r.data.code || r.data.status);
    const _codeOk = (_code === 200 || _code === 201 || _code === '200' || _code === '201' || _code === true || _code === 'success');
    if (!r.data || !_codeOk) {
      if (r.data && r.data.pending_transaksi) {
        throw new Error('Zakki: ada transaksi pending! Batalkan dulu transaksi lama. Total: Rp' + r.data.pending_transaksi.total_bayar);
      }
      throw new Error('Zakki create gagal (code=' + _code + '): ' + ((r.data && (r.data.message || r.data.msg || r.data.error)) || '') + ' | raw: ' + _safeLog.slice(0, 200));
    }
    const d = r.data.data || r.data;
    const _txId = d.id_transaksi || d.id || d.trx_id || d.transaction_id;
    if (!_txId) throw new Error('Zakki: id_transaksi tidak ada di response. Raw: ' + _safeLog.slice(0, 300));
    _zakkiMap.set(orderId, _txId);
    const qrImage  = d.qris_image || d.qr_image || d.image || d.qrcode || d.qr || '';
    const qrString = d.qr_string  || d.qrstring || d.emv   || '';
    const rincian  = d.rincian || {};
    const _total   = rincian.total_bayar || d.total_bayar || d.total || d.amount || d.nominal || amount;
    const _fee     = rincian.kode_unik   || d.kode_unik   || d.fee   || 0;
    if (!qrImage && !qrString) {
      console.warn('[Zakki/create] QR kosong! Field tersedia:', Object.keys(d).join(', '));
    }
    return {
      _qrisString   : qrString,
      _qrImage      : qrImage,
      _totalPayment : _total,
      _fee          : _fee,
      _zakkiId      : _txId,
      _rincian      : rincian,
    };
  },

  async check(orderId, unitPrice, totalBayar, createdAt, pakData) {
    if (!C.zakki || !C.zakki.token) return { status: 'pending' };
    var zakkiId = (pakData && pakData._zakkiId) || _zakkiMap.get(orderId);
    if (!zakkiId) {
      console.warn('[Zakki/check] id_transaksi tidak ditemukan untuk order:', orderId, '— anggap pending');
      return { status: 'pending' };
    }
    try {
      const r = await axios.get(C.zakki.baseUrl + '/cektopup', {
        params: { idtopup: zakkiId }, timeout: 12000,
      });
      console.log('[Zakki/check]', orderId, '| zakki_id:', zakkiId, '| status:', r.data && r.data.kategori_status);
      if (r.data && r.data.code === 200 && r.data.kategori_status === 'SUCCESS') {
        return { status: 'completed' };
      }
      return { status: 'pending' };
    } catch (e) {
      console.warn('[Zakki/check] error:', orderId, e.message);
      return { status: 'pending' };
    }
  },

  async cancel(orderId, unitPrice, totalBayar, pakData) {
    var zakkiId = (pakData && pakData._zakkiId) || _zakkiMap.get(orderId);
    if (!zakkiId) { console.log('[Zakki/cancel] skip — id_transaksi tidak diketahui:', orderId); return; }
    await axios.get(C.zakki.baseUrl + '/cancel', {
      params: { id_transaksi: zakkiId }, timeout: 10000,
    }).catch(function(e) { console.error('[Zakki/cancel]', e.message); });
    _zakkiMap.delete(orderId);
  },
};

const zakki_wrapped = {
  create: zakki.create.bind(zakki),
  async check(orderId, unitPrice, totalBayar, createdAt, pakData)  { return zakki.check(orderId, unitPrice, totalBayar, createdAt, pakData); },
  async cancel(orderId, unitPrice, totalBayar, pakData)            { return zakki.cancel(orderId, unitPrice, totalBayar, pakData); },
};

const PAYMENT_GW = (process.env.PAYMENT_GATEWAY || 'pakasir').toLowerCase().trim();

const _isOrderKuota = PAYMENT_GW === 'orderkuota' || PAYMENT_GW === 'orderkouta';
const _isAtlantyc   = PAYMENT_GW === 'atlantyc';
const _isZakki      = PAYMENT_GW === 'zakki';

const pgw = _isZakki ? zakki_wrapped : (_isAtlantyc ? atl_wrapped : (_isOrderKuota ? ok_gw : pak_wrapped));

function _pgwConfigured() {
  if (_isZakki)      return !!(C.zakki && C.zakki.token);
  if (_isAtlantyc)   return !!(C.atl && C.atl.apikey);
  if (_isOrderKuota) return !!(C.ok.authUsername && C.ok.authToken && C.ok.baseQris);
  return !!(C.pak.slug && C.pak.apikey);
}

module.exports = { pgw, PAYMENT_GW, _pgwConfigured };
