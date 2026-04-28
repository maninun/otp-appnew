'use strict';

const axios = require('axios');
const C     = require('./config');

const BASE = 'https://www.rumahotp.io';

const rotp = {
  h: function() { return { 'x-apikey': C.otp.apikey, 'Accept': 'application/json' }; },
  async get(url) {
    const r = await axios.get(BASE + url, { headers: rotp.h(), timeout: 15000 });
    return r.data;
  },
  async getPublic(url) {
    const r = await axios.get(BASE + url, { timeout: 15000 });
    return r.data;
  },

  async services()                                  { return rotp.get('/api/v2/services'); },
  async countries(service_id)                       { return rotp.get('/api/v2/countries?service_id=' + service_id); },
  async operators(country, provider_id)             { return rotp.get('/api/v2/operators?country=' + encodeURIComponent(country) + '&provider_id=' + provider_id); },
  async order(number_id, provider_id, operator_id) {
    return rotp.get('/api/v2/orders?number_id=' + number_id + '&provider_id=' + provider_id + '&operator_id=' + (operator_id || 'any'));
  },
  async orderStatus(order_id)                       { return rotp.get('/api/v1/orders/get_status?order_id=' + order_id); },
  async orderDetail(order_id)                       { return rotp.get('/api/v1/orders/get_status?order_id=' + order_id); },
  async cancelOrder(order_id)                       { return rotp.get('/api/v1/orders/set_status?order_id=' + order_id + '&status=cancel'); },
  async balance()                                   { return rotp.get('/api/v1/user/balance'); },

  async depositCreate(amount)                       { return rotp.get('/api/v2/deposit/create?amount=' + amount + '&payment_id=qris'); },
  async depositStatus(deposit_id)                   { return rotp.get('/api/v2/deposit/get_status?deposit_id=' + deposit_id); },
  async depositCancel(deposit_id)                   { return rotp.get('/api/v1/deposit/cancel?deposit_id=' + deposit_id); },

  async h2hProducts()                               { return rotp.getPublic('/api/v1/h2h/product'); },
  async h2hListRekening()                           { return rotp.getPublic('/api/v1/h2h/list/rekening'); },
  async h2hCheckRekening(bank_code, account_number) { return rotp.getPublic('/api/v1/h2h/check/rekening?bank_code=' + encodeURIComponent(bank_code) + '&account_number=' + encodeURIComponent(account_number)); },
  async h2hListUsername()                           { return rotp.getPublic('/api/v1/h2h/list/username'); },
  async h2hCheckUsername(account_code, account_number) { return rotp.getPublic('/api/v1/h2h/check/username?account_code=' + encodeURIComponent(account_code) + '&account_number=' + encodeURIComponent(account_number)); },
  async h2hCreateTransaction(target, id)            { return rotp.get('/api/v1/h2h/transaksi/create?target=' + encodeURIComponent(target) + '&id=' + encodeURIComponent(id)); },
  async h2hTransactionStatus(transaksi_id)          { return rotp.get('/api/v1/h2h/transaksi/status?transaksi_id=' + encodeURIComponent(transaksi_id)); },
};

rotp.baseUrl = BASE; // BUG FIX: ekspor baseUrl agar admin diag menampilkan URL yang benar
module.exports = rotp;
