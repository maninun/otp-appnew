'use strict';

const { verifyToken, makeUserToken } = require('./auth');
const { getUser } = require('./models');

async function userAuth(req, res, next) {
  const token   = req.headers['x-user-token'];
  const payload = verifyToken(token);
  if (!payload || payload.role !== 'user' || !payload.sub) {
    return res.status(401).json({ ok: false, message: 'Login diperlukan.' });
  }
  req.user = payload.sub;
  try {
    const ur = await getUser(payload.sub);
    if (ur.data && ur.data.banned) {
      return res.status(403).json({ ok: false, message: 'Akun Anda telah diblokir. Hubungi admin.' });
    }
    if (ur.data && ur.data.lastTokenReset && payload.iat && payload.iat < ur.data.lastTokenReset) {
      return res.status(401).json({ ok: false, message: 'Sesi tidak valid. Login ulang.' });
    }
  } catch(e) {

    console.error('[userAuth] DB error saat verifikasi user:', e.message);
    return res.status(503).json({ ok: false, message: 'Layanan sementara tidak tersedia. Coba lagi.' });
  }
  next();
}

module.exports = { userAuth, makeUserToken };
