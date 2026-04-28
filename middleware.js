'use strict';

const compression = require('compression');
const cors        = require('cors');
const express     = require('express');
const morgan      = require('morgan');
const { getLogs } = require('./logger');

function getLogBuffer() {
  return getLogs();
}

function setupCompression(app) {
  // BUGFIX: exclude text/event-stream from compression — gzip buffers SSE events
  // sehingga tidak terkirim real-time. Filter ini memastikan SSE lewat tanpa compress.
  app.use(compression({
    level: 6,
    threshold: 1024,
    filter: function(req, res) {
      var ct = res.getHeader('Content-Type') || '';
      if (String(ct).indexOf('text/event-stream') !== -1) return false;
      if (req.path && req.path.indexOf('/stream') !== -1) return false;
      return compression.filter(req, res);
    },
  }));
  app.use(morgan(':method :url :status :res[content-length] - :response-time ms'));
}

function setupCors(app) {
  const corsOrigin = process.env.CORS_ORIGIN || '*';
  if (corsOrigin === '*' && process.env.NODE_ENV === 'production') {
    console.warn('⚠️  [SECURITY] CORS_ORIGIN tidak diset — semua origin diizinkan (*). Sebaiknya set CORS_ORIGIN=https://domain-kamu.com di production.');
  }
  app.use(cors({
    origin: corsOrigin,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-token', 'x-user-token'],
    exposedHeaders: ['x-user-token'],
    optionsSuccessStatus: 204,
  }));

  app.options('/api/*', cors());
}

function setupBodyParsers(app) {
  app.use(express.json({ limit: '256kb' }));
  app.use(express.urlencoded({ extended: true, limit: '64kb' }));
}

function setupSecurityHeaders(app) {
  app.use(function(req, res, next) {
    res.set('X-Content-Type-Options',            'nosniff');
    res.set('X-Frame-Options',                   'DENY');
    res.set('X-XSS-Protection',                  '1; mode=block');
    res.set('X-Permitted-Cross-Domain-Policies', 'none');
    res.set('Referrer-Policy',                   'strict-origin-when-cross-origin');
    res.set('Permissions-Policy',                'geolocation=(), camera=(), microphone=()');
    res.set('Strict-Transport-Security',         'max-age=31536000; includeSubDomains; preload');
    res.set('Content-Security-Policy',
      "default-src 'self'; " +
      "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://accounts.google.com; " +
      "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://fonts.googleapis.com; " +
      "img-src 'self' data: blob: https:; " +
      "font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com; " +
      "connect-src 'self' blob: https:; " +
      "frame-src https://accounts.google.com; " +
      "object-src 'none'; " +
      "base-uri 'self';"
    );
    // ── Cache-Control: real-time untuk semua data, cache hanya untuk aset statis ──
    if (req.path.startsWith('/api/')) {
      // Semua API endpoint: tidak di-cache sama sekali
      res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
      res.set('Pragma',        'no-cache');
      res.set('Expires',       '0');
    } else if (req.path.match(/\.(js|css)$/i)) {
      // JS/CSS: cache 1 jam di browser, tapi validasi ke server (stale-while-revalidate)
      // Ketika file berubah di server, browser langsung dapat versi baru
      res.set('Cache-Control', 'public, max-age=3600, stale-while-revalidate=60');
      res.set('Vary',          'Accept-Encoding');
    } else if (req.path.match(/\.(png|jpg|jpeg|ico|svg|webp|gif)$/i)) {
      // Gambar: cache 1 hari
      res.set('Cache-Control', 'public, max-age=86400');
    } else if (req.path.match(/\.(woff|woff2|ttf|eot)$/i)) {
      // Font: cache 1 minggu (jarang berubah)
      res.set('Cache-Control', 'public, max-age=604800, immutable');
    } else {
      // Semua halaman HTML: tidak di-cache (selalu ambil versi terbaru)
      res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
      res.set('Pragma',        'no-cache');
      res.set('Expires',       '0');
    }
    next();
  });
}

module.exports = { setupCompression, setupCors, setupBodyParsers, setupSecurityHeaders, getLogBuffer };
