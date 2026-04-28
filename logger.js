'use strict';

var _logBuf = [];

function _capLog(level, args) {
  var msg = args.map(function(a){ return typeof a === 'object' ? JSON.stringify(a) : String(a); }).join(' ');
  _logBuf.push({ ts: Date.now(), level: level, msg: msg });
  if (_logBuf.length > 500) _logBuf.shift();
}

var _origLog  = console.log;
var _origWarn = console.warn;
var _origErr  = console.error;

console.log   = function() { _capLog('info',  Array.prototype.slice.call(arguments)); _origLog.apply(console, arguments); };
console.warn  = function() { _capLog('warn',  Array.prototype.slice.call(arguments)); _origWarn.apply(console, arguments); };
console.error = function() { _capLog('error', Array.prototype.slice.call(arguments)); _origErr.apply(console, arguments); };

module.exports = {
  getLogs : function() { return _logBuf.slice(); },
  origLog : _origLog,
  origWarn: _origWarn,
  origErr : _origErr,
};
