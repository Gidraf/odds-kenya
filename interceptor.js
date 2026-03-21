/**
 * REQUEST INTERCEPTOR SNIPPET — pure JS, paste into DevTools console
 *
 * Intercepts fetch() and XMLHttpRequest.
 * Sends method, url, status, headers, req/resp body, timing to your webhook.
 *
 * Usage:
 *   1. Open DevTools (F12) → Console tab on any site
 *   2. Paste this entire block and press Enter
 *   3. Navigate / click — requests stream to your dashboard
 */

(function () {
  'use strict';

  // ── CONFIG — edit these ──────────────────────────────────────
  var WEBHOOK_URL  = 'https://uat-api.ajiriwa.gidraf.dev/api/interceptor/collect';
  var MAX_BODY     = 512 * 1024;
  var SKIP_PATTERN = /\.(png|jpe?g|gif|svg|ico|webp|woff2?|ttf|eot|otf|css)(\?|$)|google-analytics|googletagmanager|doubleclick|hotjar|sentry\.io|facebook\.net|adnxs|adservice\.google/i;
  // ────────────────────────────────────────────────────────────

  var SESSION_ID = 'session_' + Math.random().toString(36).slice(2, 10);

  if (window.__icActive) {
    console.warn('[IC] Already active. Session:', window.__icSID);
    return;
  }
  window.__icActive = true;
  window.__icSID    = SESSION_ID;
  window.__icCount  = 0;

  console.log(
    '%c[IC] Active  Session: ' + SESSION_ID,
    'background:#c6f135;color:#080808;padding:4px 10px;font-weight:bold;border-radius:3px'
  );
  console.log('%cWebhook: ' + WEBHOOK_URL, 'color:#38bdf8');

  // Hold real fetch before we override it
  var _realFetch = window.fetch.bind(window);

  // ── Helpers ──────────────────────────────────────────────────

  function shouldSkip(url) {
    if (!url || typeof url !== 'string') return true;
    if (url.indexOf(WEBHOOK_URL) === 0)  return true;
    if (url.indexOf('data:') === 0)      return true;
    if (url.indexOf('blob:') === 0)      return true;
    return SKIP_PATTERN.test(url);
  }

  function truncate(str) {
    if (!str) return '';
    if (typeof str !== 'string') {
      try { str = JSON.stringify(str); } catch (e) { str = String(str); }
    }
    if (str.length <= MAX_BODY) return str;
    return str.slice(0, MAX_BODY) + '\n...[truncated ' + (str.length - MAX_BODY) + ' bytes]';
  }

  function parseHeaders(headers) {
    var out = {};
    if (!headers) return out;
    if (typeof headers.forEach === 'function') {
      headers.forEach(function (v, k) { out[k] = v; });
    } else if (typeof headers === 'object') {
      var keys = Object.keys(headers);
      for (var i = 0; i < keys.length; i++) {
        out[keys[i]] = headers[keys[i]];
      }
    }
    return out;
  }

  function send(payload) {
    payload.session_id = SESSION_ID;
    payload.tab_url    = window.location.href;
    payload.tab_title  = document.title;

    _realFetch(WEBHOOK_URL, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-Session-ID': SESSION_ID },
      body:    JSON.stringify(payload),
    }).catch(function () {});

    window.__icCount++;
    var countEl = document.getElementById('__ic_count');
    var lastEl  = document.getElementById('__ic_last');
    if (countEl) countEl.textContent = window.__icCount;
    if (lastEl) {
      try {
        var u = new URL(payload.url);
        lastEl.textContent = u.pathname.slice(0, 40);
      } catch (e) {
        lastEl.textContent = payload.url.slice(0, 40);
      }
    }
  }

  // ── Override fetch ───────────────────────────────────────────

  window.fetch = function (input, init) {
    init = init || {};
    var url    = typeof input === 'string' ? input : (input && input.url ? input.url : String(input));
    var method = (init.method || (input && input.method) || 'GET').toUpperCase();

    if (shouldSkip(url)) return _realFetch(input, init);

    var reqBody    = init.body ? String(init.body).slice(0, 2000) : null;
    var reqHeaders = parseHeaders(init.headers || (input && input.headers));
    var t0         = performance.now();

    return _realFetch(input, init).then(
      function (response) {
        var dur = Math.round(performance.now() - t0);
        var ct  = response.headers.get('content-type') || '';
        var st  = response.status;
        var rh  = parseHeaders(response.headers);
        response.clone().text().then(function (text) {
          send({ url: url, method: method, status: st,
                 req_headers: reqHeaders, resp_headers: rh,
                 req_body: reqBody, resp_body: truncate(text),
                 content_type: ct, duration_ms: dur });
        }).catch(function () {});
        return response;
      },
      function (err) {
        send({ url: url, method: method, status: 0,
               req_headers: reqHeaders, resp_headers: {},
               req_body: reqBody, resp_body: '[fetch error] ' + err.message,
               content_type: '', duration_ms: Math.round(performance.now() - t0) });
        throw err;
      }
    );
  };

  // ── Override XMLHttpRequest ──────────────────────────────────

  var _xOpen = XMLHttpRequest.prototype.open;
  var _xSend = XMLHttpRequest.prototype.send;
  var _xHdr  = XMLHttpRequest.prototype.setRequestHeader;

  XMLHttpRequest.prototype.open = function (method, url) {
    this._icMethod    = (method || 'GET').toUpperCase();
    this._icUrl       = url ? String(url) : '';
    this._icHeaders   = {};
    this._icSkip      = shouldSkip(this._icUrl);
    this._icT0        = performance.now();
    return _xOpen.apply(this, arguments);
  };

  XMLHttpRequest.prototype.setRequestHeader = function (name, value) {
    if (this._icHeaders) this._icHeaders[name] = value;
    return _xHdr.apply(this, arguments);
  };

  XMLHttpRequest.prototype.send = function (body) {
    if (!this._icSkip) {
      var self    = this;
      var reqBody = body ? String(body).slice(0, 2000) : null;
      this.addEventListener('loadend', function () {
        var dur = Math.round(performance.now() - self._icT0);
        var ct  = self.getResponseHeader('content-type') || '';
        var rh  = {};
        (self.getAllResponseHeaders() || '').split('\r\n').forEach(function (line) {
          var idx = line.indexOf(': ');
          if (idx > 0) rh[line.slice(0, idx).toLowerCase()] = line.slice(idx + 2);
        });
        var respBody = '';
        try { respBody = typeof self.responseText === 'string' ? self.responseText : JSON.stringify(self.response); }
        catch (e) { respBody = ''; }
        send({ url: self._icUrl, method: self._icMethod, status: self.status,
               req_headers: self._icHeaders, resp_headers: rh,
               req_body: reqBody, resp_body: truncate(respBody),
               content_type: ct, duration_ms: dur });
      });
    }
    return _xSend.apply(this, arguments);
  };

  // ── Floating badge ───────────────────────────────────────────

  var badge = document.createElement('div');
  badge.id  = '__ic_badge';
  badge.style.cssText = 'position:fixed;bottom:16px;right:16px;z-index:2147483647;background:rgba(8,8,8,.96);border:1px solid rgba(198,241,53,.3);padding:8px 12px;font-family:IBM Plex Mono,monospace;cursor:pointer;user-select:none;box-shadow:0 4px 24px rgba(0,0,0,.8);border-radius:2px;min-width:160px';
  badge.innerHTML = '<div id="__ic_inner" style="display:flex;flex-direction:column;gap:3px">'
    + '<div style="display:flex;align-items:center;gap:6px">'
    + '<div style="width:7px;height:7px;border-radius:50%;background:#c6f135;box-shadow:0 0 5px #c6f13588;animation:__ic_p 2s infinite"></div>'
    + '<span style="font-size:10px;letter-spacing:1px;color:#c6f135;font-weight:700">INTERCEPTING</span>'
    + '<span id="__ic_count" style="font-size:9px;color:rgba(255,255,255,.4);margin-left:2px">0</span>'
    + '</div>'
    + '<div id="__ic_last" style="font-size:8px;color:rgba(255,255,255,.25);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"></div>'
    + '</div>'
    + '<style>@keyframes __ic_p{0%,100%{opacity:1}50%{opacity:.25}}</style>';

  var _minimised = false;
  badge.addEventListener('click', function () {
    _minimised = !_minimised;
    var inner = document.getElementById('__ic_inner');
    if (inner) inner.style.display = _minimised ? 'none' : 'flex';
  });

  document.body.appendChild(badge);

  console.log('%c[IC] Ready — navigate the page to capture requests.', 'color:#c6f135');
  console.log('%cTo stop: location.reload()', 'color:#555');

})();