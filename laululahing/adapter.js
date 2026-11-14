/* Laululahing väljaspool Claude'i: asendab Claude'i andmebaasi kohaliku salvestuse ja avaliku MQTT-sõnumiserveriga.
   Host (admin.html) hoiab mängu seisu oma brauseris ja saadab selle telefonidele; telefonid saadavad vastused hostile. */
(function () {
  var ROLE = window.LL_ROLE === "host" ? "host" : "phone";
  var ROOM = "karlfest-ll-party-4h8d2k";
  var qs = new URLSearchParams(location.search);
  var BROKERS = qs.get("broker") ? [qs.get("broker")] : [
    "wss://broker.emqx.io:8084/mqtt",
    "wss://broker.hivemq.com:8884/mqtt",
    "wss://test.mosquitto.org:8081/mqtt"
  ];
  var T = "karlfest/" + ROOM + "/";
  var LS = "llparty-" + ROLE + ":";

  function lsGet(k) { try { var v = localStorage.getItem(LS + k); return v == null ? null : JSON.parse(v); } catch (e) { return null; } }
  function lsSet(k, v) { try { if (v == null) localStorage.removeItem(LS + k); else localStorage.setItem(LS + k, JSON.stringify(v)); } catch (e) {} }
  function merge(a, b) {
    var o = Object.assign({}, a || {});
    for (var k in b) {
      var bv = b[k], av = a ? a[k] : undefined;
      o[k] = (bv && typeof bv === "object" && !Array.isArray(bv) && av && typeof av === "object" && !Array.isArray(av)) ? merge(av, bv) : bv;
    }
    return o;
  }
  function snap(d, id) { return { exists: d != null, data: function () { return d; }, id: id }; }

  var uid = lsGet("uid");
  if (!uid) { uid = "p" + Math.random().toString(36).slice(2, 10) + Date.now().toString(36); lsSet("uid", uid); }

  /* ---- ühendus ---- */
  var clients = [];
  var dot = null;
  function status() {
    var up = clients.some(function (c) { return c.__up; });
    if (!dot) {
      dot = document.createElement("div");
      dot.style.cssText = "position:fixed;right:8px;bottom:6px;z-index:99;font:600 11px Arial,sans-serif;padding:3px 8px;border-radius:999px;background:rgba(0,0,0,.45);color:#fff;pointer-events:none";
      (document.body || document.documentElement).appendChild(dot);
    }
    dot.textContent = up ? "● ühendatud" : "○ ühendus puudub";
    dot.style.color = up ? "#7dffa0" : "#ff9a9a";
  }
  function pub(topic, obj, retain) {
    var s = JSON.stringify(obj);
    clients.forEach(function (c) { if (c.__up) { try { c.publish(T + topic, s, { qos: 0, retain: !!retain }); } catch (e) {} } });
  }
  function connect() {
    if (!window.mqtt) { status(); return; }
    BROKERS.forEach(function (url) {
      try {
        var c = mqtt.connect(url, { clientId: "ll_" + ROLE + "_" + Math.random().toString(16).slice(2, 10), keepalive: 30, reconnectPeriod: 2500, connectTimeout: 8000, clean: true });
        c.on("connect", function () { c.__up = true; c.subscribe(ROLE === "host" ? T + "ans/+" : T + "state"); status(); resend(); });
        c.on("close", function () { c.__up = false; status(); });
        c.on("error", function () {});
        c.on("message", function (topic, payload) { var d; try { d = JSON.parse(payload.toString()); } catch (e) { return; } onMsg(topic.slice(T.length), d); });
        clients.push(c);
      } catch (e) {}
    });
    status();
  }

  /* ---- dokumendid ---- */
  var docs = {}, dl = {}, subs = lsGet("subs") || {}, cl = [];
  var stateV = 0, phoneStateJson = "";
  function load(path) { if (!(path in docs)) docs[path] = (ROLE === "phone" && path === "game/state") ? null : lsGet(path); return docs[path]; }
  function fire(path) { (dl[path] || []).forEach(function (n) { try { n(snap(docs[path])); } catch (e) { console.error(e); } }); }
  function put(path, d) {
    docs[path] = d; lsSet(path, d); fire(path);
    if (ROLE === "host" && path === "game/state") { stateV = Date.now(); sendState(); }
    if (ROLE === "phone" && path === "subs/" + uid) sendMine();
  }
  function sendState() {
    var st = load("game/state"); if (!st) return;
    var o = Object.assign({}, st, { _v: stateV || (stateV = Date.now()) }); delete o.manual;
    pub("state", o, true);
  }
  function sendMine() {
    var d = load("subs/" + uid); if (!d) return;
    pub("ans/" + uid, Object.assign({}, d, { _v: d._v || Date.now() }), true);
  }
  function resend() { if (ROLE === "host") sendState(); else sendMine(); }
  function fireSubs() {
    var q = { docs: Object.keys(subs).map(function (id) { return snap(subs[id], id); }) };
    cl.forEach(function (n) { try { n(q); } catch (e) { console.error(e); } });
  }
  function onMsg(topic, d) {
    if (ROLE === "phone" && topic === "state") {
      if (!d || typeof d !== "object") return;
      var cur = docs["game/state"];
      if (cur && (cur._v || 0) > (d._v || 0)) return;
      var j = JSON.stringify(d); if (j === phoneStateJson) return; phoneStateJson = j;
      docs["game/state"] = d; fire("game/state");
    } else if (ROLE === "host" && topic.indexOf("ans/") === 0) {
      var id = topic.slice(4);
      if (d == null) { return; }
      var old = subs[id];
      if (old && (old._v || 0) >= (d._v || 0)) return;
      subs[id] = d; lsSet("subs", subs); fireSubs();
    }
  }

  var db = {
    doc: function (path) {
      return {
        onSnapshot: function (next) {
          (dl[path] = dl[path] || []).push(next);
          var isPhoneState = ROLE === "phone" && path === "game/state";
          if (!isPhoneState) setTimeout(function () { next(snap(load(path))); }, 0);
          else setTimeout(function () { if (docs[path] == null) next(snap(null)); }, 6000);
          return function () {};
        },
        get: function () { return Promise.resolve(snap(load(path))); },
        set: function (d) {
          if (ROLE === "phone" && path === "subs/" + uid) d = Object.assign({}, d, { _v: Date.now() });
          put(path, d); return Promise.resolve();
        },
        update: function (d) {
          var n = merge(load(path) || {}, d);
          if (ROLE === "phone" && path === "subs/" + uid) n._v = Date.now();
          put(path, n); return Promise.resolve();
        },
        delete: function () {
          if (path.indexOf("subs/") === 0 && ROLE === "host") { delete subs[path.slice(5)]; lsSet("subs", subs); fireSubs(); }
          else put(path, null);
          return Promise.resolve();
        }
      };
    },
    collection: function () {
      return {
        onSnapshot: function (next) { cl.push(next); setTimeout(fireSubs, 0); return function () {}; },
        get: function () { return Promise.resolve({ docs: Object.keys(subs).map(function (id) { return snap(subs[id], id); }) }); }
      };
    }
  };
  var user = { isOwner: function () { return Promise.resolve(ROLE === "host"); }, id: function () { return Promise.resolve(uid); } };
  window.claude = { use: function (n) { return Promise.resolve(n === "db" ? db : n === "user" ? user : null); } };

  /* vastuste võti: esimesel avamisel laetakse kaasapandud nimekiri */
  if (ROLE === "host" && !lsGet("key/songs") && window.LL_KEY) {
    try { lsSet("key/songs", JSON.parse(decodeURIComponent(escape(atob(window.LL_KEY))))); } catch (e) {}
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", connect); else connect();
  setInterval(resend, 3500);
  document.addEventListener("visibilitychange", function () { if (!document.hidden) resend(); });
})();
