/* Deck engine: navigation, step builds, appendix links, notes, charts and animations. */
(function () {
  const stage = document.getElementById('stage');
  const slides = [...document.querySelectorAll('.slide')];
  const main = slides.filter(s => !s.classList.contains('apx'));
  const byId = Object.fromEntries(slides.map(s => [s.id, s]));
  const notes = document.getElementById('notes');
  const progress = document.getElementById('progress');
  let cur = 0, backStack = [];

  // ---------- scale the 1600x900 stage to the window ----------
  function fit() {
    const k = Math.min(innerWidth / 1600, innerHeight / 900);
    stage.style.transform = `scale(${k})`;
  }
  addEventListener('resize', fit); fit();

  // ---------- footer on every slide ----------
  main.forEach((s, i) => s.dataset.n = i + 1);
  // appendix markers say which slides they back up, from the current order ("for slide 12", "for slides 12-14")
  slides.filter(s => s.classList.contains('apx') && s.id !== 'a0').forEach(s => {
    const ns = main.filter(m => m.dataset.apx === s.id).map(m => +m.dataset.n);
    const el = s.querySelector('.eyebrow .sec-name'); if (!el || !ns.length) return;
    el.textContent = ns.length === 1 ? `for slide ${ns[0]}` : `for slides ${Math.min(...ns)}–${Math.max(...ns)}`;
  });
  slides.forEach(s => {
    const f = document.createElement('div'); f.className = 'foot';
    const isApx = s.classList.contains('apx');
    let html = `<span class="brand">DSAI5102 · Wikipedia live edits</span><span class="line"><i></i><i style="animation-delay:-2s"></i><i style="animation-delay:-4s"></i></span>`;
    if (isApx) {
      html += `<span class="more"><a class="chipb" data-back>← back to slide</a><a class="chipb" data-go="a0">index</a></span>`;
      html += `<span class="pn"><b>${s.dataset.label || ''}</b></span>`;
    } else {
      if (s.dataset.apx) html += `<span class="more">More:<a class="chipb" data-apx="${s.dataset.apx}">${byId[s.dataset.apx]?.dataset.label || ''}</a></span>`;
      html += `<span class="pn"><b>${s.dataset.n}</b> / ${main.length}</span>`;
    }
    f.innerHTML = html; s.appendChild(f);
  });

  document.addEventListener('click', e => {
    const a = e.target.closest('a[data-apx],a[data-go],a[data-back]');
    if (!a) return;
    e.preventDefault();
    if (a.dataset.apx) { backStack.push(slides[cur].id); go(slides.indexOf(byId[a.dataset.apx]), true); }
    else if (a.dataset.go) { if (!slides[cur].classList.contains('apx')) backStack.push(slides[cur].id); go(slides.indexOf(byId[a.dataset.go]), true); }
    else back();
  });

  function backLabel() { const id = backStack[backStack.length - 1] || slides[cur].dataset.from; const t = byId[id]; return t ? (t.dataset.n ? 'slide ' + t.dataset.n : t.dataset.label) : 'slide'; }
  function back() {
    let id = backStack.pop();
    if (!id) id = slides[cur].dataset.from;            // opened directly: go to the slide this page explains
    if (id && byId[id]) go(slides.indexOf(byId[id]), true);
  }

  // ---------- step builds ----------
  const frags = s => [...s.querySelectorAll('.f')].sort((a, b) => (+a.dataset.f || 0) - (+b.dataset.f || 0));
  function stepsShown(s) { return frags(s).filter(f => f.classList.contains('shown')).length; }
  function setSteps(s, n) {
    const fs = frags(s);
    // group fragments that share a data-f number
    const groups = []; fs.forEach(f => { const k = f.dataset.f || ('x' + groups.length); const g = groups.find(g => g.k === k); g ? g.els.push(f) : groups.push({ k, els: [f] }); });
    groups.forEach((g, i) => g.els.forEach(el => {
      const on = i < n; const was = el.classList.contains('shown');
      el.classList.toggle('shown', on);
      if (on && !was) { (el.classList.contains('count') ? [el] : el.querySelectorAll('.count')).forEach(countUp); }
      if (on && !was && el.dataset.on && hooks[el.dataset.on]) hooks[el.dataset.on](s, el, true);
      if (!on && was && el.dataset.on && hooks[el.dataset.on]) hooks[el.dataset.on](s, el, false);
    }));
    s.dataset.step = n; s._groups = groups.length;
    if (typeof syncPresenter === 'function') try { syncPresenter(); } catch (e) {}
  }
  function groupCount(s) { const ks = new Set(frags(s).map((f, i) => f.dataset.f || ('x' + i))); return ks.size; }

  function next() {
    const s = slides[cur], n = +(s.dataset.step || 0);
    if (n < groupCount(s)) setSteps(s, n + 1);
    else if (cur < slides.length - 1) go(cur + 1);
  }
  function prev() {
    const s = slides[cur], n = +(s.dataset.step || 0);
    if (n > 0) setSteps(s, n - 1);
    else if (cur > 0) go(cur - 1, false, true);
  }

  // ---------- go to a slide ----------
  function go(i, keepHistory, full) {
    if (i < 0 || i >= slides.length) return;
    const old = slides[cur];
    if (old && old !== slides[i]) { old.classList.remove('active'); leave(old); }
    cur = i; const s = slides[i];
    s.classList.add('active');
    setSteps(s, 0);
    enter(s);
    if (full) setSteps(s, groupCount(s));
    history.replaceState(null, '', '#' + s.id);
    const mi = main.indexOf(s);
    progress.style.width = mi >= 0 ? ((mi + 1) / main.length * 100) + '%' : '100%';
    progress.style.background = mi >= 0 ? '' : '#9A9EA6';
    const n = s.querySelector('aside.notes');
    notes.innerHTML = n ? n.innerHTML : '<i>No notes on this slide.</i>';
    const sc = s.querySelector('.apx-scroll'); if (sc) sc.scrollTop = 0;
    s.querySelectorAll('[data-back]').forEach(b => b.textContent = '← back to ' + backLabel());
    try { syncPresenter(); } catch (e) {}
    try { syncBar(); } catch (e) {}
  }

  function enter(s) {
    s.querySelectorAll('svg').forEach(v => v.unpauseAnimations && v.unpauseAnimations());
    s.querySelectorAll('.count').forEach(c => { if (!c.closest('.f')) countUp(c); });
    const h = hooks['enter_' + s.id]; if (h) h(s);
  }
  function leave(s) {
    s.querySelectorAll('svg').forEach(v => v.pauseAnimations && v.pauseAnimations());
    s.querySelectorAll('video').forEach(v => v.pause());
    (s._charts || []).forEach(c => c.destroy()); s._charts = [];
    s._spk = s._hod = s._daily = null;
    (s._timers || []).forEach(t => { clearInterval(t); cancelAnimationFrame(t); }); s._timers = [];
    const h = hooks['leave_' + s.id]; if (h) h(s);
  }

  // ---------- keys ----------
  addEventListener('keydown', e => {
    if (e.target.closest('input,textarea')) return;
    const k = e.key;
    if (['ArrowRight', 'PageDown', ' ', 'Enter'].includes(k)) { e.preventDefault(); next(); }
    else if (['ArrowLeft', 'PageUp', 'Backspace'].includes(k)) { e.preventDefault(); prev(); }
    else if (k === 'ArrowDown') { const sc = slides[cur].querySelector('.apx-scroll'); if (sc) { sc.scrollBy({ top: 200, behavior: 'smooth' }); e.preventDefault(); } else next(); }
    else if (k === 'ArrowUp') { const sc = slides[cur].querySelector('.apx-scroll'); if (sc) { sc.scrollBy({ top: -200, behavior: 'smooth' }); e.preventDefault(); } else prev(); }
    else if (k === 'Home') go(0);
    else if (k === 'End') go(main.length - 1);
    else if (k === 'n' || k === 'N') notes.classList.toggle('open');
    else if (k === 'a' || k === 'A') { if (!slides[cur].classList.contains('apx')) backStack.push(slides[cur].id); go(slides.indexOf(byId.a0), true); }
    else if (k === 'd' || k === 'D') { const s = slides[cur]; if (s.dataset.apx) { backStack.push(s.id); go(slides.indexOf(byId[s.dataset.apx]), true); } }
    else if (k === 'b' || k === 'B' || k === 'Escape') { if (slides[cur].classList.contains('apx')) back(); }
    else if (k === 'f' || k === 'F') { document.fullscreenElement ? document.exitFullscreen() : document.documentElement.requestFullscreen(); }
  });

  // ---------- helpers ----------
  const fmt = (v, d) => v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
  function countUp(el) {
    const to = parseFloat(el.dataset.to), d = +(el.dataset.dec || 0), dur = +(el.dataset.dur || 1400);
    const pre = el.dataset.pre || '', suf = el.dataset.suf || '';
    const t0 = performance.now(), delay = +(el.dataset.delay || 0);
    // big numbers on slides 16-17: tighten the font's wide '.' and ',' (see .tight in deck.css)
    const tight = el.closest('.atile, .bcard'); const put = s => { if (tight) el.innerHTML = s.replace(/([.,])/g, '<span class="tight">$1</span>'); else el.textContent = s; };
    put(pre + fmt(0, d) + suf);
    const tick = t => {
      const p = Math.min(1, Math.max(0, (t - t0 - delay) / dur)), e = 1 - Math.pow(1 - p, 3);
      put(pre + fmt(to * e, d) + suf);
      if (p < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }
  const css = v => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
  function chart(s, canvas, cfg) {
    const c = new Chart(canvas, cfg); (s._charts = s._charts || []).push(c); return c;
  }
  if (window.Chart) {
    Chart.defaults.font.family = '"Plus Jakarta Sans", "Segoe UI", sans-serif';
    Chart.defaults.font.size = 15;
    Chart.defaults.color = '#5B5F66';
    Chart.defaults.plugins.legend.display = false;
    Chart.defaults.plugins.tooltip.enabled = true;
    Chart.defaults.animation.duration = 1400;
  }
  const D = window.DECK_DATA;

  // the "live edit stream" canvas: dots at the real average rate (19.4 edits/s), bots yellow, humans purple
  const WIKIS = ['enwiki', 'wikidatawiki', 'commonswiki', 'cewiki', 'frwiki', 'dewiki', 'arwiki', 'ruwiki', 'jawiki', 'eswiki', 'itwiki', 'zhwiki', 'afwiki', 'trwiki', 'enwiktionary'];
  function streamCanvas(s, canvas, opts = {}) {
    const ctx = canvas.getContext('2d'); const W = canvas.width = canvas.offsetWidth || 1600, H = canvas.height = canvas.offsetHeight || 900;
    const dots = []; let last = performance.now(), acc = 0; const rate = opts.rate || 19.4;
    const lanes = opts.lanes || [80, 820];
    const frame = t => {
      const dt = Math.min(0.05, (t - last) / 1000); last = t; acc += dt * rate;
      while (acc >= 1) {
        acc -= 1; const bot = Math.random() < 0.486;
        dots.push({ x: -20, y: lanes[0] + Math.random() * (lanes[1] - lanes[0]), v: 120 + Math.random() * 160, bot, r: bot ? 3 : 3.6, label: Math.random() < 0.08 ? WIKIS[Math.floor(Math.random() * WIKIS.length)] : null });
      }
      ctx.clearRect(0, 0, W, H);
      for (let i = dots.length - 1; i >= 0; i--) {
        const d = dots[i]; d.x += d.v * dt;
        if (d.x > W + 40) { dots.splice(i, 1); continue; }
        const a = Math.min(1, d.x / 200) * (opts.alpha || 0.85);
        ctx.globalAlpha = a; ctx.fillStyle = d.bot ? '#F9D45C' : '#A989C5';
        ctx.beginPath(); ctx.arc(d.x, d.y, d.r, 0, 7); ctx.fill();
        ctx.globalAlpha = a * 0.25; ctx.fillRect(d.x - 40, d.y - 0.5, 40, 1);
        if (d.label) { ctx.globalAlpha = a * 0.55; ctx.fillStyle = opts.labelColor || '#9A9EA6'; ctx.font = '13px "JetBrains Mono", Consolas, monospace'; ctx.fillText(d.label, d.x + 8, d.y + 4); }
      }
      ctx.globalAlpha = 1;
      s._timers.push(requestAnimationFrame(frame));
    };
    s._timers = s._timers || []; s._timers.push(requestAnimationFrame(frame));
  }
  // ---------- problem slide: a recent-changes feed, newest on top; it starts slow, then runs at the real ~19.4 edits/s ----------
  function feedCanvas(s, canvas, onReal) {
    const ctx = canvas.getContext('2d'); const dpr = Math.min(2, window.devicePixelRatio || 1);
    const W = canvas.offsetWidth || 880, H = canvas.offsetHeight || 408, RH = 46;
    canvas.width = W * dpr; canvas.height = H * dpr; ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const rows = []; const t0 = performance.now(); let last = t0, acc = 0, off = 0, real = false;
    const pad = n => String(n).padStart(2, '0');
    const mk = () => { const bot = Math.random() < 0.486, d = new Date(), big = Math.random() < 0.15;
      const n = Math.round(big ? 400 + Math.random() * 4000 : 2 + Math.random() * 380), plus = Math.random() < 0.72;
      return { time: pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()), wiki: WIKIS[Math.floor(Math.random() * WIKIS.length)], bot,
        bar: 140 + Math.random() * 260, bar2: Math.random() < 0.4 ? 40 + Math.random() * 90 : 0, diff: (plus ? '+' : '\u2212') + n.toLocaleString('en-US'), plus, born: performance.now() }; };
    for (let i = 0; i < 4; i++) rows.push(mk());
    const pill = (x, y, txt, font, bg, fg, padX) => { ctx.font = font; const w = ctx.measureText(txt).width + padX * 2;
      ctx.fillStyle = bg; ctx.beginPath(); ctx.roundRect(x, y - 13, w, 26, 13); ctx.fill(); ctx.fillStyle = fg; ctx.fillText(txt, x + padX, y + 5); return w; };
    const frame = t => {
      const dt = Math.min(0.05, (t - last) / 1000); last = t; const el = (t - t0) / 1000;
      const k = Math.min(1, el / 3.6), rate = 19.4 * (0.08 + 0.92 * k * k);
      if (!real && k >= 1) { real = true; onReal && onReal(); }
      acc += dt * rate;
      while (acc >= 1) { acc -= 1; rows.unshift(mk()); off += RH; }
      off *= Math.exp(-dt * 16); if (off < 0.2) off = 0;
      const maxRows = Math.ceil(H / RH) + 3; if (rows.length > maxRows) rows.length = maxRows;
      ctx.clearRect(0, 0, W, H);
      rows.forEach((r, i) => {
        const y = i * RH - off + RH / 2 + 4; if (y > H + RH) return;
        const fresh = Math.max(0, 1 - (t - r.born) / 900);
        if (fresh > 0) { ctx.fillStyle = 'rgba(80,158,227,' + 0.10 * fresh + ')'; ctx.fillRect(0, y - RH / 2, W, RH); }
        ctx.fillStyle = '#EEF2F7'; ctx.fillRect(24, y + RH / 2 - 1, W - 48, 1);
        ctx.font = '15px "JetBrains Mono", Consolas, monospace'; ctx.fillStyle = '#8A94A3'; ctx.fillText(r.time, 24, y + 5);
        let x = 124;
        x += pill(x, y, r.wiki, '500 14px "JetBrains Mono", Consolas, monospace', '#EAF3FC', '#2F80D0', 10) + 10;
        x += pill(x, y, r.bot ? 'BOT' : 'HUMAN', '700 12px "Plus Jakarta Sans", sans-serif', r.bot ? '#FFF1C2' : '#F1EAF8', r.bot ? '#8A6A00' : '#7B5EA0', 10) + 16;
        ctx.fillStyle = '#E3EAF2'; ctx.beginPath(); ctx.roundRect(x, y - 6, r.bar, 12, 6); ctx.fill();
        if (r.bar2) { ctx.beginPath(); ctx.roundRect(x + r.bar + 10, y - 6, r.bar2, 12, 6); ctx.fill(); }
        ctx.font = '600 15px "JetBrains Mono", Consolas, monospace'; ctx.fillStyle = r.plus ? '#2E8B6E' : '#D64545';
        const dw = ctx.measureText(r.diff).width; ctx.fillText(r.diff, W - 24 - dw, y + 5);
      });
      s._timers.push(requestAnimationFrame(frame));
    };
    s._timers = s._timers || []; s._timers.push(requestAnimationFrame(frame));
  }
  // ---------- title slide: a turning dotted globe; each edit pings where its wiki's language is mainly spoken ----------
  // Coarse continent outlines [lon, lat], only used to place land dots.
  const LAND = [
    [[-166,68],[-156,71],[-140,70],[-128,70],[-115,68],[-95,72],[-80,73],[-65,62],[-60,55],[-56,52],[-66,45],[-70,42],[-75,38],[-76,35],[-81,31],[-80,25],[-83,29],[-90,30],[-97,27],[-97,22],[-92,18],[-87,21],[-88,16],[-83,15],[-83,10],[-79,9],[-80,7],[-86,12],[-92,14],[-96,16],[-105,20],[-106,23],[-110,24],[-112,29],[-115,31],[-117,32],[-118,34],[-121,35],[-124,40],[-124,46],[-125,49],[-131,54],[-136,58],[-145,60],[-152,59],[-158,57],[-162,55],[-158,58],[-163,60],[-165,62],[-166,64]],
    [[-73,78],[-60,82],[-30,83],[-20,80],[-20,72],[-25,68],[-40,65],[-44,60],[-50,62],[-54,67],[-58,74],[-68,76]],
    [[-80,9],[-75,11],[-72,12],[-62,11],[-60,8],[-52,5],[-50,0],[-44,-2],[-35,-5],[-35,-9],[-39,-15],[-41,-22],[-48,-26],[-53,-33],[-58,-38],[-62,-39],[-65,-42],[-66,-47],[-69,-51],[-68,-55],[-72,-53],[-74,-47],[-73,-40],[-71,-30],[-70,-18],[-76,-14],[-81,-6],[-80,-1],[-78,2],[-77,7]],
    [[-9,37],[-9,43],[-2,43.5],[-4,48],[2,51],[5,53],[8,54],[8,57],[11,58],[5,58],[5,62],[11,64],[15,68],[20,70],[28,71],[40,67],[45,68],[55,69],[60,70],[70,73],[80,73],[100,77],[112,74],[130,72],[140,72],[160,70],[170,70],[180,69],[178,64],[170,60],[163,58],[160,53],[156,51],[155,57],[142,59],[136,54],[140,48],[135,43],[130,42],[129,35],[126,35],[126,38],[122,40],[118,38],[122,32],[120,26],[115,22],[108,21],[106,17],[109,12],[105,9],[103,10],[100,13],[99,8],[103,1.5],[101,3],[98,8],[98,16],[94,17],[92,22],[88,22],[85,20],[80,15],[80,10],[77,8],[73,16],[72,21],[68,23],[66,25],[57,25],[56,27],[50,30],[48,30],[50,27],[51,24],[56,26],[59,22],[56,18],[52,16],[45,13],[43,13],[39,21],[35,28],[33,30],[35,32],[36,36],[28,37],[26,40],[23,38],[20,40],[16,40],[16,38],[12,42],[9,44],[3,43],[0,40],[-2,37],[-5,36]],
    [[-17,21],[-17,15],[-12,8],[-8,4.5],[-2,5],[5,6],[9,4],[9,0],[13,-5],[12,-12],[14,-23],[18,-34],[20,-35],[26,-34],[32,-29],[35,-24],[35,-18],[40,-15],[40,-10],[39,-5],[42,0],[48,4],[51,11],[44,11],[43,12],[39,16],[37,22],[35,27],[32,31],[29,31],[20,32],[20,30],[15,32],[11,33],[10,37],[3,37],[-2,35],[-6,36],[-10,30],[-13,27]],
    [[44,-25],[47,-25],[50,-15],[49,-12],[44,-16]],
    [[114,-22],[114,-34],[118,-35],[124,-33],[131,-31],[138,-35],[141,-38],[146,-39],[150,-37],[153,-31],[153,-25],[146,-19],[142,-11],[136,-12],[136,-15],[130,-12],[126,-14],[122,-18]],
    [[130,31],[131,34],[135,34],[140,35],[141,38],[142,43],[145,44],[141,45],[140,41],[136,37],[132,35],[129,33]],
    [[-5,50],[1,51],[2,53],[0,54],[-2,56],[-2,58],[-5,58.5],[-6,56],[-5,55],[-3,54],[-4,53],[-5,52]],
    [[-10,52],[-6,52],[-6,54],[-8,55],[-10,54]],
    [[-24,65],[-22,66.5],[-14,66.5],[-13,65],[-18,63.5]],
    [[95,5],[98,4],[106,-6],[104,-6],[100,-2]],
    [[109,1],[111,-3],[116,-4],[119,1],[117,7],[113,3]],
    [[131,-1],[141,-3],[150,-10],[143,-9],[138,-8],[132,-4]],
    [[120,18],[122,18],[126,7],[125,6],[122,10],[120,14]],
    [[172,-34],[178,-38],[175,-41],[172,-44],[167,-46],[171,-41]],
  ];
  // The busiest wikis in the window: share of all edits (%), flagged-bot share, and where the language is mainly
  // spoken (approximate). null = multilingual (Wikidata, Commons, everything else): pings anywhere on land.
  // Shares and bot shares: mart_wiki_activity, 28 Oct – 11 Nov 2026; overall 48.6% bots.
  const SPOTS = { en: [[-98,39],[-100,55],[-2,53],[78,21],[134,-25]], zh: [[104,35],[113,28],[121,24]], fr: [[2.5,46.5],[-72,47]],
    ar: [[31,27],[45,24],[-6,32],[44,33]], ru: [[38,55],[60,57],[85,55]], es: [[-3.7,40],[-102,23],[-64,-34],[-74,4]] };
  const GLOBE_WIKIS = [
    ['commonswiki', 26.39, .679, null], ['wikidatawiki', 26.00, .154, null], ['enwiki', 10.95, .309, SPOTS.en],
    ['enwiktionary', 3.97, .896, SPOTS.en], ['zhwikisource', 2.46, .983, SPOTS.zh], ['cewiki', 2.26, .995, [[45.7,43.3]]],
    ['frwiki', 1.84, .373, SPOTS.fr], ['elwiktionary', 1.54, .992, [[22,39]]], ['dewiki', 1.46, .206, [[10,51]]],
    ['zhwiktionary', 1.16, .989, SPOTS.zh], ['arwiki', 1.16, .816, SPOTS.ar], ['ruwiki', 1.15, .361, SPOTS.ru],
    ['afwiki', .90, .980, [[25,-29]]], ['itwiki', .90, .317, [[12.5,42.5]]], ['zhwiki', .88, .349, SPOTS.zh],
    ['trwiki', .88, .680, [[35,39]]], ['eswiki', .86, .129, SPOTS.es], ['jawiki', .74, .198, [[138,36]]],
    ['kaawiki', .64, .557, [[59,43]]], ['cebwiki', .60, .995, [[123.9,10.3]]], ['kowiktionary', .47, .811, [[127.5,36.5]]],
    ['mgwiktionary', .45, 1, [[47,-19]]], ['other wikis', 12.34, .478, null],
  ];
  const GLOBE_TOTAL = GLOBE_WIKIS.reduce((a, w) => a + w[1], 0);
  let LAND_DOTS = null;
  function landDots() {
    if (LAND_DOTS) return LAND_DOTS;
    const inside = (p, lon, lat) => { let c = false; for (let i = 0, j = p.length - 1; i < p.length; j = i++) {
      const [xi, yi] = p[i], [xj, yj] = p[j]; if ((yi > lat) !== (yj > lat) && lon < (xj - xi) * (lat - yi) / (yj - yi) + xi) c = !c; } return c; };
    LAND_DOTS = [];
    for (let lat = -58; lat <= 82; lat += 2.4) {
      const step = 2.4 / Math.max(0.25, Math.cos(lat * Math.PI / 180));
      for (let lon = -180; lon < 180; lon += step) if (LAND.some(p => inside(p, lon, lat))) LAND_DOTS.push([lon, lat]);
    }
    return LAND_DOTS;
  }
  function globeCanvas(s, canvas) {
    const ctx = canvas.getContext && canvas.getContext('2d'); if (!ctx) return;
    const css = canvas.offsetWidth || 860, dpr = Math.min(2, window.devicePixelRatio || 1);
    canvas.width = canvas.height = css * dpr; ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const C = css / 2, R = css / 2 - 28, tilt = 22 * Math.PI / 180, rad = Math.PI / 180;
    const still = window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches;
    const dots = landDots(), pings = []; const t0 = performance.now(); let last = t0, acc = 0;
    const pick = () => { let r = Math.random() * GLOBE_TOTAL; for (const w of GLOBE_WIKIS) { if ((r -= w[1]) <= 0) return w; } return GLOBE_WIKIS[0]; };
    const project = (lon, lat, lon0) => {
      const l = (lon - lon0) * rad, p = lat * rad, cp = Math.cos(p);
      const z = Math.sin(tilt) * Math.sin(p) + Math.cos(tilt) * cp * Math.cos(l);
      return [C + R * cp * Math.sin(l), C - R * (Math.cos(tilt) * Math.sin(p) - Math.sin(tilt) * cp * Math.cos(l)), z];
    };
    const draw = (t, dt) => {
      const age = (t - t0) / 1000, lon0 = 10 - age * 9;   // one turn every 40 s, starting over Europe and Africa
      const grow = still ? 1 : Math.min(1, age / 1.2), ease = 1 - Math.pow(1 - grow, 3);
      ctx.clearRect(0, 0, css, css);
      // atmosphere
      const g = ctx.createRadialGradient(C, C, R * .92, C, C, R * 1.08); g.addColorStop(0, 'rgba(150,205,255,0)'); g.addColorStop(.5, 'rgba(150,205,255,.35)'); g.addColorStop(1, 'rgba(150,205,255,0)');
      ctx.globalAlpha = ease; ctx.fillStyle = g; ctx.beginPath(); ctx.arc(C, C, R * 1.08, 0, 7); ctx.fill();
      // graticule
      ctx.strokeStyle = 'rgba(207,227,246,.16)'; ctx.lineWidth = 1;
      const line = pts => { ctx.beginPath(); let on = false; for (const [lo, la] of pts) { const [x, y, z] = project(lo, la, lon0); if (z > 0) { on ? ctx.lineTo(x, y) : ctx.moveTo(x, y); on = true; } else on = false; } ctx.stroke(); };
      for (let lo = -180; lo < 180; lo += 30) { const pts = []; for (let la = -80; la <= 80; la += 4) pts.push([lo, la]); line(pts); }
      for (let la = -60; la <= 60; la += 30) { const pts = []; for (let lo = -180; lo <= 180; lo += 4) pts.push([lo, la]); line(pts); }
      // land
      ctx.fillStyle = '#e4f1fd';
      for (const [lo, la] of dots) { const [x, y, z] = project(lo, la, lon0); if (z <= 0) continue;
        ctx.globalAlpha = ease * (0.25 + 0.7 * z); ctx.beginPath(); ctx.arc(x, y, 2.3, 0, 7); ctx.fill(); }
      // new edits at the window's real rate
      if (!still && grow >= 1) { acc += dt * 19.4;
        while (acc >= 1) { acc -= 1; const w = pick(), bot = Math.random() < w[2];
          const at = w[3] ? w[3][Math.floor(Math.random() * w[3].length)] : dots[Math.floor(Math.random() * dots.length)];
          pings.push({ lon: at[0] + (Math.random() - .5) * 5, lat: at[1] + (Math.random() - .5) * 4, bot, born: t, label: Math.random() < 0.07 && w[0] !== 'other wikis' ? w[0] : null }); }
        if (pings.length > 160) pings.splice(0, pings.length - 160);
      }
      // pings: a glowing dot with a ring that grows and fades over 2.4 s
      ctx.font = '600 15px "JetBrains Mono", Consolas, monospace';
      for (let i = pings.length - 1; i >= 0; i--) {
        const p = pings[i], k = (t - p.born) / 2400; if (k >= 1) { pings.splice(i, 1); continue; }
        const [x, y, z] = project(p.lon, p.lat, lon0); if (z <= 0.05) continue;
        const col = p.bot ? '255,214,74' : '214,182,255', fade = (1 - k * k) * Math.min(1, z * 2.5);
        ctx.globalAlpha = 1;
        const glow = ctx.createRadialGradient(x, y, 0, x, y, 14); glow.addColorStop(0, `rgba(${col},${0.55 * fade})`); glow.addColorStop(1, `rgba(${col},0)`);
        ctx.fillStyle = glow; ctx.beginPath(); ctx.arc(x, y, 14, 0, 7); ctx.fill();
        ctx.strokeStyle = `rgba(${col},${0.9 * (1 - k) * Math.min(1, z * 2.5)})`; ctx.lineWidth = 2;
        ctx.beginPath(); ctx.arc(x, y, 4 + 26 * k, 0, 7); ctx.stroke();
        ctx.fillStyle = `rgba(${col},${fade})`; ctx.beginPath(); ctx.arc(x, y, 4.5, 0, 7); ctx.fill();
        if (p.label) { ctx.fillStyle = `rgba(255,255,255,${0.95 * fade})`; ctx.fillText(p.label, x + 11, y - 9); }
      }
      // light from the top left
      const sh = ctx.createRadialGradient(C - R * .4, C - R * .45, R * .1, C, C, R * 1.02);
      sh.addColorStop(0, 'rgba(255,255,255,.10)'); sh.addColorStop(.6, 'rgba(255,255,255,0)'); sh.addColorStop(1, 'rgba(6,20,36,.35)');
      ctx.globalAlpha = ease; ctx.fillStyle = sh; ctx.beginPath(); ctx.arc(C, C, R, 0, 7); ctx.fill(); ctx.globalAlpha = 1;
    };
    s._timers = s._timers || [];
    if (still) { draw(t0, 0); return; }
    const frame = t => { const dt = Math.min(0.05, (t - last) / 1000); last = t; draw(t, dt); s._timers.push(requestAnimationFrame(frame)); };
    s._timers.push(requestAnimationFrame(frame));
  }
  function liveCounter(s, el) {
    const t0 = performance.now();
    const id = setInterval(() => { el.textContent = fmt(Math.floor((performance.now() - t0) / 1000 * 19.4), 0); }, 80);
    (s._timers = s._timers || []).push(id);
  }

  // ---------- per-slide hooks ----------
  const hooks = {
    enter_s1(s) { globeCanvas(s, s.querySelector('canvas.bg')); liveCounter(s, s.querySelector('#live1')); },
    enter_s2(s) { const sp = s.querySelector('.rc-speed'); sp.classList.remove('on'); feedCanvas(s, s.querySelector('canvas.rc'), () => sp.classList.add('on')); liveCounter(s, s.querySelector('#live2')); },
    enter_s17(s) { globeCanvas(s, s.querySelector('canvas.bg'));
      // edits to Wikimedia wikis during a 15-minute talk at the window's average (25,093,717 in 15 days = 19.4/s):
      // count up to ~17,400, then keep ticking at that rate while the slide is open
      const el = s.querySelector('.wq-live'); if (el) { const rate = 25093717 / (15 * 86400), base = Math.round(rate * 900), t0 = performance.now();
        let n = 0; const up = setInterval(() => { const p = Math.min(1, (performance.now() - t0) / 2400), e = 1 - Math.pow(1 - p, 3);
          n = p < 1 ? Math.round(base * e) : Math.round(base + rate * (performance.now() - t0 - 2400) / 1000); el.textContent = n.toLocaleString('en-US'); }, 50);
        (s._timers = s._timers || []).push(up); } },

    enter_s5(s) {
      const h = D.hourly;
      const labels = h.labels.map(l => l.slice(0, 5) + ' ' + l.slice(6) + ':00');
      chart(s, s.querySelector('#c-hourly'), {
        type: 'line',
        data: { labels, datasets: [
          { label: 'Bots', data: h.bots, borderColor: '#F9D45C', backgroundColor: 'rgba(249,212,92,.18)', fill: true, pointRadius: 0, borderWidth: 1.8, tension: .25 },
          { label: 'Humans', data: h.humans, borderColor: '#A989C5', backgroundColor: 'rgba(169,137,197,.12)', fill: true, pointRadius: 0, borderWidth: 1.8, tension: .25 } ] },
        options: { maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
          animation: { x: { type: 'number', easing: 'linear', duration: 6, from: NaN, delay: ctx => ctx.type === 'data' ? ctx.index * 6 : 0 }, y: { duration: 0 } },
          scales: { x: { grid: { display: false }, ticks: { maxTicksLimit: 8, maxRotation: 0, callback: function (v) { const l = this.getLabelForValue(v); return l.endsWith('00:00') ? l.slice(0, 5).replace('10-', 'Oct ').replace('11-', 'Nov ') : ''; }, autoSkip: false }, border: { display: false } },
            y: { grid: { color: '#ECEAE4' }, border: { display: false }, ticks: { callback: v => (v / 1000) + 'k' }, title: { display: true, text: 'Edits per hour, all wikis' } } } }
      });
    },
    show_daily(s, el, on) {
      if (!on || s._daily) return;
      const d = D.daily;
      s._daily = chart(s, s.querySelector('#c-daily'), {
        type: 'bar',
        data: { labels: d.labels.map(l => (l.startsWith('10') ? 'Oct ' : 'Nov ') + +l.slice(3)), datasets: [{ data: d.botShare,
          backgroundColor: d.botShare.map(v => v === Math.max(...d.botShare) ? '#E3B92E' : (v === Math.min(...d.botShare) ? '#D8C98F' : 'rgba(249,212,92,.65)')), borderRadius: 4 }] },
        options: { maintainAspectRatio: false, scales: { x: { grid: { display: false }, ticks: { font: { size: 12 } } }, y: { min: 0, max: 70, grid: { color: '#ECEAE4' }, ticks: { callback: v => v + '%', padding: 8 }, border: { display: false } } }, layout: { padding: { left: 6 } },
          plugins: { tooltip: { callbacks: { label: c => c.parsed.y + '% bots' } } } },
        plugins: [{ id: 'lbl', afterDatasetsDraw(c) { const { ctx } = c; const m = c.getDatasetMeta(0); const v = c.data.datasets[0].data;
          ctx.save(); ctx.font = '600 13px "Plus Jakarta Sans", sans-serif'; ctx.fillStyle = '#5B5F66'; ctx.textAlign = 'center';
          const iMax = v.indexOf(Math.max(...v)), iMin = v.indexOf(Math.min(...v));
          [iMax, iMin].forEach(i => ctx.fillText(v[i] + '%', m.data[i].x, m.data[i].y - 6)); ctx.restore(); } }]
      });
    },
    leave_s5(s) { s._daily = null; },

    enter_s6(s) { dumbbell(s); },
    show_human(s, el, on) { s.querySelectorAll('.dumb .d2, .dumb .bar').forEach(e => e.classList.toggle('go', on)); placeDumb(s, on); },
    show_conc(s, el, on) { s.querySelectorAll('.conc i').forEach(i => i.style.width = on ? i.dataset.w + '%' : '0'); },

    enter_s7(s) {
      const H = D.hod, hrs = [...Array(24).keys()].map(h => String(h).padStart(2, '0'));
      const band = { id: 'band', beforeDatasetsDraw(c) { const { ctx, chartArea: a, scales: { x } } = c;
        const x0 = x.getPixelForValue(13) - (x.getPixelForValue(1) - x.getPixelForValue(0)) / 2, x1 = x.getPixelForValue(21) + (x.getPixelForValue(1) - x.getPixelForValue(0)) / 2;
        ctx.save(); ctx.fillStyle = s._band ? 'rgba(169,137,197,.14)' : 'rgba(0,0,0,0)'; ctx.fillRect(x0, a.top, x1 - x0, a.bottom - a.top);
        if (s._band) { ctx.fillStyle = '#6E4E8E'; ctx.font = '600 14px "Plus Jakarta Sans", sans-serif'; ctx.textAlign = 'center'; ctx.fillText('13:00–22:00 · above 14,800 an hour', (x0 + x1) / 2, a.bottom - 14); }
        ctx.restore(); } };
      const peaks = { id: 'peaks', afterDatasetsDraw(c) { const m = c.getDatasetMeta(0); const { ctx } = c; ctx.save();
        [[17, '16,434 at 17:00', -16], [4, '7,911 at 04:00', 28]].forEach(([i, t, dy]) => { const p = m.data[i]; if (!p) return;
          ctx.fillStyle = '#6E4E8E'; ctx.beginPath(); ctx.arc(p.x, p.y, 6, 0, 7); ctx.fill();
          ctx.font = '600 15px "Plus Jakarta Sans", sans-serif'; ctx.textAlign = 'center'; ctx.fillText(t, p.x, p.y + dy); }); ctx.restore(); } };
      s._hod = chart(s, s.querySelector('#c-hod'), {
        type: 'line',
        data: { labels: hrs, datasets: [
          { label: 'Humans', data: H.humans, borderColor: '#A989C5', backgroundColor: '#A989C5', borderWidth: 3.5, pointRadius: 0, tension: .35 },
          { label: 'Bots', data: H.bots, borderColor: '#F9D45C', backgroundColor: '#F9D45C', borderWidth: 3, pointRadius: 0, tension: .35 } ] },
        options: { maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
          animation: { duration: 1800 },
          scales: { x: { grid: { display: false }, title: { display: true, text: 'Hour of day (UTC)' } }, y: { min: 0, max: 18000, grid: { color: '#ECEAE4' }, border: { display: false }, ticks: { callback: v => (v / 1000) + 'k' }, title: { display: true, text: 'Average Wikipedia edits in the hour' } } } },
        plugins: [band, peaks]
      });
    },
    show_band(s, el, on) { s._band = on; s._hod && s._hod.update('none'); },
    leave_s7(s) { s._band = false; },

    enter_s8(s) {
      const val = { id: 'vals', afterDatasetsDraw(c) { const { ctx } = c; ctx.save(); ctx.font = '600 16px "Plus Jakarta Sans", sans-serif'; ctx.textAlign = 'center'; ctx.fillStyle = '#111214';
        c.data.datasets.forEach((ds, di) => { if (c.getDatasetMeta(di).hidden) return; c.getDatasetMeta(di).data.forEach((b, i) => ctx.fillText(ds.data[i] + '%', b.x, b.y - 8)); }); ctx.restore(); } };
      s._spk = chart(s, s.querySelector('#c-spikes'), {
        type: 'bar',
        data: { labels: ['1,000–5,000 views', '5,000–20,000', '20,000+'], datasets: [
          { label: 'Spikes', data: [14.2, 32.4, 56.9], backgroundColor: '#EF8C8C', borderRadius: 5, barPercentage: .82, categoryPercentage: .7 },
          { label: 'Steadily popular articles', data: [19.8, 47.9, 83.6], backgroundColor: '#A7ADB8', borderRadius: 5, barPercentage: .82, categoryPercentage: .7, hidden: true } ] },
        options: { maintainAspectRatio: false, animation: { duration: 1200 },
          scales: { x: { grid: { display: false }, title: { display: true, text: 'Views in the hour' } }, y: { min: 0, max: 100, grid: { color: '#ECEAE4' }, border: { display: false }, ticks: { callback: v => v + '%' }, title: { display: true, text: 'Hours with an edit in the next 2 h' } } } },
        plugins: [val]
      });
    },
    show_fail(s, el, on) { s.querySelector('#t-dbt').classList.toggle('fail', on); s.querySelector('#t-exp').classList.toggle('skip', on); },
    show_base(s, el, on) { if (!s._spk) return; s._spk.setDatasetVisibility(1, on); s._spk.update(); },

    enter_s3(s) { },
  };

  // dumbbell: rank by edits -> rank by human edits (log scale), finding 2
  const RANKS = [['English', 1, 1, 30.9], ['Chechen', 2, 65, 99.5], ['French', 3, 3, 37.3], ['German', 4, 2, 20.6], ['Arabic', 5, 17, 81.6], ['Russian', 6, 5, 36.1], ['Afrikaans', 7, 56, 98.0]];
  const X0 = 150, X1 = 700, lx = r => X0 + Math.log(r) / Math.log(70) * (X1 - X0);
  function dumbbell(s) {
    const svg = s.querySelector('svg.dumb'); if (svg.dataset.built) { placeDumb(s, false); return; }
    let h = '';
    [1, 2, 5, 10, 20, 50].forEach(t => h += `<g class="axis"><line x1="${lx(t)}" x2="${lx(t)}" y1="30" y2="${30 + RANKS.length * 62}"/><text x="${lx(t)}" y="18" text-anchor="middle">#${t}</text></g>`);
    RANKS.forEach(([n, a, b, bs], i) => {
      const y = 62 + i * 62, bad = b - a >= 10;
      h += `<text class="name" x="0" y="${y + 6}">${n}</text>`;
      h += `<line class="bar" data-a="${a}" data-b="${b}" x1="${lx(a)}" x2="${lx(a)}" y1="${y}" y2="${y}" stroke="${bad ? '#EF8C8C' : '#D3CFE0'}"/>`;
      h += `<circle cx="${lx(a)}" cy="${y}" r="9" fill="#5B5F66"/>`;
      h += `<circle class="d2" data-a="${a}" data-b="${b}" cx="${lx(a)}" cy="${y}" r="10" fill="${bad ? '#B54848' : '#A989C5'}"/>`;
      h += `<text class="rk" data-b="${b}" x="${lx(Math.max(a, b)) + 18}" y="${y + 6}" style="opacity:0;transition:opacity .6s 1s" fill="${bad ? '#B54848' : '#5B5F66'}" font-weight="600">#${b}${bad ? ` · ${bs}% bots` : ''}</text>`;
    });
    svg.innerHTML = h; svg.dataset.built = 1;
  }
  function placeDumb(s, on) {
    s.querySelectorAll('.dumb .d2').forEach(c => c.setAttribute('cx', lx(on ? +c.dataset.b : +c.dataset.a)));
    s.querySelectorAll('.dumb .bar').forEach(l => l.setAttribute('x2', lx(on ? +l.dataset.b : +l.dataset.a)));
    s.querySelectorAll('.dumb .rk').forEach(t => t.style.opacity = on ? 1 : 0);
  }

  // ---------- overview (O): every slide and appendix page, click to jump ----------
  const ov = document.createElement('div'); ov.id = 'overview'; document.body.appendChild(ov);
  const titleOf = s => (s.querySelector('h1,h2')?.textContent || s.id).replace(/\s+/g, ' ').trim();
  function openOverview() {
    const item = s => `<a data-i="${slides.indexOf(s)}" class="${s === slides[cur] ? 'cur' : ''}"><b>${s.classList.contains('apx') ? s.dataset.label : s.dataset.n}</b>${titleOf(s)}</a>`;
    ov.innerHTML = `<h4>Slides</h4><div class="g">${main.map(item).join('')}</div><h4>Appendix</h4><div class="g">${slides.filter(s => s.classList.contains('apx')).map(item).join('')}</div>`;
    ov.classList.add('open');
  }
  ov.addEventListener('click', e => { const a = e.target.closest('a[data-i]'); if (!a) return; ov.classList.remove('open');
    if (slides[+a.dataset.i].classList.contains('apx') && !slides[cur].classList.contains('apx')) backStack.push(slides[cur].id);
    go(+a.dataset.i, true, slides[+a.dataset.i].classList.contains('apx')); });
  addEventListener('keydown', e => {
    if (e.key === 'o' || e.key === 'O') { ov.classList.contains('open') ? ov.classList.remove('open') : openOverview(); }
    else if (e.key === 'Escape') ov.classList.remove('open');
    else if (e.key === 's' || e.key === 'S') openPresenter();
    else if (e.key === 'p' || e.key === 'P') { const w = slides[cur].querySelector('.demo-vid'); if (w) toggleDemo(w); }
  });

  // ---------- recorded demo: click or P plays/pauses; the step whose start time has passed lights up ----------
  function toggleDemo(w) { const v = w.querySelector('video'); if (v.ended) v.currentTime = 0; v.paused ? v.play() : v.pause(); }
  document.querySelectorAll('.demo-vid').forEach(w => {
    const v = w.querySelector('video'), bar = w.querySelector('.demo-bar i');
    const steps = [...w.closest('.slide').querySelectorAll('.demo-steps [data-t]')];
    w.addEventListener('click', () => toggleDemo(w));
    v.addEventListener('play', () => w.classList.add('playing'));
    v.addEventListener('pause', () => w.classList.remove('playing'));
    v.addEventListener('timeupdate', () => {
      bar.style.width = (v.duration ? v.currentTime / v.duration * 100 : 0) + '%';
      const t = v.currentTime; let k = -1; steps.forEach((s, i) => { if (t >= +s.dataset.t) k = i; });
      steps.forEach((s, i) => s.classList.toggle('on', i === k && (t > 0 || !v.paused)));
    });
  });

  // ---------- touch swipe ----------
  let tx = null;
  addEventListener('touchstart', e => { tx = e.touches[0].clientX; }, { passive: true });
  addEventListener('touchend', e => { if (tx === null) return; const dx = e.changedTouches[0].clientX - tx; tx = null; if (Math.abs(dx) > 50) (dx < 0 ? next : prev)(); });

  // ---------- presenter window (S): current + next slide, script, timer ----------
  // The popup is about:blank written from here, so it can call this window directly (works from file://).
  // Its two previews are iframes of this deck with ?preview=<id>, which shows one slide fully built, no chrome.
  let pw = null, t0 = null;
  function openPresenter() {
    if (pw && !pw.closed) { pw.focus(); return; }
    pw = window.open('', 'deck-presenter', 'width=1280,height=800');
    if (!pw) { alert('Allow pop-ups for this page to open the presenter view.'); return; }
    t0 = t0 || Date.now();
    const base = location.href.split('#')[0].split('?')[0];
    pw.document.write(`<!doctype html><html><head><title>Presenter · Wikipedia live edits</title><style>
      body{margin:0;background:#0b1a2b;color:#F2F1EC;font-family:Plus Jakarta Sans,Segoe UI,sans-serif;display:grid;grid-template-columns:1.35fr 1fr;grid-template-rows:auto 1fr;gap:14px;padding:14px;height:100vh;box-sizing:border-box}
      .card{background:#102A43;border:1px solid #1f3a5a;border-radius:12px;padding:12px;display:flex;flex-direction:column;min-height:0}
      .h{font:600 11px Plus Jakarta Sans,sans-serif;letter-spacing:.12em;text-transform:uppercase;color:#A9D0F2;margin-bottom:8px;display:flex;justify-content:space-between}
      .fr{position:relative;width:100%;aspect-ratio:16/9;overflow:hidden;border-radius:6px;background:#000}
      .fr iframe{position:absolute;left:0;top:0;width:1600px;height:900px;border:0;transform-origin:0 0;pointer-events:none}
      #notes{font-size:21px;line-height:1.5;overflow:auto;flex:1} #notes b{color:#A9D0F2}
      #timer{font:600 44px Plus Jakarta Sans,sans-serif;font-variant-numeric:tabular-nums} #pos{color:#9A9EA6;font-size:16px}
      button{background:#16304d;color:#A9D0F2;border:1px solid #1f3a5a;border-radius:8px;padding:8px 14px;font:600 14px Plus Jakarta Sans,sans-serif;cursor:pointer;margin-right:6px}
    </style></head><body>
      <div class="card" style="grid-row:1/3"><div class="h"><span>Current</span><span id="step"></span></div><div class="fr"><iframe id="cur"></iframe></div>
        <div class="h" style="margin-top:14px"><span>Script</span></div><div id="notes"></div></div>
      <div class="card"><div class="h"><span>Next</span></div><div class="fr"><iframe id="nxt"></iframe></div></div>
      <div class="card"><div class="h"><span>Timer</span><span>target 13:30</span></div><div id="timer">0:00</div><div id="pos"></div>
        <div style="margin-top:12px"><button id="b-prev">← Prev</button><button id="b-next">Next →</button><button id="b-reset">Reset timer</button></div></div>
    </body></html>`);
    pw.document.close();
    const d = pw.document;
    const fit = () => d.querySelectorAll('.fr').forEach(f => f.querySelector('iframe').style.transform = `scale(${f.clientWidth / 1600})`);
    pw.addEventListener('resize', fit); setTimeout(fit, 50);
    d.getElementById('b-prev').onclick = () => prev(); d.getElementById('b-next').onclick = () => next();
    d.getElementById('b-reset').onclick = () => { t0 = Date.now(); };
    pw.addEventListener('keydown', e => { if (['ArrowRight', ' ', 'PageDown'].includes(e.key)) { e.preventDefault(); next(); } else if (['ArrowLeft', 'PageUp'].includes(e.key)) { e.preventDefault(); prev(); } else if (e.key === 'r' || e.key === 'R') t0 = Date.now(); });
    const tick = setInterval(() => { if (pw.closed) return clearInterval(tick); const s = Math.floor((Date.now() - t0) / 1000);
      const el = d.getElementById('timer'); el.textContent = Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0'); el.style.color = s > 810 ? '#EF8C8C' : '#F2F1EC'; }, 500);
    pw._base = base; syncPresenter();
  }
  function syncPresenter() {
    if (!pw || pw.closed) return;
    const d = pw.document, s = slides[cur], nx = slides[cur + 1];
    const setSrc = (el, id) => { const u = pw._base + '?preview=' + id + '#' + id; if (el.dataset.id !== id) { el.dataset.id = id; el.src = u; } };
    setSrc(d.getElementById('cur'), s.id);
    if (nx) setSrc(d.getElementById('nxt'), nx.id);
    const n = s.querySelector('aside.notes'); d.getElementById('notes').innerHTML = n ? n.innerHTML : '';
    d.getElementById('pos').textContent = s.classList.contains('apx') ? 'Appendix ' + s.dataset.label : `Slide ${s.dataset.n} of ${main.length}`;
    d.getElementById('step').textContent = groupCount(s) ? `step ${s.dataset.step || 0} / ${groupCount(s)}` : '';
  }

  // ---------- control bar (navy pill, as the delivery deck): prev, count, next, slides, Q&A, notes, presenter, full screen ----------
  const ic = d => `<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${d}</svg>`;
  const bar = document.createElement('div'); bar.id = 'bar';
  bar.innerHTML = `<button data-b="prev" title="Back">${ic('<path d="M15 18l-6-6 6-6"/>')}</button><span class="count"></span><button data-b="next" title="Next">${ic('<path d="M9 18l6-6-6-6"/>')}</button><span class="sep"></span>`
    + `<button data-b="ov">${ic('<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>')}Slides</button>`
    + `<button data-b="qa">${ic('<circle cx="12" cy="12" r="9"/><path d="M9.5 9a2.5 2.5 0 1 1 3.5 2.3c-.6.3-1 .9-1 1.6V14"/><path d="M12 17h.01"/>')}Q&amp;A</button>`
    + `<button data-b="notes">${ic('<path d="M4 4h16v16H4z"/><path d="M8 9h8M8 13h8M8 17h5"/>')}Notes</button>`
    + `<button data-b="pres">${ic('<rect x="2" y="4" width="20" height="13" rx="2"/><path d="M8 21h8M12 17v4"/>')}Presenter</button>`
    + `<button data-b="fs" title="Full screen">${ic('<path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/>')}</button>`;
  document.body.appendChild(bar);
  bar.addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; e.stopPropagation();
    ({ prev, next, ov: () => ov.classList.contains('open') ? ov.classList.remove('open') : openOverview(),
       qa: () => { if (!slides[cur].classList.contains('apx')) backStack.push(slides[cur].id); go(slides.indexOf(byId.a0), true); },
       notes: () => notes.classList.toggle('open'), pres: openPresenter,
       fs: () => document.fullscreenElement ? document.exitFullscreen() : document.documentElement.requestFullscreen() })[b.dataset.b](); });
  function syncBar() { const s = slides[cur]; bar.querySelector('.count').textContent = s.classList.contains('apx') ? s.dataset.label : `${s.dataset.n} / ${main.length}`; }
  let idleT; const wake = () => { bar.classList.remove('idle'); clearTimeout(idleT); idleT = setTimeout(() => bar.classList.add('idle'), 2500); };
  addEventListener('mousemove', wake); wake();

  // ---------- start ----------
  const pv = /[?&]preview=([\w-]+)/.exec(location.search);
  if (pv && byId[pv[1]]) document.documentElement.setAttribute('data-preview', '1');
  slides.forEach(s => s.querySelectorAll('svg').forEach(v => v.pauseAnimations && v.pauseAnimations()));
  const startId = pv ? pv[1] : location.hash.slice(1);
  const start = slides.findIndex(s => s.id === startId);
  go(start >= 0 ? start : 0, false, !!pv || (start >= 0 && slides[start].classList.contains('apx')));
  window.deck = { go: id => go(slides.indexOf(byId[id])), next, prev };
})();
