(() => {
  const grid = document.getElementById('grid');
  const dot = document.getElementById('dot');
  const conn = document.getElementById('conn');
  const count = document.getElementById('count');
  const toast = document.getElementById('toast');
  const termTitle = document.getElementById('term-title');
  const termBody = document.getElementById('term-body');
  const termClear = document.getElementById('term-clear');
  const splitter = document.getElementById('splitter');
  const gridPane = document.getElementById('grid-pane');

  let snapshot = [];
  let highlightIdx = -1;
  let activeSid = null;

  function basename(p) {
    if (!p) return '(no cwd)';
    const parts = p.split('/').filter(Boolean);
    return parts.length ? parts[parts.length - 1] : p;
  }
  function ago(ts) {
    const d = Math.max(0, Math.floor(Date.now() / 1000 - ts));
    if (d < 60) return d + 's';
    if (d < 3600) return Math.floor(d / 60) + 'm';
    return Math.floor(d / 3600) + 'h';
  }
  function esc(s) {
    return String(s).replace(/[&<>"']/g, c => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[c]));
  }
  function tmuxLabel(s) {
    if (!s.tmux_socket) return 'no tmux';
    const w = s.tmux_window ? ':' + s.tmux_window : '';
    const p = s.tmux_pane ? ' ' + s.tmux_pane : '';
    return `${s.tmux_session || '?'}${w}${p}`;
  }

  function render() {
    count.textContent = snapshot.length + (snapshot.length === 1 ? ' session' : ' sessions');
    if (!snapshot.length) {
      grid.innerHTML = '<div class="empty-state">No Claude Code sessions running. Start one with <code>claude</code> in a tmux pane.</div>';
      highlightIdx = -1;
      return;
    }
    if (highlightIdx >= snapshot.length) highlightIdx = snapshot.length - 1;

    grid.innerHTML = snapshot.map((s, i) => {
      const status = s.status || 'ready';
      const hasTmux = !!(s.tmux_socket && s.tmux_session && s.tmux_window && s.tmux_pane);
      const title = (s.display_name && s.display_name.trim()) || basename(s.cwd);
      const cwdSub = s.display_name ? `<span class="cwd-sub" title="${esc(s.cwd)}">${esc(basename(s.cwd))}</span>` : '';
      const summary = (s.summary || '').trim();
      const summaryHtml = summary
        ? `<div class="summary">${esc(summary)}</div>`
        : `<div class="summary empty">(no prompt yet)</div>`;
      let toolInner;
      if (s.current_tool) toolInner = `<span class="tool">▸ ${esc(s.current_tool)}</span>`;
      else if (s.bg_pending) toolInner = `<span class="tool">⌛ ${s.bg_pending} bg${s.bg_label ? ': ' + esc(s.bg_label) : ''}</span>`;
      else if (s.pending_msg) toolInner = `<span>${esc(s.pending_msg)}</span>`;
      else toolInner = '<span></span>';
      const tool = toolInner;
      const err = s.error ? `<div class="summary" style="color:var(--red)">${esc(s.error)}</div>` : '';
      const idxBadge = i < 9 ? `<span class="idx">${i + 1}</span>` : '';
      const hl = i === highlightIdx ? ' highlight' : '';
      const noTmux = hasTmux ? '' : ' no-tmux';
      const active = s.session_id === activeSid ? ' active' : '';
      return `
        <div class="card ${status}${hl}${noTmux}${active}" data-sid="${esc(s.session_id)}" data-tmux="${hasTmux ? '1' : '0'}" data-idx="${i}" title="${esc(s.cwd)}">
          <div class="row">
            <div class="title">${idxBadge}<span>${esc(title)}</span>${cwdSub}</div>
            <span class="pill ${status}">${status.replace('_', ' ')}</span>
          </div>
          ${summaryHtml}
          ${err}
          <div class="meta">
            ${tool}
            <span>${esc(tmuxLabel(s))} · ${ago(s.last_event_at)} ago</span>
          </div>
        </div>`;
    }).join('');
  }

  function flashError(card, msg) {
    if (card) {
      card.classList.add('flash-error');
      setTimeout(() => card.classList.remove('flash-error'), 1500);
    }
    toast.textContent = msg;
    toast.classList.add('show');
    clearTimeout(flashError._t);
    flashError._t = setTimeout(() => toast.classList.remove('show'), 1800);
  }

  /* --- embedded terminal -------------------------------------------- */

  let term = null;
  let fit = null;
  let ws = null;
  let wsReady = false;
  let pendingSid = null;

  function ensureTerm() {
    if (term) return;
    termBody.classList.remove('empty');
    termBody.innerHTML = '';
    term = new Terminal({
      fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
      fontSize: 13,
      theme: { background: '#000000', foreground: '#f0f3f6' },
      cursorBlink: true,
      scrollback: 5000,
      allowProposedApi: true,
    });
    fit = new FitAddon.FitAddon();
    term.loadAddon(fit);
    term.open(termBody);
    requestAnimationFrame(() => { try { fit.fit(); } catch {} });
    term.onData(d => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'input', data: d }));
      }
    });
    window.addEventListener('resize', sendResize);
  }

  function sendResize() {
    if (!term || !fit || !ws || ws.readyState !== WebSocket.OPEN) return;
    try { fit.fit(); } catch { return; }
    const { cols, rows } = term;
    ws.send(JSON.stringify({ type: 'resize', cols, rows }));
  }

  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${proto}://${location.host}/ws/embed`);
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => {
      wsReady = true;
      if (pendingSid) { switchEmbed(pendingSid); pendingSid = null; }
    };
    ws.onmessage = (ev) => {
      if (ev.data instanceof ArrayBuffer) {
        if (term) term.write(new Uint8Array(ev.data));
      } else if (typeof ev.data === 'string') {
        try {
          const ctl = JSON.parse(ev.data);
          if (ctl && ctl.type === 'error') flashError(null, ctl.error || 'embed error');
        } catch {
          if (term) term.write(ev.data);
        }
      }
    };
    ws.onclose = () => {
      wsReady = false;
      if (term) term.writeln('\r\n\x1b[90m[embed disconnected — reconnecting…]\x1b[0m');
      setTimeout(connectWS, 1000);
    };
    ws.onerror = () => { wsReady = false; };
  }

  function switchEmbed(sid) {
    ensureTerm();
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      pendingSid = sid;
      return;
    }
    try { fit.fit(); } catch {}
    const cols = term.cols, rows = term.rows;
    ws.send(JSON.stringify({ type: 'switch', session_id: sid, cols, rows }));
    activeSid = sid;
    const s = snapshot.find(x => x.session_id === sid);
    termTitle.textContent = s ? ((s.display_name && s.display_name.trim()) || basename(s.cwd)) : sid;
    render();
    if (term) term.focus();
  }

  function detachEmbed() {
    if (ws) try { ws.close(); } catch {}
    ws = null;
    wsReady = false;
    if (term) { term.dispose(); term = null; }
    fit = null;
    termBody.innerHTML = 'click a session to attach';
    termBody.classList.add('empty');
    termTitle.textContent = 'no session';
    activeSid = null;
    render();
    setTimeout(connectWS, 100);
  }

  termClear.addEventListener('click', detachEmbed);

  grid.addEventListener('click', (e) => {
    const card = e.target.closest('.card');
    if (!card) return;
    if (card.dataset.tmux !== '1') {
      flashError(card, 'session not in tmux');
      return;
    }
    switchEmbed(card.dataset.sid);
  });

  document.addEventListener('keydown', (e) => {
    if (e.target && /input|textarea/i.test(e.target.tagName)) return;
    if (term && document.activeElement && termBody.contains(document.activeElement)) {
      // typing into terminal — let xterm handle it
      return;
    }
    if (!snapshot.length) return;
    if (e.key >= '1' && e.key <= '9') {
      const i = parseInt(e.key, 10) - 1;
      if (i < snapshot.length) {
        const card = grid.querySelector(`.card[data-idx="${i}"]`);
        if (card && card.dataset.tmux === '1') switchEmbed(card.dataset.sid);
        else if (card) flashError(card, 'session not in tmux');
      }
      return;
    }
    if (e.key === 'j' || e.key === 'ArrowDown') {
      highlightIdx = Math.min(snapshot.length - 1, Math.max(0, highlightIdx + 1));
      render();
      e.preventDefault();
      return;
    }
    if (e.key === 'k' || e.key === 'ArrowUp') {
      highlightIdx = Math.max(0, highlightIdx - 1);
      render();
      e.preventDefault();
      return;
    }
    if (e.key === 'Enter' && highlightIdx >= 0) {
      const card = grid.querySelector(`.card[data-idx="${highlightIdx}"]`);
      if (card && card.dataset.tmux === '1') switchEmbed(card.dataset.sid);
      else if (card) flashError(card, 'session not in tmux');
    }
  });

  /* --- splitter ----------------------------------------------------- */
  let dragging = false;
  splitter.addEventListener('mousedown', (e) => {
    dragging = true;
    splitter.classList.add('dragging');
    document.body.style.cursor = 'col-resize';
    e.preventDefault();
  });
  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const w = Math.max(260, Math.min(window.innerWidth * 0.6, e.clientX));
    gridPane.style.width = w + 'px';
    sendResize();
  });
  document.addEventListener('mouseup', () => {
    if (dragging) {
      dragging = false;
      splitter.classList.remove('dragging');
      document.body.style.cursor = '';
      sendResize();
    }
  });

  /* --- SSE ---------------------------------------------------------- */
  function connectSSE() {
    const es = new EventSource('/events');
    es.addEventListener('snapshot', e => {
      snapshot = JSON.parse(e.data);
      render();
      dot.className = 'dot live';
      conn.textContent = 'live';
    });
    es.addEventListener('ping', () => {
      dot.className = 'dot live';
      conn.textContent = 'live';
    });
    es.onerror = () => {
      dot.className = 'dot dead';
      conn.textContent = 'reconnecting…';
    };
  }

  // init
  termBody.classList.add('empty');
  termBody.innerHTML = 'click a session to attach';
  connectSSE();
  connectWS();
  setInterval(render, 5000);
})();
