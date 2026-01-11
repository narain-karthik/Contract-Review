// PED Review — Admin full access; dept-limited cycles/notes; header + item fields
// + Shared CSV via File System Access API (Open / Reload / Save)
// + Signature Dashboard Logic
(function () {
  const dept = localStorage.getItem('userDepartment') || localStorage.getItem('userRole');
  const isAdmin = (localStorage.getItem('userIsAdmin') === 'true');
  console.log('PED Form Debug - isAdmin:', isAdmin, 'userIsAdmin value:', localStorage.getItem('userIsAdmin'));
  if (!dept) { window.location.href = 'login.html'; return; }

  let currentFormId = null;

  // PED cycles: Engineering 7, Manufacturing 1, Materials 1, Purchase 1 => total 10 cycles
  const PED_GROUPS = [
    {key:'engineering', count:7},
    {key:'manufacturing', count:1},
    {key:'materials', count:1},
    {key:'purchase', count:1}
  ];
  const TOTAL_PED_CYCLES = PED_GROUPS.reduce((s,g)=>s+g.count,0); // 10
  const NOTE_DEPTS = ['special-process','welding','assembly','quality','painting','customer-service','commercial'];

  // ---------- Server-driven allowed departments (PED) ----------
  let serverAllowedDeptsPed = null;
  function updateServerAllowedDepartmentsPed(list){
    if (Array.isArray(list)) serverAllowedDeptsPed = list.slice();
    else serverAllowedDeptsPed = null;
    enforcePEDAccess();
  }

  function refreshDeptAccessPed(){
    // Build poKey from form fields or URL params
    const p = new URLSearchParams(window.location.search);
    const urlCustomer = p.get('customer');
    const urlBid = p.get('bid');
    const urlPo = p.get('po');
    const urlCr = p.get('cr');

    const customerVal = document.getElementById('customerName') ? document.getElementById('customerName').value : (urlCustomer || '');
    const bidVal = document.getElementById('bidDt') ? document.getElementById('bidDt').value : (urlBid || '');
    const poVal = document.getElementById('poRevDt') ? document.getElementById('poRevDt').value : (urlPo || '');
    const crVal = document.getElementById('crRevDt') ? document.getElementById('crRevDt').value : (urlCr || '');

    const poKey = `${customerVal}|${bidVal}|${poVal}|${crVal}`;
    if (!poKey || poKey === '|||') return;

    fetch('/api/forms/check-departments?formType=PED&poKey=' + encodeURIComponent(poKey), { credentials: 'same-origin' })
      .then(r => r.json())
      .then(js => {
        if (js && js.allowedDepartments) updateServerAllowedDepartmentsPed(js.allowedDepartments);
      })
      .catch(() => {/* ignore */});
  }

  function pedAllowedSet(currentDept){
    if (isAdmin) return null;
    let start = 0;
    for (const g of PED_GROUPS){
      const end = start + g.count - 1;
      if (g.key === currentDept) {
        const s = new Set();
        for (let i=start;i<=end;i++) s.add(i);
        return s;
      }
      start += g.count;
    }
    return new Set();
  }

  // ---------- UI refs ----------
  const addPedBtn = document.getElementById('addItemBtnPed');
  const printBtnPed = document.getElementById('printBtnPed');
  const resetBtnPed = document.getElementById('resetBtnPed');
  const exportCsvPedBtn = document.getElementById('exportCsvPed');
  const exportTxtPedBtn = document.getElementById('exportTxtPed');

  // Shared CSV buttons
  const openSharedBtn = document.getElementById('openSharedCsvPed');
  const reloadSharedBtn = document.getElementById('reloadSharedCsvPed');
  const saveSharedBtn = document.getElementById('saveSharedCsvPed');
  const fileNameBadge = document.getElementById('sharedCsvNamePed');

  // ---------- Accessibility helpers ----------
  function makeCycleFocusable(td) {
    td.tabIndex = 0;
    td.setAttribute('role', 'button');
    td.setAttribute('aria-pressed', td.textContent.trim() !== '' ? 'true' : 'false');
  }
  function ensureExistingCellA11y() {
    document.querySelectorAll('#tbody-ped td.cycle').forEach(td => {
      makeCycleFocusable(td);
      const v = td.textContent.trim();
      td.classList.remove('state-yes', 'state-no', 'state-na');
      if (v === '✓') td.classList.add('state-yes');
      else if (v === 'x') td.classList.add('state-no');
      else if (v === 'NA') td.classList.add('state-na');
    });
    document.querySelectorAll('#tbody-ped td.dept-note').forEach(td => {
      if (!td.hasAttribute('tabindex')) td.tabIndex = 0;
      td.setAttribute('role','button');
      if (!td.dataset.toggleIndex) td.dataset.toggleIndex = '3';
      const v = td.textContent.trim();
      td.classList.remove('state-yes', 'state-no', 'state-na');
      if (v === '✓') td.classList.add('state-yes');
      else if (v === 'x') td.classList.add('state-no');
      else if (v === 'NA') td.classList.add('state-na');
      td.setAttribute('aria-pressed', v !== '' ? 'true' : 'false');
    });
  }

  // ---------- Table wiring ----------
  const STATUS_STATES = ['✓', 'x', 'NA', ''];

  function togglePedCycle(td) {
    if (td.dataset.locked === '1') return;
    const pedStates = ['', '✓', 'x', 'NA'];
    const current = td.textContent.trim();
    let idx = pedStates.indexOf(current);
    if (idx === -1) idx = 0;
    const next = pedStates[(idx + 1) % pedStates.length];
    td.textContent = next;
    td.classList.remove('state-yes', 'state-no', 'state-na');
    if (next === '✓') td.classList.add('state-yes');
    if (next === 'x') td.classList.add('state-no');
    if (next === 'NA') td.classList.add('state-na');
    td.setAttribute('aria-pressed', next !== '' ? 'true' : 'false');
  }

  function toggleDeptNoteStatus(td) {
    if (td.dataset.locked === '1') return;
    let idx = parseInt(td.dataset.toggleIndex || (STATUS_STATES.length - 1), 10);
    idx = (idx + 1) % STATUS_STATES.length;
    const next = STATUS_STATES[idx];
    td.innerText = next;
    td.dataset.toggleIndex = String(idx);
    td.setAttribute('aria-pressed', next !== '' ? 'true' : 'false');
    td.classList.remove('state-yes', 'state-no', 'state-na');
    if (next === '✓') td.classList.add('state-yes');
    if (next === 'x') td.classList.add('state-no');
    if (next === 'NA') td.classList.add('state-na');
  }

  function initPedRowDelegation() {
    const tbody = document.getElementById('tbody-ped');
    if (!tbody) return;

    tbody.addEventListener('click', (e) => {
      const td = e.target.closest('td');
      if (!td || !tbody.contains(td)) return;

      if (e.target.matches('.del-row')) {
        if (!isAdmin) { alert('Only Admin can delete rows.'); return; }
        const tr = e.target.closest('tr');
        if (!tr) return;
        if (!confirm('Delete this row?')) return;
        tr.remove();
        scheduleAutoSave();
        return;
      }

      // Single-click toggle for PED cycles (Engineering / Mfg / Materials / Purchase)
      if (td.classList.contains('cycle')) {
        togglePedCycle(td);
        scheduleAutoSave();
        return;
      }

      // Single-click toggle for Dept Notes (Special Process etc.)
      if (td.classList.contains('dept-note')) {
        // allow both status toggle and text editing:
        // - click in empty space to toggle
        // - user can still place caret and type
        // We toggle on single click always (like cycles).
        toggleDeptNoteStatus(td);
        scheduleAutoSave();
        return;
      }
    });

    tbody.addEventListener('keydown', (e) => {
      const t = e.target;
      if (t.classList.contains('cycle')) {
        if (t.dataset.locked === '1') return;
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); togglePedCycle(t); scheduleAutoSave(); }
      }
      if (t.classList.contains('dept-note')) {
        if (t.dataset.locked === '1') return;
        // Keyboard shortcut: Ctrl+Enter to cycle status
        if (e.key === 'Enter' && e.ctrlKey) {
          e.preventDefault();
          toggleDeptNoteStatus(t);
          scheduleAutoSave();
        }
      }
    });
  }

  function createPedEditable(classes = '') {
    const td = document.createElement('td');
    td.contentEditable = isAdmin ? "true" : "false";
    td.className = 'editable bordered ' + (classes || '') + (isAdmin ? '' : ' locked-edit');
    td.innerHTML = '';
    return td;
  }
  function createDeptEditable() {
    const td = document.createElement('td');
    td.contentEditable = isAdmin ? "true" : "false";
    td.className = 'editable bordered dept-note' + (isAdmin ? '' : ' locked-edit');
    td.innerHTML = '';
    td.tabIndex = 0; td.setAttribute('role','button'); td.dataset.toggleIndex='3';
    td.setAttribute('aria-pressed','false');
    return td;
  }
  function createPedCycleCell() {
    const td = document.createElement('td');
    td.className = 'cycle';
    td.innerText = '';
    makeCycleFocusable(td);
    return td;
  }

  function getNextItemNumberForTbody(tbody) {
    const rows = Array.from(tbody.querySelectorAll('tr'));
    if (!rows.length) return 10;
    let max = 0;
    rows.forEach(r => {
      const td = r.querySelector('td.fixed');
      if (!td) return;
      const v = parseInt(td.textContent.trim());
      if (!isNaN(v) && v > max) max = v;
    });
    return (max === 0) ? 10 : max + 10;
  }

  function addPedRow(itemNo) {
    const tbody = document.getElementById('tbody-ped');
    if (!tbody) return;
    const tr = document.createElement('tr');

    const tdItem = document.createElement('td');
    tdItem.className = 'fixed editable';
    tdItem.contentEditable = "true";
    tdItem.textContent = itemNo;
    tr.appendChild(tdItem);

    tr.appendChild(createPedEditable());
    tr.appendChild(createPedEditable());
    const rev = createPedEditable('small'); rev.classList.add('fixed'); tr.appendChild(rev);
    const qty = createPedEditable('small'); qty.classList.add('fixed'); tr.appendChild(qty);

    for (let i=0;i<TOTAL_PED_CYCLES;i++){ tr.appendChild(createPedCycleCell()); } // 10
    for (let i=0;i<7;i++){ tr.appendChild(createDeptEditable()); }

    const remarks = document.createElement('td');
    remarks.contentEditable = isAdmin ? "true" : "false";
    remarks.className = 'editable bordered' + (isAdmin ? '' : ' locked-edit');
    tr.appendChild(remarks);

    const tdAct = document.createElement('td');
    tdAct.className = 'row-actions';
    const del = document.createElement('button'); del.className = 'del-row'; del.type = 'button'; del.textContent = 'Delete';
    tdAct.appendChild(del); tr.appendChild(tdAct);

    tbody.appendChild(tr);
    enforcePEDAccess();
  }

  // ---------- Locks ----------
  function enforcePEDAccess(){
    const tbody = document.getElementById('tbody-ped');
    if (!tbody) return;

    const allowed = pedAllowedSet(dept);
    tbody.querySelectorAll('tr').forEach(tr => {
      const cycles = Array.from(tr.querySelectorAll('td.cycle'));
      cycles.forEach((td, idx) => {
        const deptAllowedByServer = !serverAllowedDeptsPed || serverAllowedDeptsPed.length === 0 || serverAllowedDeptsPed.includes(dept);
        const can = isAdmin || (deptAllowedByServer && allowed && allowed.has(idx));
        if (can) {
          td.dataset.locked = '0';
          td.style.pointerEvents = 'auto';
          td.style.opacity = '1';
          td.classList.remove('locked');
          td.removeAttribute('aria-disabled');
          td.tabIndex = 0;
        } else {
          td.dataset.locked = '1';
          td.style.pointerEvents = 'none';
          td.style.opacity = '0.5';
          td.classList.add('locked');
          td.setAttribute('aria-disabled','true');
          td.tabIndex = -1;
        }
      });

      const notes = Array.from(tr.querySelectorAll('td.dept-note'));
      notes.forEach((td, noteIdx) => {
        const noteDept = NOTE_DEPTS[noteIdx];
        const canNote = isAdmin || (dept === noteDept);
        // Additionally respect serverAllowedDeptsPed: only allow editing dept notes (if mapped) when server allows
        const deptAllowedByServer = !serverAllowedDeptsPed || serverAllowedDeptsPed.length === 0 || serverAllowedDeptsPed.includes(noteDept);
        if (canNote && deptAllowedByServer) {
          td.dataset.locked = '0';
          td.contentEditable = "true";
          td.style.opacity = '1';
          td.removeAttribute('aria-disabled');
          td.tabIndex = 0;
        } else {
          td.dataset.locked = '1';
          td.contentEditable = "false";
          td.style.opacity = '0.6';
          td.setAttribute('aria-disabled','true');
          td.tabIndex = -1;
        }
      });

      if (!isAdmin) {
        const tds = Array.from(tr.children);
        for (let i=0;i<tds.length;i++){
          // keep Item No (0) editable; lock 1..4 (part/desc/rev/qty)
          if (i >= 1 && i <= 4) {
            tds[i].contentEditable = "false";
            tds[i].classList.add('locked-edit');
          }
        }
      }
    });
  }

  function lockHeaderFields() {
    console.log('lockHeaderFields called - isAdmin:', isAdmin);
    const ids = ['customerName', 'customerSelect', 'bidDt', 'poRevDt', 'crRevDt', 'recordNo', 'recordDate'];
    const amendmentField = document.getElementById('amendmentDetailsText');

    if (isAdmin) {
      console.log('Admin user detected - UNLOCKING header fields');
      ids.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
          el.disabled = false;
          el.classList.remove('locked-edit');
          el.style.backgroundColor = '';
          el.style.cursor = '';
        }
      });
      if (amendmentField) {
        amendmentField.readOnly = false;
        amendmentField.classList.remove('locked-edit');
        amendmentField.style.backgroundColor = '';
        amendmentField.style.cursor = '';
      }
      return;
    }

    console.log('Non-admin user - locking header fields');
    ids.forEach(id => {
      const el = document.getElementById(id);
      if (el) { el.disabled = true; el.classList.add('locked-edit'); }
    });
    if (amendmentField) {
      amendmentField.readOnly = true;
      amendmentField.classList.add('locked-edit');
      amendmentField.style.backgroundColor = '#f3f4f6';
      amendmentField.style.cursor = 'not-allowed';
    }
  }

  // ---------- CSV helpers ----------
  function parseCSV(text){
    const rows = [];
    let i=0, field='', row=[], inQuotes=false;
    while (i < text.length){
      const c = text[i];
      if (inQuotes){
        if (c === '"'){
          if (text[i+1] === '"'){ field += '"'; i++; }
          else { inQuotes = false; }
        } else { field += c; }
      } else {
        if (c === '"'){ inQuotes = true; }
        else if (c === ','){ row.push(field); field=''; }
        else if (c === '\n'){ row.push(field); rows.push(row); row=[]; field=''; }
        else if (c === '\r'){ /* ignore */ }
        else { field += c; }
      }
      i++;
    }
    if (field.length || row.length) { row.push(field); rows.push(row); }
    return rows;
  }
  function toCSV(rows){
    return rows.map(cols => cols.map(v=>{
      const s = String(v ?? '');
      return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
    }).join(',')).join('\n');
  }

  function trToPedRow(tr){
    const tds = Array.from(tr.children);
    const key = (tds[0]?.innerText || '').trim();
    const part = (tds[1]?.innerText || '').trim();
    const desc = (tds[2]?.innerText || '').trim();
    const rev = (tds[3]?.innerText || '').trim();
    const qty = (tds[4]?.innerText || '').trim();
    const pedCycles = tds.slice(5, 5 + TOTAL_PED_CYCLES).map(td => td.innerText.trim()); // 10
    const notesStart = 5 + TOTAL_PED_CYCLES; // 15
    const notes = tds.slice(notesStart, notesStart + 7).map(td => td.innerText.trim());    // 7
    const remarks = (tds[notesStart + 7]?.innerText || '').trim();
    return { key, part, desc, rev, qty, pedCycles, notes, remarks };
  }
  function buildPEDMapFromDOM(){
    const map = new Map();
    document.querySelectorAll('#tbody-ped tr').forEach(tr=>{
      const r = trToPedRow(tr);
      if (r.key) map.set(r.key, r);
    });
    return map;
  }

  function buildPEDObjectsFromDOM(){
    const rows = [];
    document.querySelectorAll('#tbody-ped tr').forEach(tr=>{
      rows.push(trToPedRow(tr));
    });
    return rows;
  }

  function applyPEDDataToDOM(dataRows){
    const tbody = document.getElementById('tbody-ped');
    tbody.innerHTML = '';

    // If no rows saved yet, create default rows 10..100
    if (!dataRows || dataRows.length === 0) {
      [10,20,30,40,50,60,70,80,90,100].forEach(n => addPedRow(n));
      ensureExistingCellA11y();
      enforcePEDAccess();
      return;
    }

    dataRows.forEach(r => {
      const tr = document.createElement('tr');

      const tdItem = document.createElement('td');
      tdItem.className = 'fixed editable';
      tdItem.contentEditable = "true";
      tdItem.textContent = r.key || '';
      tr.appendChild(tdItem);

      const partTd = createPedEditable(); partTd.innerText = r.part || ''; tr.appendChild(partTd);
      const descTd = createPedEditable(); descTd.innerText = r.desc || ''; tr.appendChild(descTd);
      const revTd = createPedEditable('small'); revTd.classList.add('fixed'); revTd.innerText = r.rev || ''; tr.appendChild(revTd);
      const qtyTd = createPedEditable('small'); qtyTd.classList.add('fixed'); qtyTd.innerText = r.qty || ''; tr.appendChild(qtyTd);

      const cycles = r.pedCycles || [];
      for (let i=0;i<TOTAL_PED_CYCLES;i++){
        const td = createPedCycleCell();
        const val = cycles[i] || '';
        td.innerText = val;
        td.classList.remove('state-yes','state-no','state-na');
        if (val === '✓') td.classList.add('state-yes');
        else if (val === 'x') td.classList.add('state-no');
        else if (val === 'NA') td.classList.add('state-na');
        tr.appendChild(td);
      }

      const notes = r.notes || [];
      for (let i=0;i<7;i++){
        const td = createDeptEditable();
        const val = notes[i] || '';
        td.innerText = val;
        td.classList.remove('state-yes','state-no','state-na');
        if (val === '✓') td.classList.add('state-yes');
        else if (val === 'x') td.classList.add('state-no');
        else if (val === 'NA') td.classList.add('state-na');
        tr.appendChild(td);
      }

      const remarksTd = createPedEditable();
      remarksTd.innerText = r.remarks || '';
      tr.appendChild(remarksTd);

      const tdAct = document.createElement('td');
      tdAct.className = 'row-actions';
      const del = document.createElement('button'); del.className='del-row'; del.type='button'; del.textContent='Delete';
      tdAct.appendChild(del); tr.appendChild(tdAct);

      tbody.appendChild(tr);
    });

    ensureExistingCellA11y();
    enforcePEDAccess();
  }

  // CSV mapping
  function makePEDHeader(){
    const header = ['ItemNo','PartNumber','PartDescription','Rev','Qty'];
    for (let i=1;i<=TOTAL_PED_CYCLES;i++) header.push(`PED${i}`);
    header.push('Note_SP','Note_Welding','Note_Assembly','Note_Quality','Note_Painting','Note_CS','Note_Commercial','Remarks');
    return header;
  }
  function csvRowsToPEDObjects(rows){
    if (!rows || rows.length===0) return [];
    const first = rows[0].map(c=>c.trim().toLowerCase());
    let startIdx = 0;
    let hasHeader = false;
    if (first.includes('itemno') && first.includes('partnumber')) { hasHeader = true; startIdx = 1; }
    const colIndex = (name) => first.indexOf(name.toLowerCase());
    const idxItem = hasHeader ? colIndex('itemno') : 0;
    const idxPart = hasHeader ? colIndex('partnumber') : 1;
    const idxDesc = hasHeader ? colIndex('partdescription') : 2;
    const idxRev  = hasHeader ? colIndex('rev') : 3;
    const idxQty  = hasHeader ? colIndex('qty') : 4;
    const pedStart = hasHeader ? 5 : 5;
    const noteStart = pedStart + TOTAL_PED_CYCLES;
    const rowsOut = [];
    for (let r=startIdx;r<rows.length;r++){
      const row = rows[r];
      if (!row || row.length===0) continue;
      const key = (row[idxItem] ?? '').trim();
      if (!key) continue;
      const part = (row[idxPart] ?? '').trim();
      const desc = (row[idxDesc] ?? '').trim();
      const rev  = (row[idxRev] ?? '').trim();
      const qty  = (row[idxQty] ?? '').trim();
      const pedCycles = [];
      for (let i=0;i<TOTAL_PED_CYCLES;i++){ pedCycles.push((row[pedStart+i] ?? '').trim()); }
      const notes = [];
      for (let i=0;i<7;i++){ notes.push((row[noteStart+i] ?? '').trim()); }
      const remarks = (row[noteStart+7] ?? '').trim();
      rowsOut.push({ key, part, desc, rev, qty, pedCycles, notes, remarks });
    }
    return rowsOut;
  }
  function pedObjectsToCsvRows(objs){
    const header = makePEDHeader();
    const rows = [header];
    objs.forEach(r=>{
      const row = [r.key, r.part, r.desc, r.rev, r.qty, ...(r.pedCycles || []), ...(r.notes || [])];
      row.push(r.remarks ?? '');
      rows.push(row);
    });
    return rows;
  }

  // Shared CSV state
  let pedCsvHandle = null;

  async function openSharedCsv(){
    if (!window.showOpenFilePicker){ alert('Your browser does not support the File System Access API. Use Chrome or Edge.'); return; }
    try{
      const [handle] = await window.showOpenFilePicker({
        multiple:false,
        types:[{ description:'CSV', accept:{ 'text/csv':['.csv'] } }]
      });
      pedCsvHandle = handle;
      if (fileNameBadge) fileNameBadge.textContent = handle.name || 'Shared CSV';
      await reloadSharedCsv();
    }catch(e){ /* canceled */ }
  }

  async function reloadSharedCsv(){
    if (!pedCsvHandle){ await openSharedCsv(); return; }
    try{
      const file = await pedCsvHandle.getFile();
      const text = await file.text();
      const rows = parseCSV(text);
      const objs = csvRowsToPEDObjects(rows);
      applyPEDDataToDOM(objs);
      enforcePEDAccess();
      ensureExistingCellA11y();
      alert('Shared CSV reloaded.');
    }catch(e){
      console.error(e);
      alert('Failed to reload CSV.');
    }
  }

  function mergeNonAdminPED(existingRows){
    const existingMap = new Map(existingRows.map(r=>[String(r.key), r]));
    const domMap = buildPEDMapFromDOM();
    const allowed = pedAllowedSet(dept);
    const noteIndex = NOTE_DEPTS.indexOf(dept);
    existingMap.forEach((er, key) => {
      const domRow = domMap.get(key);
      if (!domRow) return;
      if (allowed && allowed.size){
        er.pedCycles = er.pedCycles || [];
        for (let i=0;i<TOTAL_PED_CYCLES;i++){
          if (allowed.has(i)){
            er.pedCycles[i] = (domRow.pedCycles || [])[i] ?? '';
          }
        }
      }
      if (noteIndex >= 0){
        er.notes = er.notes || new Array(7).fill('');
        er.notes[noteIndex] = (domRow.notes || [])[noteIndex] ?? '';
      }
    });
    return Array.from(existingMap.values());
  }

  async function saveSharedCsv(){
    if (!window.showSaveFilePicker && !pedCsvHandle){
      alert('Your browser does not support the File System Access API. Use Chrome or Edge.');
      return;
    }
    if (!pedCsvHandle){
      try{
        pedCsvHandle = await window.showSaveFilePicker({
          suggestedName:'ped-review-shared.csv',
          types:[{description:'CSV', accept:{'text/csv':['.csv']}}]
        });
        if (fileNameBadge) fileNameBadge.textContent = pedCsvHandle.name || 'Shared CSV';
      }catch(e){ return; }
    }
    try{
      let outputObjects = [];
      let hasExisting = false;
      try{
        const file = await pedCsvHandle.getFile();
        const text = await file.text();
        const existingRows = csvRowsToPEDObjects(parseCSV(text));
        if (existingRows.length){ hasExisting = true; }
        if (isAdmin || !hasExisting){
          outputObjects = [];
          document.querySelectorAll('#tbody-ped tr').forEach(tr=>{
            outputObjects.push(trToPedRow(tr));
          });
        } else {
          outputObjects = mergeNonAdminPED(existingRows);
        }
      }catch(e){
        outputObjects = [];
        document.querySelectorAll('#tbody-ped tr').forEach(tr=>{
          outputObjects.push(trToPedRow(tr));
        });
      }
      const rows = pedObjectsToCsvRows(outputObjects);
      const writable = await pedCsvHandle.createWritable();
      await writable.write(toCSV(rows));
      await writable.close();
      alert('Shared CSV saved.');
    }catch(e){
      console.error(e);
      alert('Failed to save CSV.');
    }
  }

  // ---------- Auto-save functionality ----------
  let autoSaveTimer = null;
  let autoRefreshTimer = null;
  let isSaving = false;
  let isDirty = false;
  let lastSaveTime = null;
  let lastEditTime = null;
  let saveStartTime = null;

  const p = new URLSearchParams(window.location.search);
  const urlCustomer = p.get('customer');
  const urlBid = p.get('bid');
  const urlPo = p.get('po');
  const urlCr = p.get('cr');

  function getPoKey() {
    const customer = document.getElementById('customerName')?.value || urlCustomer || '';
    const bid = document.getElementById('bidDt')?.value || urlBid || '';
    const po = document.getElementById('poRevDt')?.value || urlPo || '';
    const cr = document.getElementById('crRevDt')?.value || urlCr || '';
    return `${customer}|${bid}|${po}|${cr}`.trim();
  }

  function showSaveIndicator(message) {
    let indicator = document.getElementById('autoSaveIndicator');
    if (!indicator) {
      indicator = document.createElement('div');
      indicator.id = 'autoSaveIndicator';
      indicator.style.cssText = 'position:fixed;top:10px;right:10px;background:#4CAF50;color:white;padding:10px 20px;border-radius:4px;z-index:10000;font-size:14px;box-shadow:0 2px 5px rgba(0,0,0,0.2);';
      document.body.appendChild(indicator);
    }
    indicator.textContent = message;
    indicator.style.display = 'block';

    if (message.includes('✓')) {
      indicator.style.background = '#4CAF50';
      setTimeout(() => { indicator.style.display = 'none'; }, 2000);
    } else if (message.includes('Error') || message.includes('❌')) {
      indicator.style.background = '#f44336';
    } else {
      indicator.style.background = '#2196F3';
    }
  }

  async function autoSaveForm() {
    if (isSaving) return;
    if (!isAdmin) return;  // only admin saves PED via autosave in this design

    const data = buildPEDObjectsFromDOM();
    const poKey = getPoKey();
    if (!poKey || poKey === '|||') {
      showSaveIndicator('Fill Customer and PO to enable auto-save.');
      return;
    }

    isSaving = true;
    saveStartTime = new Date();
    showSaveIndicator('Saving...');

    const payload = {
      poKey: poKey,
      customer: document.getElementById('customerName')?.value || '',
      bid: document.getElementById('bidDt')?.value || '',
      po: document.getElementById('poRevDt')?.value || '',
      cr: document.getElementById('crRevDt')?.value || '',
      recordNo: document.getElementById('recordNo')?.value || '',
      recordDate: document.getElementById('recordDate')?.value || '',
      amendmentDetails: document.getElementById('amendmentDetailsText')?.value || '',
      rows: data
    };

    try {
      const res = await fetch('/api/ped-form/save', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        credentials: 'same-origin',
        body: JSON.stringify(payload)
      });
      if (res.ok) {
        const json = await res.json();
        lastSaveTime = new Date();
        if (json && json.allowedDepartments) {
          updateServerAllowedDepartmentsPed(json.allowedDepartments);
        }
        isDirty = false;
        showSaveIndicator(`✓ Auto-saved by ${json.lastModifiedBy || 'you'}`);
      } else {
        showSaveIndicator('Error saving');
        scheduleAutoSave();
      }
    } catch (err) {
      console.error('autoSaveForm error', err);
      showSaveIndicator('Error saving');
      scheduleAutoSave();
    } finally {
      isSaving = false;
    }
  }

  function scheduleAutoSave() {
    if (!isAdmin) return;
    isDirty = true;
    if (autoSaveTimer) clearTimeout(autoSaveTimer);
    autoSaveTimer = setTimeout(autoSaveForm, 2000);
  }

  async function loadFormData() {
    const customer = document.getElementById('customerName')?.value || '';
    const bid = document.getElementById('bidDt')?.value || '';
    const po = document.getElementById('poRevDt')?.value || '';
    const cr = document.getElementById('crRevDt')?.value || '';

    const tbody = document.getElementById('tbody-ped');
    tbody.innerHTML = '';

    if (!customer || !bid || !po || !cr) {
      // Initial blank context, build default rows 10..100
      [10,20,30,40,50,60,70,80,90,100].forEach(n => addPedRow(n));
      return;
    }

    const poKey = `${customer}|${bid}|${po}|${cr}`;  // use pipes, same as backend

    try {
      const response = await fetch(`/api/ped-form/load?poKey=${encodeURIComponent(poKey)}`, { credentials: 'same-origin' });
      const result = await response.json();

      if (response.ok && result.exists) {
        if (result.formId) {
          currentFormId = result.formId;
        }
        document.getElementById('recordNo').value = result.recordNo || '';
        document.getElementById('recordDate').value = result.recordDate || '';
        if (document.getElementById('amendmentDetailsText')) {
          document.getElementById('amendmentDetailsText').value = result.amendmentDetails || '';
        }

        result.rows.forEach(rowData => {
          const tr = document.createElement('tr');

          const tdItem = document.createElement('td');
          tdItem.className = 'fixed';
          tdItem.textContent = rowData.key;
          if (isAdmin) tdItem.contentEditable = 'true';
          tr.appendChild(tdItem);

          const createInput = (cls, val) => {
            const td = document.createElement('td');
            const inp = document.createElement('div');
            inp.className = cls;
            inp.contentEditable = isAdmin ? "true" : "false";
            inp.innerText = val || '';
            if (!isAdmin) td.classList.add('locked-edit');
            td.appendChild(inp);
            return td;
          };

          const createDateCell = (cls, val) => {
            const td = document.createElement('td');
            td.className = 'date-cell';
            const input = document.createElement('div');
            input.className = cls;
            input.contentEditable = isAdmin ? "true" : "false";
            input.innerText = val || '';
            const span = document.createElement('div');
            span.className = 'date-text';
            td.appendChild(input);
            td.appendChild(span);
            return td;
          };

          tr.appendChild(createInput('cell-input part', rowData.part));
          tr.appendChild(createInput('cell-input desc', rowData.desc));
          tr.appendChild(createInput('cell-input rev', rowData.rev));

          const qtyTd = document.createElement('td');
          const qtyInp = document.createElement('div');
          qtyInp.className = 'cell-input qty';
          qtyInp.contentEditable = isAdmin ? "true" : "false";
          qtyInp.innerText = rowData.qty || '';
          if (!isAdmin) qtyTd.classList.add('locked-edit');
          qtyTd.appendChild(qtyInp);
          tr.appendChild(qtyTd);

          // Render PED Cycles
          const cycles = rowData.pedCycles || [];
          for (let i = 0; i < TOTAL_PED_CYCLES; i++) {
            const td = createPedCycleCell();
            const val = cycles[i] || '';
            td.innerText = val;
            td.classList.remove('state-yes', 'state-no', 'state-na');
            if (val === '✓') td.classList.add('state-yes');
            else if (val === 'x') td.classList.add('state-no');
            else if (val === 'NA') td.classList.add('state-na');
            tr.appendChild(td);
          }

          // Render Notes (7 cols)
          const notes = rowData.notes || [];
          for (let i = 0; i < 7; i++) {
            const td = createDeptEditable();
            const val = notes[i] || '';
            td.innerText = val;
            td.classList.remove('state-yes', 'state-no', 'state-na');
            if (val === '✓') td.classList.add('state-yes');
            else if (val === 'x') td.classList.add('state-no');
            else if (val === 'NA') td.classList.add('state-na');
            tr.appendChild(td);
          }

          // Render Remarks
          tr.appendChild(createInput('cell-input remarks', rowData.remarks));

          const act = document.createElement('td');
          act.className = 'row-actions';
          const del = document.createElement('button');
          del.className = 'del-row';
          del.type = 'button';
          del.textContent = 'Delete';
          act.appendChild(del);
          tr.appendChild(act);

          tbody.appendChild(tr);
          wireRow(tr);
        });

        attachAutoSaveListeners();
        showSaveIndicator(`✓ Loaded - last edited by ${result.lastModifiedBy || 'unknown'}`);
      } else {
        // No existing data: create default rows 10..100
        [10,20,30,40,50,60,70,80,90,100].forEach(n => addPedRow(n));
      }
    } catch (error) {
      console.error('Error loading form:', error);
      // If failure, still show defaults so user can work
      [10,20,30,40,50,60,70,80,90,100].forEach(n => addPedRow(n));
    }
  }

  // wireRow helper was implicit in previous code but needed for proper event attachment
  function wireRow(tr) {
    // Re-apply focusability and events for new cells
    // (Most events are delegated to tbody, but specific cell setups like tabIndex are done here)
    tr.querySelectorAll('.cycle').forEach(td => makeCycleFocusable(td));
    tr.querySelectorAll('.dept-note').forEach(td => {
      if(!td.hasAttribute('tabindex')) td.tabIndex = 0;
      td.setAttribute('role','button');
    });
  }

  async function autoRefreshForm() {
    if (isSaving) return;
    if (isDirty) return;  // do NOT refresh while local edits are unsaved

    const customer = document.getElementById('customerName')?.value || urlCustomer || '';
    const bid = document.getElementById('bidDt')?.value || urlBid || '';
    const po = document.getElementById('poRevDt')?.value || urlPo || '';
    const cr = document.getElementById('crRevDt')?.value || urlCr || '';

    if (!customer || !bid || !po || !cr) return;

    const poKey = `${customer}|${bid}|${po}|${cr}`;

    try {
      const response = await fetch(`/api/ped-form/load?poKey=${encodeURIComponent(poKey)}`, { credentials: 'same-origin' });
      const result = await response.json();
      if (response.ok && result.exists) {
        const focusedElement = document.activeElement;
        const hasFocus = focusedElement && (focusedElement.tagName === 'INPUT' || focusedElement.isContentEditable);

        const tbody = document.getElementById('tbody-ped');
        const currentRows = Array.from(tbody.querySelectorAll('tr'));
        const currentRowCount = currentRows.length;
        const newRowCount = result.rows.length;

        if (!hasFocus) {
          document.getElementById('recordNo').value = result.recordNo || '';
          document.getElementById('recordDate').value = result.recordDate || '';

          // Add rows if needed
          if (newRowCount > currentRowCount) {
            for (let i = currentRowCount; i < newRowCount; i++) {
              addPedRow(result.rows[i].key || '');
            }
          } else if (newRowCount < currentRowCount) {
            for (let i = currentRowCount - 1; i >= newRowCount; i--) {
              currentRows[i].remove();
            }
          }

          // Update data
          const updatedRows = Array.from(tbody.querySelectorAll('tr'));
          result.rows.forEach((rowData, idx) => {
            const tr = updatedRows[idx];
            if (tr && !tr.contains(focusedElement)) {
              // Item No
              tr.querySelector('td.fixed').textContent = rowData.key;

              // Input cells (Part, Desc, Rev, Qty, Remarks)
              const inputs = tr.querySelectorAll('.cell-input');
              // Assuming indices: 0=part, 1=desc, 2=rev, 3=qty, 4=remarks.
              // But createInput creates div with class .cell-input inside a TD.
              // Let's use class names if available, or relative positions.

              // Helper to safely update text
              const safeUpdate = (sel, val) => {
                const el = tr.querySelector(sel);
                if (el) el.innerText = val || '';
              };

              safeUpdate('.part', rowData.part);
              safeUpdate('.desc', rowData.desc);
              safeUpdate('.rev', rowData.rev);
              safeUpdate('.qty', rowData.qty);
              safeUpdate('.remarks', rowData.remarks);

              // Update Cycles
              const cycles = tr.querySelectorAll('.cycle');
              (rowData.pedCycles || []).forEach((val, i) => {
                if (cycles[i]) {
                  cycles[i].innerText = val;
                  cycles[i].classList.remove('state-yes', 'state-no', 'state-na');
                  if (val === '✓') cycles[i].classList.add('state-yes');
                  else if (val === 'x') cycles[i].classList.add('state-no');
                  else if (val === 'NA') cycles[i].classList.add('state-na');
                }
              });

              // Update Dept Notes
              const notes = tr.querySelectorAll('.dept-note');
              (rowData.notes || []).forEach((val, i) => {
                if (notes[i]) {
                  notes[i].innerText = val;
                  notes[i].classList.remove('state-yes', 'state-no', 'state-na');
                  if (val === '✓') notes[i].classList.add('state-yes');
                  else if (val === 'x') notes[i].classList.add('state-no');
                  else if (val === 'NA') notes[i].classList.add('state-na');
                }
              });
            }
          });
        }
      }
    } catch (error) {
      console.error('Auto-refresh error:', error);
    }

    // also refresh server-driven dept access
    refreshDeptAccessPed();
  }

  function attachAutoSaveListeners() {
    const fields = ['customerName', 'bidDt', 'poRevDt', 'crRevDt', 'recordNo', 'recordDate', 'amendmentDetailsText'];
    fields.forEach(id => {
      const el = document.getElementById(id);
      if (el) {
        el.removeEventListener('input', scheduleAutoSave);
        el.addEventListener('input', scheduleAutoSave);
      }
    });
  }

  (function(){
    const p = new URLSearchParams(window.location.search);
    const customer = p.get('customer'); const bid = p.get('bid'); const po = p.get('po'); const cr = p.get('cr');
    if (customer && document.getElementById('customerName')) document.getElementById('customerName').value = customer;
    if (document.getElementById('customerSelect')) {
      const sel = document.getElementById('customerSelect');
      if (customer) sel.value = customer;
    }
    if (bid && document.getElementById('bidDt')) document.getElementById('bidDt').value = bid;
    if (po && document.getElementById('poRevDt')) document.getElementById('poRevDt').value = po;
    if (cr && document.getElementById('crRevDt')) document.getElementById('crRevDt').value = cr;

    // Delegate initRow events
    initPedRowDelegation();

    if (addPedBtn) addPedBtn.addEventListener('click', () => {
      if(!isAdmin) { alert('Only Admin can add rows.'); return; }
      addPedRow(getNextItemNumberForTbody(document.getElementById('tbody-ped')));
      scheduleAutoSave();
    });
    if (resetBtnPed) resetBtnPed.addEventListener('click', () => {
      if(!confirm('Reset all cycles/notes?')) return;
      document.querySelectorAll('#tbody-ped .cycle, #tbody-ped .dept-note').forEach(td => {
        if(td.dataset.locked==='1' && !isAdmin) return;
        td.innerText=''; td.classList.remove('state-yes','state-no','state-na');
      });
      scheduleAutoSave();
    });
    if (printBtnPed) printBtnPed.addEventListener('click', () => window.print());
    if (exportCsvPedBtn) exportCsvPedBtn.addEventListener('click', () => {
      const rows = buildPEDObjectsFromDOM();
      const csv = toCSV(pedObjectsToCsvRows(rows));
      const b = new Blob([csv], {type:'text/csv'});
      const u = URL.createObjectURL(b);
      const a = document.createElement('a'); a.href=u; a.download='ped-review.csv'; a.click();
      setTimeout(()=>URL.revokeObjectURL(u),1500);
    });
    if (exportTxtPedBtn) exportTxtPedBtn.addEventListener('click', () => {
      const rows = buildPEDObjectsFromDOM();
      const lines = rows.map(r => [r.key,r.part,r.desc,r.rev,r.qty,...(r.pedCycles||[]),...(r.notes||[]),r.remarks].join('\t'));
      const b = new Blob([lines.join('\n')], {type:'text/plain'});
      const u = URL.createObjectURL(b);
      const a = document.createElement('a'); a.href=u; a.download='ped-review.txt'; a.click();
      setTimeout(()=>URL.revokeObjectURL(u),1500);
    });

    if (openSharedBtn) openSharedBtn.addEventListener('click', openSharedCsv);
    if (reloadSharedBtn) reloadSharedBtn.addEventListener('click', reloadSharedCsv);
    if (saveSharedBtn) saveSharedBtn.addEventListener('click', saveSharedCsv);

    setTimeout(() => {
      loadFormData();
      attachAutoSaveListeners();
      if (isAdmin) {
        autoRefreshTimer = setInterval(autoRefreshForm, 5000);
      } else {
        // non-admins still poll for dept access changes
        setInterval(refreshDeptAccessPed, 10000);
      }
      // initial dept access check after load
      setTimeout(refreshDeptAccessPed, 300);
    }, 100);
  })();

  async function loadComments() {
    if (!currentFormId) return;

    try {
      const response = await fetch(`/api/ped-comments/${currentFormId}`, { credentials: 'same-origin' });
      if (!response.ok) return;

      const data = await response.json();
      const container = document.getElementById('commentsContainer');
      if (!container) return;

      if (!data.comments || data.comments.length === 0) {
        container.innerHTML = '<p style="color: #999; font-style: italic; padding: 20px; text-align: center;">No comments yet. Be the first to add one!</p>';
        return;
      }

      container.innerHTML = data.comments.map(comment => {
        const date = new Date(comment.createdAt);
        const formattedDate = date.toLocaleString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit'
        });

        return `
          <div style="background: white; padding: 15px; margin-bottom: 15px; border-left: 4px solid #4CAF50; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
              <div>
                <strong style="color: #333; font-size: 15px;">${comment.username}</strong>
                <span style="color: #666; margin-left: 10px; font-size: 13px;">(${comment.department.toUpperCase()})</span>
              </div>
              <span style="color: #999; font-size: 12px;">${formattedDate}</span>
            </div>
            <div style="color: #555; line-height: 1.6; white-space: pre-wrap;">${comment.text}</div>
          </div>
        `;
      }).join('');
    } catch (err) {
      console.error('Error loading comments:', err);
    }
  }

  async function postComment() {
    if (!currentFormId) return;

    const textarea = document.getElementById('newComment');
    if (!textarea) return;

    const commentText = textarea.value.trim();
    if (!commentText) {
      alert('Please enter a comment before submitting.');
      return;
    }

    try {
      const response = await fetch(`/api/ped-comments/${currentFormId}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        credentials: 'same-origin',
        body: JSON.stringify({ comment: commentText })
      });

      if (!response.ok) {
        const error = await response.json();
        alert('Failed to post comment: ' + (error.error || 'Unknown error'));
        return;
      }

      textarea.value = '';
      await loadComments();

      const container = document.getElementById('commentsContainer');
      if (container) {
        container.scrollTop = container.scrollHeight;
      }
    } catch (err) {
      console.error('Error posting comment:', err);
      alert('Failed to post comment. Please try again.');
    }
  }

  const submitCommentBtn = document.getElementById('submitComment');
  if (submitCommentBtn) {
    submitCommentBtn.addEventListener('click', postComment);
  }

  const newCommentTextarea = document.getElementById('newComment');
  if (newCommentTextarea) {
    newCommentTextarea.addEventListener('keydown', (e) => {
      if (e.ctrlKey && e.key === 'Enter') {
        postComment();
      }
    });
  }

  // =========================================================
  // PED Signature Dashboard Functionality
  // =========================================================

  function initPEDSignatureDashboard() {
    // 1. Find all checkboxes with class 'dept-sign-checkbox'
    document.querySelectorAll('.dept-sign-checkbox').forEach(cb => {
      const targetDept = cb.dataset.dept; // e.g. "engineering"
      const td = cb.closest('td');
      const status = td.querySelector('.dept-signed-status');

      // 2. Check access rights: Only users from 'targetDept' or 'isAdmin' can interact
      // Note: 'dept' and 'isAdmin' variables are available from the outer closure
      if (!isAdmin && dept !== targetDept) {
        cb.disabled = true;
        cb.style.opacity = '0.5';
        cb.title = "You are not authorized to sign for this department.";
        // We return early so we don't attach the click listener below
        return;
      }

      // 3. Attach change listener for signing
      cb.addEventListener('change', async () => {
        if (!cb.checked) return; // Ignore unchecking (signatures are permanent)

        // Confirmation Dialog
        const confirmMsg =
          `${targetDept.toUpperCase()} DEPARTMENT DECLARATION\n\n` +
          `I confirm that the ${targetDept} department information in this PED Review is correct and complete.\n\n` +
          `Once signed, this action cannot be undone.\n\nProceed?`;

        if (!confirm(confirmMsg)) {
          cb.checked = false;
          return;
        }

        // Get PO Key
        const customer = document.getElementById('customerName')?.value || '';
        const bid = document.getElementById('bidDt')?.value || '';
        const po = document.getElementById('poRevDt')?.value || '';
        const cr = document.getElementById('crRevDt')?.value || '';
        const poKey = `${customer}|${bid}|${po}|${cr}`;

        if (!poKey || poKey === '|||') {
          alert('PO details are incomplete.');
          cb.checked = false;
          return;
        }

        try {
          // 4. Send signature request to API
          // Note: Reusing the same endpoint logic, just passing formType='PED'
          // Ensure your backend's /api/forms/sign-department handles 'PED' (it should based on previous prompt logic)
          // If the endpoint is strictly CR-only in the backend, you might need to adjust the backend.
          // Assuming backend handles formType='PED' by writing to ped_department_signatures or similar table.
          // *Correction based on your app.py*: The current backend `sign_cr_department` function in `app.py`
          // hardcodes `if form_type.upper() != 'CR': return ... error`.
          // You will need to UPDATE YOUR BACKEND (app.py) to support formType='PED'
          // OR this frontend code will fail.
          // *Assuming backend support exists or will be added*:
          const res = await fetch('/api/forms/sign-department', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({
              formType: 'PED', // Sending PED type
              poKey: poKey,
              department: targetDept
            })
          });

          const data = await res.json();
          if (!res.ok) {
            alert(data.error || 'Signing failed');
            cb.checked = false;
            return;
          }

          // 5. On success: Update UI
          cb.disabled = true; // Lock the checkbox
          if (status) {
            status.style.display = 'block';
            status.innerHTML = `✔ Signed by ${data.signedBy}<br><span style="font-size:10px;color:#666">Just now</span>`;
          }

          // 6. Lock editable fields for this department immediately in the DOM
          lockDepartmentFields(targetDept);

          alert(`${targetDept} department signed successfully.`);
          refreshDeptAccessPed(); // Refresh access rules from server

        } catch (e) {
          console.error('Signature error:', e);
          alert('An error occurred while signing.');
          cb.checked = false;
        }
      });
    });
  }

  // Helper to lock specific department fields after signing
  function lockDepartmentFields(deptName) {
    // Lock Note Cells corresponding to the department
    const noteIndex = NOTE_DEPTS.indexOf(deptName);
    if (noteIndex !== -1) {
      document.querySelectorAll('#tbody-ped tr').forEach(tr => {
        const notes = tr.querySelectorAll('td.dept-note');
        if (notes[noteIndex]) {
          const td = notes[noteIndex];
          td.dataset.locked = '1';
          td.contentEditable = "false";
          td.style.opacity = '0.6';
          td.setAttribute('aria-disabled', 'true');
          td.tabIndex = -1;
        }
      });
    }

    // Lock Cycle Cells if the department is Engineering/Mfg/Materials/Purchase
    const group = PED_GROUPS.find(g => g.key === deptName);
    if (group) {
      // Calculate start index for this group
      let startIdx = 0;
      for (const g of PED_GROUPS) {
        if (g.key === deptName) break;
        startIdx += g.count;
      }
      const endIdx = startIdx + group.count;

      document.querySelectorAll('#tbody-ped tr').forEach(tr => {
        const cycles = tr.querySelectorAll('td.cycle');
        for (let i = startIdx; i < endIdx; i++) {
          if (cycles[i]) {
            cycles[i].dataset.locked = '1';
            cycles[i].style.pointerEvents = 'none';
            cycles[i].style.opacity = '0.5';
            cycles[i].classList.add('locked');
            cycles[i].setAttribute('aria-disabled', 'true');
            cycles[i].tabIndex = -1;
          }
        }
      });
    }
  }

  async function loadPEDSignatures() {
    const customer = document.getElementById('customerName')?.value || '';
    const bid = document.getElementById('bidDt')?.value || '';
    const po = document.getElementById('poRevDt')?.value || '';
    const cr = document.getElementById('crRevDt')?.value || '';
    const poKey = `${customer}|${bid}|${po}|${cr}`;

    if (!customer || !po) return;

    try {
      // Note: You might need to implement this endpoint in app.py if it doesn't exist yet
      // Currently `get_cr_signed_departments` is hardcoded to `cr_department_signatures`.
      // You likely need a new endpoint `get_ped_signed_departments` or modify the existing one to accept formType.
      // Assuming endpoint exists for now:
      const res = await fetch(`/api/forms/ped-signed-departments?poKey=${encodeURIComponent(poKey)}`, { credentials: 'same-origin' });
      if (res.ok) {
        const data = await res.json();
        if (data.signed && Array.isArray(data.signed)) {
          data.signed.forEach(sig => {
            // Find the cell in the signature board corresponding to this department
            // NOTE: Your PED_index.html MUST have `data-dept-sign="..."` attributes on the signature cells (td)
            // for this selector to work.
            const cell = document.querySelector(`td[data-dept-sign="${sig.department}"]`);
            if (cell) {
              const cb = cell.querySelector('.dept-sign-checkbox');
              const status = cell.querySelector('.dept-signed-status');

              if (cb) {
                cb.checked = true;
                cb.disabled = true;
              }
              if (status) {
                status.style.display = 'block';
                status.innerHTML = `✔ Signed by ${sig.signedBy}<br><span style="font-size:10px;color:#666">${new Date(sig.signedAt).toLocaleDateString()}</span>`;
              }

              // Also enforce locking if signatures are found on load
              lockDepartmentFields(sig.department);
            }
          });
        }
      }
    } catch (e) {
      console.error('Error loading signatures', e);
    }
  }

  window.addEventListener('DOMContentLoaded', () => {
    loadComments();
    setInterval(loadComments, 10000);
    // Initialize signature functionality after delay to ensure DOM and Data are ready
    setTimeout(() => {
      initPEDSignatureDashboard();
      loadPEDSignatures();
    }, 1000);
  });
})();