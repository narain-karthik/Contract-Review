// Contract Review — Admin full access; department-limited cycles; header + item fields
// + Shared CSV via File System Access API (Open / Reload / Save)
(function(){
  const dept = localStorage.getItem('userDepartment') || localStorage.getItem('userRole');
  const isAdmin = (localStorage.getItem('userIsAdmin') === 'true');
  if (!dept) { window.location.href = 'login.html'; return; }

  let currentFormId = null;

  // ---------- Department cycle ranges (total 64) ----------
  // Engineering 13, Manufacturing 8, Materials 8, Purchase 5,
  // Special Process 3, Welding 3, Assembly 4, Quality 10,
  // Painting 5, Customer Service 4, Commercial 1
  const CR_GROUPS = [
    {key:'engineering',       count:13},
    {key:'manufacturing',     count:8},
    {key:'materials',         count:8},
    {key:'purchase',          count:5},
    {key:'special-process',   count:3},
    {key:'welding',           count:3},
    {key:'assembly',          count:4},
    {key:'quality',           count:10},
    {key:'painting',          count:5},
    {key:'customer-service',  count:4},
    {key:'commercial',        count:1}
  ];

  // ---------- Server-driven allowed departments ----------
  // When backend determines which department(s) should be editable it will return
  // allowedDepartments (array of department keys). We store it here and use it
  // inside enforceCRAccess() to decide whether the current user's department may edit.
  let serverAllowedDepts = null;
  function updateServerAllowedDepartments(list){
    if (Array.isArray(list)) serverAllowedDepts = list.slice();
    else serverAllowedDepts = null;
    enforceCRAccess();
  }
  function refreshDeptAccess(){
    const poKey = getPoKey();
    if (!poKey || poKey === '|||') return;
    fetch('/api/forms/check-departments?formType=CR&poKey=' + encodeURIComponent(poKey), { credentials: 'same-origin' })
      .then(r => r.json())
      .then(js => {
        if (js && js.allowedDepartments) updateServerAllowedDepartments(js.allowedDepartments);
      })
      .catch(() => {/* ignore */});
  }

  function allowedCycleIndexSet(currentDept, reviewColsCount){
    if (isAdmin) return null;
    let start = 0;
    for (const g of CR_GROUPS){
      const end = Math.min(start + g.count - 1, reviewColsCount - 1);
      if (g.key === currentDept) {
        const set = new Set();
        for (let i=start;i<=end;i++) set.add(i);
        return set;
      }
      start += g.count;
    }
    return new Set();
  }

  // ---------- UI references ----------
  const addBtn = document.getElementById('addItemBtn');
  const printBtn = document.getElementById('printBtn');
  const resetBtn = document.getElementById('resetBtn');
  const exportCsvBtn = document.getElementById('exportCsv');
  const exportTxtBtn = document.getElementById('exportTxt');

  const openSharedBtn = document.getElementById('openSharedCsv');
  const reloadSharedBtn = document.getElementById('reloadSharedCsv');
  const saveSharedBtn = document.getElementById('saveSharedCsv');
  const fileNameBadge = document.getElementById('sharedCsvName');

  // ---------- Cycle logic ----------
  let reviewColsCount = 0;
  const DEFAULT_REVIEW_COLS = 64;
  const states = ['', '✓', 'x', 'NA'];

  function computeReviewColsCount() {
    const firstRow = document.querySelector('#tbody tr');
    if (firstRow) {
      const cycles = firstRow.querySelectorAll('td.cycle').length;
      if (cycles > 0) return cycles;
    }
    return DEFAULT_REVIEW_COLS;
  }

  function initCycle(cell) {
    if (!cell || cell._cycleInit) return;
    cell._cycleInit = true;
    cell.addEventListener('click', () => {
      if (cell.dataset.locked === '1') return;
      const current = cell.textContent.trim();
      let idx = states.indexOf(current);
      if (idx === -1) idx = 0;
      const next = states[(idx + 1) % states.length];
      cell.textContent = next;
      cell.classList.remove('state-yes','state-no','state-na');
      if (next === '✓') cell.classList.add('state-yes');
      if (next === 'x') cell.classList.add('state-no');
      if (next === 'NA') cell.classList.add('state-na');
    });
    cell.tabIndex = 0;
    cell.addEventListener('keydown', (ev) => {
      if (cell.dataset.locked === '1') return;
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); cell.click(); }
    });
  }

  function initExisting() {
    document.querySelectorAll('.cycle').forEach(initCycle);
    document.querySelectorAll('.del-row').forEach(btn => {
      if (btn._delInit) return;
      btn._delInit = true;
      btn.addEventListener('click', () => {
        if (!isAdmin) { alert('Only Admin can delete rows.'); return; }
        const tr = btn.closest('tr');
        if (!tr) return;
        if (!confirm('Delete this row?')) return;
        tr.remove();
        scheduleAutoSave();
      });
    });
  }

  function getNextItemNumber() {
    const tbody = document.getElementById('tbody');
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

  function createCellEditable(classesList = '') {
    const td = document.createElement('td');
    td.contentEditable = isAdmin ? "true" : "false";
    td.className = 'editable bordered ' + classesList + (isAdmin ? '' : ' locked-edit');
    td.innerHTML = '';
    return td;
  }

  function createCycleCell() {
    const td = document.createElement('td');
    td.className = 'cycle';
    initCycle(td);
    return td;
  }

  function addRow(itemNo) {
    if (!reviewColsCount) reviewColsCount = computeReviewColsCount();
    const tbody = document.getElementById('tbody');
    const tr = document.createElement('tr');

    // Item No (editable)
    const tdItem = document.createElement('td');
    tdItem.className = 'fixed editable';
    tdItem.contentEditable = "true";
    tdItem.textContent = itemNo;
    tr.appendChild(tdItem);

    // Part / Desc / Rev / Qty
    tr.appendChild(createCellEditable());
    tr.appendChild(createCellEditable());
    const rev = createCellEditable('small'); rev.classList.add('fixed'); tr.appendChild(rev);
    const qty = createCellEditable('small'); qty.classList.add('fixed'); tr.appendChild(qty);

    for (let i=0;i<reviewColsCount;i++){
      tr.appendChild(createCycleCell());
    }

    const remarks = document.createElement('td');
    remarks.contentEditable = isAdmin ? "true" : "false";
    remarks.className = 'editable bordered' + (isAdmin ? '' : ' locked-edit');
    tr.appendChild(remarks);

    const tdAct = document.createElement('td');
    tdAct.className = 'row-actions';
    const del = document.createElement('button');
    del.className = 'del-row';
    del.type = 'button';
    del.textContent = 'Delete';
    tdAct.appendChild(del);
    tr.appendChild(tdAct);

    tbody.appendChild(tr);
    enforceCRAccess();
    markDepartmentSeparators();
  }

  // ---------- Department bold-line separators ----------
  function markDepartmentSeparators() {
    const separatorIndexes = [12, 20, 28, 33, 36, 39, 43, 53, 58, 62, 63];

    document.querySelectorAll('#tbody tr').forEach(tr => {
      const cycles = Array.from(tr.querySelectorAll('td.cycle'));
      cycles.forEach((td, idx) => {
        td.classList.remove('sep-right');
        if (separatorIndexes.includes(idx)) {
          td.classList.add('sep-right');
        }
      });
    });
  }

  // ---------- Locks ----------
  function lockHeaderFields() {
    const ids = ['customerName', 'customerSelect', 'bidDt', 'poRevDt', 'crRevDt', 'recordNo', 'recordDate'];
    const amendmentField = document.getElementById('amendmentDetailsText');

    if (isAdmin) {
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

  function enforceCRAccess(){
    const allowed = allowedCycleIndexSet(dept, reviewColsCount);
    document.querySelectorAll('#tbody tr').forEach(tr => {
      const cycles = Array.from(tr.querySelectorAll('td.cycle'));
      cycles.forEach((td, idx) => {
        // If server has provided allowed departments, only allow edits if current user's dept is one of those
        const deptAllowedByServer = !serverAllowedDepts || serverAllowedDepts.length === 0 || serverAllowedDepts.includes(dept);
        const canEdit = isAdmin || (deptAllowedByServer && allowed && allowed.has(idx));
        if (canEdit) {
          td.dataset.locked = '0';
          td.style.pointerEvents = 'auto';
          td.style.opacity = '1';
          td.classList.remove('locked');
        } else {
          td.dataset.locked = '1';
          td.style.pointerEvents = 'none';
          td.style.opacity = '0.5';
          td.classList.add('locked');
        }
      });
      if (!isAdmin) {
        const tds = Array.from(tr.children);
        for (let i=0;i<tds.length;i++){
          // keep Item No (index 0) editable, lock part/desc/rev/qty (1..4)
          if (i >= 1 && i <= 4) {
            tds[i].contentEditable = "false";
            tds[i].classList.add('locked-edit');
          }
        }
      }
    });
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
        } else {
          field += c;
        }
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

  // ---------- DOM <-> Data ----------
  function trToCRRow(tr){
    const tds = Array.from(tr.children);
    const key = (tds[0]?.innerText || '').trim();
    const part = (tds[1]?.innerText || '').trim();
    const desc = (tds[2]?.innerText || '').trim();
    const rev = (tds[3]?.innerText || '').trim();
    const qty = (tds[4]?.innerText || '').trim();
    const cycles = tds.slice(5, 5+reviewColsCount).map(td => td.innerText.trim());
    const remarks = (tds[5+reviewColsCount]?.innerText || '').trim();
    return { key, part, desc, rev, qty, cycles, remarks };
  }

  function buildCRMapFromDOM(){
    const map = new Map();
    document.querySelectorAll('#tbody tr').forEach(tr=>{
      const r = trToCRRow(tr);
      if (r.key) map.set(r.key, r);
    });
    return map;
  }

  function applyCRDataToDOM(dataRows){
    const tbody = document.getElementById('tbody');
    tbody.innerHTML = '';

    reviewColsCount = DEFAULT_REVIEW_COLS;

    // If no data yet, create default empty rows 10..100
    if (!dataRows || dataRows.length === 0) {
      [10,20,30,40,50,60,70,80,90,100].forEach(num => addRow(num));
      initExisting();
      enforceCRAccess();
      markDepartmentSeparators();
      return;
    }

    dataRows.forEach(r => {
      const tr = document.createElement('tr');

      // Item No (editable)
      const tdItem = document.createElement('td');
      tdItem.className = 'fixed editable';
      tdItem.contentEditable = "true";
      tdItem.innerText = r.key || '';
      tr.appendChild(tdItem);

      const makeEditable = (extra='') => {
        const td = document.createElement('td');
        td.className = 'editable bordered ' + extra;
        td.contentEditable = isAdmin ? "true" : "false";
        if (!isAdmin) td.classList.add('locked-edit');
        return td;
      };

      const tdPart = makeEditable(); tdPart.innerText = r.part || ''; tr.appendChild(tdPart);
      const tdDesc = makeEditable(); tdDesc.innerText = r.desc || ''; tr.appendChild(tdDesc);
      const tdRev  = makeEditable('small fixed'); tdRev.innerText = r.rev || ''; tr.appendChild(tdRev);
      const tdQty  = makeEditable('small fixed'); tdQty.innerText = r.qty || ''; tr.appendChild(tdQty);

      const cycles = r.cycles || [];
      for (let i=0;i<reviewColsCount;i++){
        const td = document.createElement('td');
        td.className = 'cycle';
        const val = cycles[i] || '';
        td.innerText = val;
        td.classList.remove('state-yes','state-no','state-na');
        if (val === '✓') td.classList.add('state-yes');
        else if (val === 'x') td.classList.add('state-no');
        else if (val === 'NA') td.classList.add('state-na');
        initCycle(td);
        tr.appendChild(td);
      }

      const tdRemarks = makeEditable();
      tdRemarks.innerText = r.remarks || '';
      tr.appendChild(tdRemarks);

      const tdAct = document.createElement('td');
      tdAct.className = 'row-actions';
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'del-row';
      btn.textContent = 'Delete';
      tdAct.appendChild(btn);
      tr.appendChild(tdAct);

      tbody.appendChild(tr);
    });

    initExisting();
    enforceCRAccess();
    markDepartmentSeparators();
  }

  // ---------- Shared CSV (FS Access API) ----------
  let crCsvHandle = null;

  async function openSharedCsv(){
    if (!window.showOpenFilePicker){ alert('Your browser does not support the File System Access API. Use Chrome or Edge.'); return; }
    try{
      const [handle] = await window.showOpenFilePicker({
        multiple:false,
        types:[{ description:'CSV', accept:{ 'text/csv':['.csv'] } }]
      });
      crCsvHandle = handle;
      if (fileNameBadge) fileNameBadge.textContent = handle.name || 'Shared CSV';
      await reloadSharedCsv();
    }catch(e){ /* canceled */ }
  }

  function makeCRHeader(){
    const header = ['ItemNo','PartNumber','PartDescription','Rev','Qty'];
    for (let i=1;i<=reviewColsCount;i++) header.push(`Cycle${i}`);
    header.push('Remarks');
    return header;
  }

  function csvRowsToCRObjects(rows){
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
    const cyclesStart = hasHeader ? (Math.max(idxItem, idxPart, idxDesc, idxRev, idxQty) + 1) : 5;
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
      const cycles = [];
      for (let i=0;i<reviewColsCount;i++){
        cycles.push((row[cyclesStart + i] ?? '').trim());
      }
      const remarks = (row[cyclesStart + reviewColsCount] ?? '').trim();
      rowsOut.push({ key, part, desc, rev, qty, cycles, remarks });
    }
    return rowsOut;
  }

  async function reloadSharedCsv(){
    if (!crCsvHandle){ await openSharedCsv(); return; }
    try{
      const file = await crCsvHandle.getFile();
      const text = await file.text();
      const rows = parseCSV(text);
      const objs = csvRowsToCRObjects(rows);
      applyCRDataToDOM(objs);
      enforceCRAccess();
      markDepartmentSeparators();
      alert('Shared CSV reloaded.');
    }catch(e){
      console.error(e);
      alert('Failed to reload CSV.');
    }
  }

  function buildCRObjectsFromDOM(){
    const out = [];
    document.querySelectorAll('#tbody tr').forEach(tr=>{
      out.push(trToCRRow(tr));
    });
    return out;
  }

  function mergeNonAdminCR(existingRows){
    const existingMap = new Map(existingRows.map(r=>[String(r.key), r]));
    const domMap = buildCRMapFromDOM();
    const allowed = allowedCycleIndexSet(dept, reviewColsCount);
    existingMap.forEach((er, key) => {
      const domRow = domMap.get(key);
      if (!domRow) return;
      if (allowed && allowed.size){
        er.cycles = er.cycles || [];
        for (let i=0;i<reviewColsCount;i++){
          if (allowed.has(i)){
            er.cycles[i] = (domRow.cycles || [])[i] ?? '';
          }
        }
      }
    });
    return Array.from(existingMap.values());
  }

  function crObjectsToCsvRows(objs){
    const header = makeCRHeader();
    const rows = [header];
    objs.forEach(r=>{
      const row = [r.key, r.part, r.desc, r.rev, r.qty, ...(r.cycles || [])];
      row.push(r.remarks ?? '');
      rows.push(row);
    });
    return rows;
  }

  async function saveSharedCsv(){
    if (!window.showSaveFilePicker && !crCsvHandle){
      alert('Your browser does not support the File System Access API. Use Chrome or Edge.');
      return;
    }
    if (!crCsvHandle){
      try{
        crCsvHandle = await window.showSaveFilePicker({
          suggestedName:'contract-review-shared.csv',
          types:[{description:'CSV', accept:{'text/csv':['.csv']}}]
        });
        if (fileNameBadge) fileNameBadge.textContent = crCsvHandle.name || 'Shared CSV';
      }catch(e){ return; }
    }

    reviewColsCount = computeReviewColsCount();

    try{
      let outputObjects = [];
      let hasExisting = false;
      try{
        const file = await crCsvHandle.getFile();
        const text = await file.text();
        const existingRows = csvRowsToCRObjects(parseCSV(text));
        if (existingRows.length){ hasExisting = true; }
        if (isAdmin || !hasExisting){
          outputObjects = buildCRObjectsFromDOM();
        } else {
          outputObjects = mergeNonAdminCR(existingRows);
        }
      }catch(e){
        outputObjects = buildCRObjectsFromDOM();
      }

      const rows = crObjectsToCsvRows(outputObjects);
      const writable = await crCsvHandle.createWritable();
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
    const poKey = getPoKey();
    if (!poKey || poKey === '|||') return;

    if (isSaving) return;
    isSaving = true;
    saveStartTime = new Date();

    showSaveIndicator('💾 Saving...');

    const customer = document.getElementById('customerName')?.value || urlCustomer || '';
    const bid = document.getElementById('bidDt')?.value || urlBid || '';
    const po = document.getElementById('poRevDt')?.value || urlPo || '';
    const cr = document.getElementById('crRevDt')?.value || urlCr || '';
    const recordNo = document.getElementById('recordNo')?.value || '';
    const recordDate = document.getElementById('recordDate')?.value || '';
    const amendmentDetails = document.getElementById('amendmentDetailsText')?.value || '';

    const rows = buildCRObjectsFromDOM();

    try {
      const response = await fetch('/api/cr-form/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          poKey: poKey,
          customer: customer,
          bid: bid,
          po: po,
          cr: cr,
          recordNo: recordNo,
          recordDate: recordDate,
          amendmentDetails: amendmentDetails,
          rows: rows
        }),
        credentials: 'same-origin'
      });

      if (response.ok) {
        const data = await response.json();
        lastSaveTime = new Date();

        // update allowed departments if backend returned them
        if (data && data.allowedDepartments) {
          updateServerAllowedDepartments(data.allowedDepartments);
        }

        if (!lastEditTime || lastEditTime <= saveStartTime) {
          isDirty = false;
          showSaveIndicator(`✓ Auto-saved by ${data.lastModifiedBy || 'you'}`);
        } else {
          showSaveIndicator(`✓ Saved - saving new changes...`);
          setTimeout(() => autoSaveForm(), 100);
        }
      } else {
        isDirty = true;
        showSaveIndicator('❌ Error saving - will retry');
        scheduleAutoSave();
      }
    } catch (err) {
      console.error('Auto-save error:', err);
      isDirty = true;
      showSaveIndicator('❌ Error saving - will retry');
      scheduleAutoSave();
    } finally {
      isSaving = false;
    }
  }

  function scheduleAutoSave() {
    isDirty = true;
    lastEditTime = new Date();
    if (autoSaveTimer) clearTimeout(autoSaveTimer);
    autoSaveTimer = setTimeout(autoSaveForm, 2000);
  }

  async function loadSavedForm() {
    const poKey = getPoKey();
    if (!poKey || poKey === '|||') return;

    try {
      const response = await fetch(`/api/cr-form/load?poKey=${encodeURIComponent(poKey)}`, { credentials: 'same-origin' });
      if (response.ok) {
        const data = await response.json();
        if (data.exists) {
          if (data.formId) {
            currentFormId = data.formId;
          }
          if (data.rows && data.rows.length > 0) {
            showSaveIndicator('📥 Loading saved data...');
            applyCRDataToDOM(data.rows);
          }
          if (data.recordNo && document.getElementById('recordNo')) {
            document.getElementById('recordNo').value = data.recordNo;
          }
          if (data.recordDate && document.getElementById('recordDate')) {
            document.getElementById('recordDate').value = data.recordDate;
          }
          if (data.amendmentDetails && document.getElementById('amendmentDetailsText')) {
            document.getElementById('amendmentDetailsText').value = data.amendmentDetails;
          }
          enforceCRAccess();
          markDepartmentSeparators();
          if (data.lastModifiedBy) {
            setTimeout(() => {
              showSaveIndicator(`✓ Loaded (last saved by ${data.lastModifiedBy})`);
            }, 500);
          }
        }
      }
    } catch (err) {
      console.error('Load error:', err);
    }
  }

  async function autoRefreshForm() {
    const poKey = getPoKey();
    if (!poKey || poKey === '|||') return;

    if (isSaving || isDirty) return;

    const now = new Date();
    if (lastEditTime && (now - lastEditTime) < 3000) return;

    try {
      const response = await fetch(`/api/cr-form/load?poKey=${encodeURIComponent(poKey)}`, { credentials: 'same-origin' });
      if (response.ok) {
        const data = await response.json();
        if (data.exists && data.lastModifiedAt) {
          const serverTime = new Date(data.lastModifiedAt);
          if (!lastSaveTime || serverTime > lastSaveTime) {
            applyCRDataToDOM(data.rows || []);
            enforceCRAccess();
            markDepartmentSeparators();
            lastSaveTime = serverTime;
          }
        }
      }
    } catch (err) {
      console.error('Auto-refresh error:', err);
    }
  }

  function startAutoRefresh() {
    if (autoRefreshTimer) clearInterval(autoRefreshTimer);
    autoRefreshTimer = setInterval(autoRefreshForm, 5000);
  }

  function attachAutoSaveListeners() {
    document.querySelectorAll('#tbody tr').forEach(tr => {
      tr.addEventListener('input', scheduleAutoSave);
      tr.addEventListener('click', (e) => {
        if (e.target.classList.contains('cycle')) {
          scheduleAutoSave();
        }
      });
    });

    ['customerName', 'bidDt', 'poRevDt', 'crRevDt', 'recordNo', 'recordDate', 'amendmentDetailsText'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('input', scheduleAutoSave);
    });
  }

  // Auto-fill common fields from URL
  (function(){
    if (urlCustomer && document.getElementById('customerName')) document.getElementById('customerName').value = urlCustomer;
    if (document.getElementById('customerSelect')) {
      const sel = document.getElementById('customerSelect');
      if (urlCustomer) sel.value = urlCustomer;
    }
    if (urlBid && document.getElementById('bidDt')) document.getElementById('bidDt').value = urlBid;
    if (urlPo && document.getElementById('poRevDt')) document.getElementById('poRevDt').value = urlPo;
    if (urlCr && document.getElementById('crRevDt')) document.getElementById('crRevDt').value = urlCr;
  })();

  // ---------- Comments ----------
  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  async function loadComments() {
    if (!currentFormId) return;

    try {
      const response = await fetch(`/api/cr-comments/${currentFormId}`, { credentials: 'same-origin' });
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
                <strong style="color: #333; font-size: 15px;">${escapeHtml(comment.username)}</strong>
                <span style="color: #666; margin-left: 10px; font-size: 13px;">(${escapeHtml(comment.department).toUpperCase()})</span>
              </div>
              <span style="color: #999; font-size: 12px;">${formattedDate}</span>
            </div>
            <div style="color: #555; line-height: 1.6; white-space: pre-wrap;">${escapeHtml(comment.text)}</div>
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
      const response = await fetch(`/api/cr-comments/${currentFormId}`, {
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

  // ---------- Wire buttons ----------
  if (addBtn) addBtn.addEventListener('click', () => {
    if (!isAdmin) { alert('Only Admin can add rows.'); return; }
    addRow(getNextItemNumber());
    setTimeout(() => {
      attachAutoSaveListeners();
      scheduleAutoSave();
    }, 100);
  });
  if (printBtn) printBtn.addEventListener('click', () => window.print());
  if (resetBtn) resetBtn.addEventListener('click', () => {
    if (!confirm('Reset all review cells to blank?')) return;
    document.querySelectorAll('.cycle').forEach(c => {
      if (c.dataset.locked === '1' && !isAdmin) return;
      c.textContent = '';
      c.classList.remove('state-yes','state-no','state-na');
    });
  });
  if (exportCsvBtn) exportCsvBtn.addEventListener('click', () => {
    const table = document.getElementById('review-table');
    let csv = [];
    for (let r=0;r<table.rows.length;r++){
      const row = table.rows[r];
      let cols = [];
      for (let c=0;c<row.cells.length;c++){
        const cell = row.cells[c];
        const btn = cell.querySelector('button');
        let txt = btn ? '' : cell.innerText.trim();
        txt = '"' + txt.replace(/"/g,'""') + '"';
        cols.push(txt);
      }
      csv.push(cols.join(','));
    }
    const blob = new Blob([csv.join('\n')], {type: 'text/csv'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'contract-review-checklist.csv';
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1500);
  });
  if (exportTxtBtn) exportTxtBtn.addEventListener('click', () => {
    const table = document.getElementById('review-table');
    const lines = [];
    for (let r=0;r<table.rows.length;r++){
      const row = table.rows[r];
      const cols = [];
      for (let c=0;c<row.cells.length;c++){
        const cell = row.cells[c];
        const btn = cell.querySelector('button');
        let txt = btn ? '' : cell.innerText;
        txt = txt.replace(/\t/g, ' ').replace(/\r?\n/g, ' ').trim();
        cols.push(txt);
      }
      lines.push(cols.join('\n'));
    }
    const blob = new Blob([lines.join('\n')], { type:'text/plain' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'contract-review-checklist.txt';
    a.click();
    setTimeout(()=>URL.revokeObjectURL(a.href),1500);
  });

  if (openSharedBtn) openSharedBtn.addEventListener('click', openSharedCsv);
  if (reloadSharedBtn) reloadSharedBtn.addEventListener('click', reloadSharedCsv);
  if (saveSharedBtn) saveSharedBtn.addEventListener('click', saveSharedCsv);

  // ---------- Init ----------
  document.addEventListener('DOMContentLoaded', () => {
    reviewColsCount = DEFAULT_REVIEW_COLS;
    lockHeaderFields();
    // default rows (if no data loaded later)
    if (!document.querySelector('#tbody tr')) {
      [10,20,30,40,50,60,70,80,90,100].forEach(num => addRow(num));
    }
    initExisting();
    enforceCRAccess();
    markDepartmentSeparators();
  });

  window.addEventListener('DOMContentLoaded', async () => {
    await loadSavedForm();
    await loadCRSignatures(); // New function to load existing signatures
    attachAutoSaveListeners();
    startAutoRefresh();
    loadComments();
    setInterval(loadComments, 10000);
    // fetch dept access after load and poll periodically
    setTimeout(refreshDeptAccess, 200);
    setInterval(refreshDeptAccess, 10000);
  });

    /* =========================================================
     CR – Signature Dashboard Declaration (NO EXISTING CODE TOUCHED)
     ========================================================= */

  function initCRSignatureDashboard(){
    document.querySelectorAll('.dept-sign-checkbox').forEach(cb => {
      const targetDept = cb.dataset.dept;
      const td = cb.closest('td');
      const status = td.querySelector('.dept-signed-status');

      // Only same department or admin can sign
      if (!isAdmin && dept !== targetDept){
        cb.disabled = true;
        cb.style.opacity = '0.5';
        return;
      }

      cb.addEventListener('change', async () => {
        if (!cb.checked) return;

        const confirmMsg =
          `${targetDept.toUpperCase()} DEPARTMENT DECLARATION\n\n` +
          `I confirm that the ${targetDept} department information in this Contract Review is correct and complete.\n\n` +
          `Once signed, this action cannot be undone.\n\nProceed?`;

        if (!confirm(confirmMsg)){
          cb.checked = false;
          return;
        }

        const poKey = getPoKey();
        if (!poKey || poKey === '|||'){
          alert('PO details are incomplete.');
          cb.checked = false;
          return;
        }

        const res = await fetch('/api/forms/sign-department', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'same-origin',
          body: JSON.stringify({
            formType: 'CR',
            poKey: poKey,
            department: targetDept
          })
        });

        const data = await res.json();
        if (!res.ok){
          alert(data.error || 'Signing failed');
          cb.checked = false;
          return;
        }

        // Lock checkbox permanently
        cb.disabled = true;
        if (status) status.style.display = 'block';

        // Lock department cycles immediately
        document.querySelectorAll('#tbody tr').forEach(tr => {
          const cycles = Array.from(tr.querySelectorAll('td.cycle'));
          const allowed = allowedCycleIndexSet(targetDept, reviewColsCount);
          cycles.forEach((td, idx) => {
            if (allowed && allowed.has(idx)){
              td.dataset.locked = '1';
              td.style.pointerEvents = 'none';
              td.style.opacity = '0.5';
              td.classList.add('locked');
            }
          });
        });

        alert(`${targetDept} department signed successfully.`);
        refreshDeptAccess();
      });
    });
  }

  /* =========================================================
     NEW: Load Existing Signatures
     ========================================================= */
  async function loadCRSignatures(){
    const poKey = getPoKey();
    if (!poKey || poKey === '|||') return;

    try {
      const response = await fetch(`/api/forms/cr-signed-departments?poKey=${encodeURIComponent(poKey)}`, { credentials: 'same-origin' });
      if (!response.ok) return;

      const data = await response.json();
      if (!data.signed || !Array.isArray(data.signed)) return;

      data.signed.forEach(signature => {
        const deptKey = signature.department;
        // Find the checkbox for this department
        const checkbox = document.querySelector(`.dept-sign-checkbox[data-dept="${deptKey}"]`);
        if (checkbox) {
          checkbox.checked = true;
          checkbox.disabled = true;

          const td = checkbox.closest('td');
          const status = td.querySelector('.dept-signed-status');
          if (status) status.style.display = 'block';

          // Optionally update the name displayed if available and desired
          // const nameDiv = td.querySelector('.sig-name');
          // if (nameDiv && signature.signedBy) {
          //   nameDiv.textContent = `${signature.signedBy} (Signed)`;
          // }
        }
      });

      // Re-enforce locks based on loaded signatures (if needed, though refreshDeptAccess handles logic)
      // The signed departments are effectively locked for editing by the server logic usually,
      // but visual locking helps.

    } catch (err) {
      console.error('Error loading signatures:', err);
    }
  }

  // Initialize signature dashboard after page load
  document.addEventListener('DOMContentLoaded', initCRSignatureDashboard);

})();