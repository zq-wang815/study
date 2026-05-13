const Graph = (() => {

  let expanded = false;
  let currentLineage = null;

  const ROW_H = 22;
  const HEADER_H = 28;
  const FIELD_PAD = 8;

  function buildLayout(lineage) {
    const currentTable = lineage.currentTable || 'result';
    const currentFields = [...new Set(lineage.lineageMappings.map(m => m.currentField))];

    const upstreamMap = new Map();
    for (const m of lineage.lineageMappings) {
      if (!upstreamMap.has(m.upstreamTable)) {
        upstreamMap.set(m.upstreamTable, new Set());
      }
      upstreamMap.get(m.upstreamTable).add(m.upstreamFields);
    }
    // convert Set to array
    for (const [k, v] of upstreamMap) {
      upstreamMap.set(k, [...v]);
    }

    return { currentTable, currentFields, upstreamMap };
  }

  function tableBoxW(rowCount, expanded) {
    return expanded ? 220 : 180;
  }

  function tableBoxH(rowCount, expanded) {
    return expanded ? HEADER_H + rowCount * ROW_H + FIELD_PAD * 2 : HEADER_H + 12;
  }

  function draw(svgEl, lineage) {
    currentLineage = lineage;
    render(svgEl, lineage);
  }

  function render(svgEl, lineage) {
    const svg = d3.select(svgEl);
    svg.selectAll('*').remove();

    const width = svgEl.clientWidth;
    const height = svgEl.clientHeight;

    const { currentTable, currentFields, upstreamMap } = buildLayout(lineage);
    if (currentFields.length === 0 && upstreamMap.size === 0) return;

    const upstreamTables = Array.from(upstreamMap.keys());

    // sizes
    const curW = tableBoxW(currentFields.length, expanded);
    const curH = tableBoxH(currentFields.length, expanded);

    const currentX = width - curW - 50;
    const currentY = Math.max(30, (height - curH) / 2);

    const upstreamX = 50;
    const upBoxes = [];
    let totalH = 0;
    const upData = upstreamTables.map(table => {
      const fields = upstreamMap.get(table);
      const w = tableBoxW(fields.length, expanded);
      const h = tableBoxH(fields.length, expanded);
      return { table, fields, w, h };
    });
    totalH = upData.reduce((s, b) => s + b.h + 16, 0) - 16;

    let uy = Math.max(30, (height - totalH) / 2);
    for (const b of upData) {
      upBoxes.push({ ...b, x: upstreamX, y: uy });
      uy += b.h + 16;
    }

    const g = svg.append('g');
    svg.call(d3.zoom().on('zoom', (e) => g.attr('transform', e.transform)));

    // arrows
    svg.append('defs').append('marker')
        .attr('id', 'arrow')
        .attr('viewBox', '0 -4 8 8')
        .attr('refX', 8).attr('refY', 0)
        .attr('markerWidth', 5).attr('markerHeight', 5)
        .attr('orient', 'auto')
      .append('path')
        .attr('d', 'M0,-4L8,0L0,4')
        .attr('fill', '#999');

    // draw upstream boxes
    const upFieldPositions = new Map(); // "table|field" => {x, y}
    for (const box of upBoxes) {
      // header bar
      g.append('rect')
          .attr('x', box.x).attr('y', box.y)
          .attr('width', box.w).attr('height', HEADER_H)
          .attr('rx', 5)
          .attr('fill', '#4a90d9');

      // body
      g.append('rect')
          .attr('x', box.x).attr('y', box.y + HEADER_H)
          .attr('width', box.w).attr('height', box.h - HEADER_H)
          .attr('rx', 5)
          .attr('fill', '#e8f0fe').attr('stroke', '#4a90d9').attr('stroke-width', 1.2);

      // header text
      g.append('text')
          .attr('x', box.x + box.w / 2).attr('y', box.y + HEADER_H / 2 + 4)
          .attr('text-anchor', 'middle')
          .attr('fill', '#fff').attr('font-size', 12).attr('font-weight', 'bold')
          .text(box.table);

      // fields (if expanded)
      if (expanded) {
        box.fields.forEach((f, i) => {
          const fy = box.y + HEADER_H + FIELD_PAD + i * ROW_H;
          const key = box.table + '|' + f;
          upFieldPositions.set(key, { x: box.x + box.w, y: fy + ROW_H / 2 });

          // separator line between fields
          if (i > 0) {
            g.append('line')
                .attr('x1', box.x + 6).attr('y1', fy)
                .attr('x2', box.x + box.w - 6).attr('y2', fy)
                .attr('stroke', '#d0d8e8').attr('stroke-width', 0.5);
          }

          g.append('circle')
              .attr('cx', box.x + 10).attr('cy', fy + ROW_H / 2)
              .attr('r', 3).attr('fill', '#4a90d9');

          g.append('text')
              .attr('x', box.x + 20).attr('y', fy + ROW_H / 2 + 4)
              .attr('font-size', 11).attr('fill', '#333')
              .text(f);
        });
      }
    }

    // draw current box
    g.append('rect')
        .attr('x', currentX).attr('y', currentY)
        .attr('width', curW).attr('height', HEADER_H)
        .attr('rx', 5)
        .attr('fill', '#4caf50');

    g.append('rect')
        .attr('x', currentX).attr('y', currentY + HEADER_H)
        .attr('width', curW).attr('height', curH - HEADER_H)
        .attr('rx', 5)
        .attr('fill', '#e8f4e8').attr('stroke', '#4caf50').attr('stroke-width', 1.2);

    g.append('text')
        .attr('x', currentX + curW / 2).attr('y', currentY + HEADER_H / 2 + 4)
        .attr('text-anchor', 'middle')
        .attr('fill', '#fff').attr('font-size', 12).attr('font-weight', 'bold')
        .text(currentTable);

    const curFieldPositions = new Map();
    if (expanded) {
      currentFields.forEach((f, i) => {
        const fy = currentY + HEADER_H + FIELD_PAD + i * ROW_H;
        curFieldPositions.set(f, { x: currentX, y: fy + ROW_H / 2 });

        if (i > 0) {
          g.append('line')
              .attr('x1', currentX + 6).attr('y1', fy)
              .attr('x2', currentX + curW - 6).attr('y2', fy)
              .attr('stroke', '#c8dcc8').attr('stroke-width', 0.5);
        }

        g.append('circle')
            .attr('cx', currentX + 10).attr('cy', fy + ROW_H / 2)
            .attr('r', 3).attr('fill', '#4caf50');

        g.append('text')
            .attr('x', currentX + 20).attr('y', fy + ROW_H / 2 + 4)
            .attr('font-size', 11).attr('fill', '#333')
            .text(f);
      });
    }

    // draw arrows
    if (expanded && lineage.lineageMappings.length > 0) {
      for (const m of lineage.lineageMappings) {
        const src = upFieldPositions.get(m.upstreamTable + '|' + m.upstreamFields);
        const tgt = curFieldPositions.get(m.currentField);
        if (!src || !tgt) continue;

        const midX = (src.x + tgt.x) / 2;
        g.append('path')
            .attr('d', `M${src.x},${src.y} C${midX},${src.y} ${midX},${tgt.y} ${tgt.x},${tgt.y}`)
            .attr('stroke', '#aaa').attr('stroke-width', 1)
            .attr('fill', 'none')
            .attr('marker-end', 'url(#arrow)');
      }
    } else if (!expanded && upBoxes.length > 0) {
      // table-level: one arrow per upstream table → current table
      upBoxes.forEach(box => {
        const sy = box.y + box.h / 2;
        const ty = currentY + curH / 2;
        const midX = (box.x + box.w + currentX) / 2;
        g.append('path')
            .attr('d', `M${box.x + box.w},${sy} C${midX},${sy} ${midX},${ty} ${currentX},${ty}`)
            .attr('stroke', '#aaa').attr('stroke-width', 1.5)
            .attr('fill', 'none')
            .attr('marker-end', 'url(#arrow)');
      });
    }
  }

  function toggle() {
    expanded = !expanded;
    const btn = document.getElementById('toggleFieldsBtn');
    btn.textContent = expanded ? '收起字段' : '展开字段';
    const svg = document.getElementById('graph');
    if (currentLineage) render(svg, currentLineage);
  }

  return { draw, toggle };
})();
