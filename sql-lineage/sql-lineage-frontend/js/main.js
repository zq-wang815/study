(function () {
  const sqlInput = document.getElementById('sqlInput');
  const analyzeBtn = document.getElementById('analyzeBtn');
  const errorEl = document.getElementById('error');
  const thinkingBlock = document.getElementById('thinkingBlock');
  const thinkingText = document.getElementById('thinkingText');
  const toggleBtn = document.getElementById('toggleFieldsBtn');
  const svg = document.getElementById('graph');

  sqlInput.value = [
    'SELECT o.order_id, o.order_date,',
    '       c.customer_name, c.city,',
    '       p.product_name, p.unit_price,',
    '       cat.category_name,',
    '       oi.quantity,',
    '       ROUND(o.order_amount * oi.quantity * 0.95, 2) AS actual_revenue,',
    '       CASE WHEN oi.quantity >= 5 THEN \'批发\' ELSE \'零售\' END AS sale_type',
    'FROM orders o',
    'JOIN customers c   ON o.customer_id = c.customer_id',
    'JOIN order_items oi ON o.order_id = oi.order_id',
    'JOIN products p    ON oi.product_id = p.product_id',
    'JOIN categories cat ON p.category_id = cat.category_id',
    'WHERE o.order_date >= \'2024-01-01\'',
  ].join('\n');

  analyzeBtn.addEventListener('click', () => {
    analyzeBtn.disabled = true;
    errorEl.textContent = '';
    thinkingBlock.style.display = 'none';
    thinkingText.textContent = '';
    toggleBtn.style.display = 'none';
    thinkingBlock.open = false;

    API.analyzeStream(
      sqlInput.value.trim(),
      // onStart
      () => {
        thinkingBlock.style.display = 'block';
        thinkingText.textContent = '分析中...';
        thinkingBlock.open = true;
      },
      // onThinking
      (text) => {
        if (thinkingText.textContent === '分析中...') {
          thinkingText.textContent = '';
        }
        thinkingText.textContent += text;
        thinkingText.scrollTop = thinkingText.scrollHeight;
      },
      // onResult
      (lineage) => {
        Graph.draw(svg, lineage || { currentTable: '', lineageMappings: [] });
        toggleBtn.style.display = 'block';
        toggleBtn.textContent = '展开字段';
        analyzeBtn.disabled = false;
      },
      // onError
      (err) => {
        errorEl.textContent = '分析失败: ' + err.message;
        analyzeBtn.disabled = false;
      }
    );
  });
})();
