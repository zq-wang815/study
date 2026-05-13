const API = {
  PYTHON_URL: 'http://localhost:8000',

  analyzeStream(sql, onStart, onThinking, onResult, onError) {
    fetch(`${this.PYTHON_URL}/sql-lineage/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql }),
    }).then(async (res) => {
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status}: ${text}`);
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let eventType = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        // split into lines, keep incomplete line in buffer
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) { eventType = ''; continue; }
          if (trimmed.startsWith('event: ')) {
            eventType = trimmed.slice(7);
          } else if (trimmed.startsWith('data: ')) {
            try {
              const data = JSON.parse(trimmed.slice(6));
              switch (eventType) {
                case 'start': onStart(); break;
                case 'thinking': onThinking(data.text); break;
                case 'result': onResult(data.lineage); break;
              }
            } catch (e) { /* skip bad JSON */ }
            eventType = '';
          }
        }
      }
    }).catch(onError);
  },
};
