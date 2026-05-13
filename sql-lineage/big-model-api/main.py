import json
import os
import re

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from openai import OpenAI
from pydantic import BaseModel

load_dotenv()

api_key = os.getenv("DEEPSEEK_API_KEY")
if not api_key:
    raise RuntimeError(
        "DEEPSEEK_API_KEY not set. Copy .env.example to .env and fill in your API key."
    )

client = OpenAI(
    api_key=api_key,
    base_url="https://api.deepseek.com",
)

app = FastAPI(title="SQL Lineage API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class SqlRequest(BaseModel):
    sql: str


class LineageResponse(BaseModel):
    thinking: str
    lineage: dict


SYSTEM_PROMPT = """\
你是一个SQL血缘分析专家，你将根据输入的SQL，分析出血缘关系，并且以如下JSON格式返回，只返回JSON，不要包含其他内容：

{
  "currentTable": "目标表名",
  "lineageMappings": [
    {
      "currentField": "目标表字段名1",
      "upstreamTable": "上游表名1",
      "upstreamFields": "上游字段名1"
    },
    {
      "currentField": "目标表字段名2",
      "upstreamTable": "上游表名2",
      "upstreamFields": "上游字段名2"
    }
  ]
}

注意：如果一个字段由多个上游表字段计算而来，需要分开写成多条记录，每条记录对应一个上游字段。
"""


def _extract_json(text: str) -> str:
    """Remove markdown code fences if present."""
    text = text.strip()
    match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    if match:
        return match.group(1)
    return text


@app.post("/sql-lineage", response_model=LineageResponse)
def sql_lineage(req: SqlRequest) -> LineageResponse:
    response = client.chat.completions.create(
        model="deepseek-v4-pro",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": req.sql},
        ],
        stream=False,
        extra_body={"thinking": {"type": "enabled"}},
    )
    msg = response.choices[0].message
    thinking = getattr(msg, "reasoning_content", "") or ""
    lineage = json.loads(_extract_json(msg.content or ""))
    return LineageResponse(thinking=thinking, lineage=lineage)


@app.post("/sql-lineage/stream")
def sql_lineage_stream(req: SqlRequest):
    def generate():
        yield "event: start\ndata: {}\n\n"

        response = client.chat.completions.create(
            model="deepseek-v4-pro",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": req.sql},
            ],
            stream=True,
            extra_body={"thinking": {"type": "enabled"}},
        )
        content_chunks = []
        for chunk in response:
            delta = chunk.choices[0].delta if chunk.choices else None
            if delta is None:
                continue
            rc = getattr(delta, "reasoning_content", None) or ""
            ct = getattr(delta, "content", None) or ""
            if rc:
                yield f"event: thinking\ndata: {json.dumps({'text': rc}, ensure_ascii=False)}\n\n"
            if ct:
                content_chunks.append(ct)

        full = "".join(content_chunks)
        yield f"event: result\ndata: {json.dumps({'lineage': json.loads(_extract_json(full))}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
