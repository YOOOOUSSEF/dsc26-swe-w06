"""The HTTP layer around explain_stream.

Keep `StreamSettings` and `create_app` exactly as named; the public and hidden tests import
them."""
from collections.abc import Sequence
from dataclasses import dataclass
import json

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Literal

from app.budget import DailyBudget, estimate_cost_usd
from app.providers import LLMProvider
from app.providers import build_prompt
from app.stream import explain_stream

try:
    from fastapi.sse import EventSourceResponse, ServerSentEvent
except ImportError:
    from fastapi_sse import EventSourceResponse, ServerSentEvent


@dataclass
class StreamSettings:
    ttft_timeout_s: float = 2.0
    total_timeout_s: float = 20.0
    max_output_tokens: int = 400


class ExplainRequest(BaseModel):
    clause_text: str = Field(min_length=1, max_length=8000)
    language: Literal["ar", "en"]


def create_app(providers: Sequence[LLMProvider], budget: DailyBudget,
               settings: StreamSettings | None = None) -> FastAPI:
    """Return a NEW FastAPI app exposing POST /v1/clauses/explain.

    - Header X-Tenant-ID (required, 1-64 chars). Body {"clause_text": 1-8000 chars,
      "language": "ar" | "en"}. Build the prompt with app.providers.build_prompt.
    - Before streaming: invalid input is 422; if the tenant cannot afford even the
      cheapest provider's worst-case estimate, 429 application/problem+json with type
      /problems/budget-exhausted. Providers are not called in either case.
    - Otherwise, a text/event-stream response (use fastapi.sse.EventSourceResponse and
      ServerSentEvent) whose events are explain_stream's events, named meta, token, done
      and error, each event's data as JSON.
    """
    settings = settings or StreamSettings()
    app = FastAPI()

    @app.post("/v1/clauses/explain")
    async def explain(
        body: ExplainRequest,
        x_tenant_id: str = Header(..., alias="X-Tenant-ID", min_length=1, max_length=64),
    ):
        prompt = build_prompt(body.clause_text, body.language)

        if providers:
            cheapest = min(
                estimate_cost_usd(provider, prompt, settings.max_output_tokens)
                for provider in providers
            )
            can_afford = budget.remaining(x_tenant_id) >= cheapest
        else:
            can_afford = False

        if not can_afford:
            return JSONResponse(
                status_code=429,
                media_type="application/problem+json",
                content={
                    "type": "/problems/budget-exhausted",
                    "title": "Budget exhausted",
                    "status": 429,
                },
            )

        async def event_stream():
            async for event in explain_stream(
                prompt,
                providers,
                tenant=x_tenant_id,
                budget=budget,
                ttft_timeout_s=settings.ttft_timeout_s,
                total_timeout_s=settings.total_timeout_s,
                max_output_tokens=settings.max_output_tokens,
            ):
                payload = json.dumps(event.data, ensure_ascii=False)
                sse = ServerSentEvent(event=event.kind, raw_data=payload)
                yield f"event: {sse.event}\ndata: {sse.raw_data}\n\n"

        return EventSourceResponse(event_stream())

    return app
