"""The streaming core: provider fallback, two deadlines, budget reserve/settle.

Keep `Event` and `explain_stream` exactly as named; the public and hidden tests import them."""
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
import asyncio
import contextlib
import time

from app.budget import DailyBudget, actual_cost_usd, estimate_cost_usd
from app.providers import LLMProvider, ProviderError


@dataclass(frozen=True)
class Event:
    kind: str                       # "meta" | "token" | "done" | "error"
    data: dict = field(default_factory=dict)


async def explain_stream(prompt: str, providers: Sequence[LLMProvider], *, tenant: str,
                         budget: DailyBudget, ttft_timeout_s: float, total_timeout_s: float,
                         max_output_tokens: int) -> AsyncIterator[Event]:
    """Yield Event("meta", ...), Event("token", ...)* and a final Event("done" | "error", ...).

    Rules, in order (see README.md for the full statement):
    - Try providers in the given order. Before each, reserve its estimate; skip an
      unaffordable provider without calling it.
    - Wait at most ttft_timeout_s (never past the overall deadline) for the first token.
      On timeout, ProviderError, or an empty stream before the first token: settle the
      reservation at 0 and try the next provider.
    - On the first token, emit "meta" {"provider": name}, then "token" {"text": ...} per token.
    - After the first token, never switch provider: a ProviderError now is a fatal
      "error" {"code": "upstream_interrupted", "partial": True}.
    - The whole call never runs past total_timeout_s from the start; past the deadline, emit
      "error" {"code": "deadline_exceeded", "partial": <any token sent yet>}.
    - On success, settle at the actual cost and emit
      "done" {"provider", "output_tokens", "cost_usd"}.
    - Every affordable provider failed before its first token: "error" {"code": "all_providers_failed"}.
      No provider was affordable: "error" {"code": "budget_exhausted"}.
    - Whatever happens, including the caller closing this generator early, close the
      provider's stream (aclose()) and settle its reservation at the actual cost of the
      tokens it produced.
    """
    start = time.monotonic()
    deadline = start + total_timeout_s
    affordable_attempted = False

    for provider in providers:
        reservation = estimate_cost_usd(provider, prompt, max_output_tokens)
        if not budget.try_reserve(tenant, reservation):
            continue

        affordable_attempted = True
        stream = provider.stream(prompt, max_output_tokens)
        output_tokens = 0
        sent_any = False
        settled = False
        outcome: str | None = None
        interrupted = False
        deadline_exceeded = False

        async def close_and_settle() -> None:
            nonlocal settled
            if settled:
                return
            async with contextlib.aclosing(stream):
                pass
            cost = actual_cost_usd(provider, prompt, output_tokens)
            budget.settle(tenant, reservation, cost)
            settled = True

        try:
            now = time.monotonic()
            if now >= deadline:
                outcome = "deadline"
                deadline_exceeded = True
            else:
                first_timeout = min(ttft_timeout_s, deadline - now)
                try:
                    async with asyncio.timeout(first_timeout):
                        first = await anext(stream)
                except TimeoutError:
                    outcome = "fallback"
                except ProviderError:
                    outcome = "fallback"
                except StopAsyncIteration:
                    outcome = "fallback"
                except Exception:
                    outcome = "fallback"
                else:
                    sent_any = True
                    output_tokens = 1
                    yield Event("meta", {"provider": provider.name})
                    yield Event("token", {"text": first})

                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            outcome = "deadline"
                            deadline_exceeded = True
                            break
                        try:
                            async with asyncio.timeout(remaining):
                                token = await anext(stream)
                        except TimeoutError:
                            outcome = "deadline"
                            deadline_exceeded = True
                            break
                        except ProviderError:
                            outcome = "interrupted"
                            interrupted = True
                            break
                        except StopAsyncIteration:
                            outcome = "success"
                            break
                        except Exception:
                            outcome = "interrupted"
                            interrupted = True
                            break
                        else:
                            output_tokens += 1
                            sent_any = True
                            yield Event("token", {"text": token})
        finally:
            if not settled:
                await close_and_settle()

        if outcome == "deadline" or deadline_exceeded:
            yield Event("error", {
                "code": "deadline_exceeded",
                "partial": sent_any,
            })
            return
        if outcome == "interrupted" or interrupted:
            yield Event("error", {
                "code": "upstream_interrupted",
                "partial": True,
            })
            return
        if outcome == "success":
            cost = actual_cost_usd(provider, prompt, output_tokens)
            if not settled:
                await close_and_settle()
            yield Event("done", {
                "provider": provider.name,
                "output_tokens": output_tokens,
                "cost_usd": cost,
            })
            return

    if affordable_attempted:
        yield Event("error", {"code": "all_providers_failed"})
    else:
        yield Event("error", {"code": "budget_exhausted"})
