# AI usage

Keep all three headings exactly as they are. If this file is missing or any heading is
missing, the grader stops and gives 0 ("AI_USAGE.md missing or incomplete"). The content
is not scored, but your Track Mentor reads it and the W8 defence can ask about it.

## Tools
I used Copilot in VS Code to implement the budget, streaming fallback, FastAPI endpoint,
and focused validation described in the README.

## What AI got wrong
No incorrect implementation was identified during the initial review. I verified the
requirements directly against the README and public tests before and after implementation.

## In my own words

<!-- Answer without AI, one or two sentences each. -->

1. Why is it safe to switch provider before the first token and not after?
It is safe before the first token because no client-visible output or irreversible response
has begun, while switching afterward would duplicate or split a partial answer.

2. Which checks did you put before the stream starts, and why could they not be error events instead?
The endpoint validates the body and tenant header, and checks whether the cheapest provider
fits the tenant budget before opening an SSE response. These must be normal 422 or 429
responses because HTTP validation and budget rejection occur before streaming can establish
a 200 text/event-stream connection.
