# Free Model Aggregator

One server (`main.ts`, Node 22, no dependencies) that aggregates ten free
model providers behind **both** the OpenAI Chat Completions API and the
Anthropic Messages API — JSON and streaming (SSE) — for the make.fun
competition `free-model-aggregator-01`.

## What it serves

- `POST /v1/chat/completions` — OpenAI Chat Completions, JSON or SSE
- `POST /v1/messages` — Anthropic Messages, JSON or SSE
- `GET /v1/models` — the free models it can serve; the wire shape follows the
  caller's auth header (`Authorization: Bearer …` → OpenAI list shape,
  `x-api-key: …` → Anthropic list shape)

## How it works

At trial start the server reads the brief in `MAKEFUN_PROMPT_FILE`:

- **providers/v1** — the ten providers: id, wire (`openai` / `anthropic` /
  `gemini`), auth scheme, base path on the env, key env var and key;
- **ratings/v1** — the sealed quality table: rating, context length and
  tools / vision / JSON support for every free model on every provider;
- **workload/v1** — the manifest of the requests it will serve, in arrival
  order, each with its requirements (tools, vision, JSON, minimum context).

It probes each provider's own model list (probes are free — they never count
against a provider's budget), builds the catalog document, registers
`{protocol_version: 1, port, catalog}` with `POST $MAKEFUN_ENV_URL/register`,
serves the workload, polls `GET $MAKEFUN_ENV_URL/work` and exits 0 when the
env says the trial is done.

### Routing

Each incoming request is translated once into a wire-neutral form that keeps
exactly the fields every wire reads back — system prompt, last user message,
tool names, `max_tokens`, stop sequences, images, JSON mode — so the chosen
provider's answer echoes the client's original request on any wire.

The brief names every request in arrival order, so the router plans over
the whole workload instead of grabbing greedily. For each request it builds
the plan's candidate list — every free (provider, model) pair whose
quality-table row meets the request's requirements (a JSON-mode request is
never planned onto an Anthropic-wire provider, because that wire cannot
carry the JSON-mode flag) — and prices every attempt two ways:

- the **refusal risk** of the provider's next call: a provider's first call
  is the safest in the trial, its second the most dangerous single step
  (failure schedules cluster on low call numbers), and late calls add
  budget risk, since a provider's free-tier budget is never published;
- the **weighted rating** the choice gives up against the request's best
  legal model.

A candidate's price is its own attempt price plus a greedy rollout of
every later request over the call counts that choice would leave behind,
so an early, flexible request does not burn the one provider a later,
narrower request depends on, and deep use of one provider is only bought
when the rating is worth the rising risk. On a refusal the router replans
onto another provider — never an immediate retry of the same one, since
scheduled refusals cluster on neighbouring call numbers and a spent budget
or an outage refuses every retry by definition; a provider whose budget is
spent or that is in outage is marked dead for the rest of the trial.

Every choice is a pure function of the brief, the requests and the answers
the providers give — no wall clock, no randomness — so a replayed trial
leaves the same state.

### The catalog

```sh
node main.ts --list
```

prints the catalog document exactly as the server registers it. Run outside
a trial (no `MAKEFUN_PROMPT_FILE`, no `MAKEFUN_ENV_URL`) it prints
`{"providers":[]}` and exits 0.

## Running a trial locally

The server needs the trial environment the kit provides (it serves the
simulated providers and the client workload):

```sh
MAKEFUN_ENV_URL=http://127.0.0.1:8080 \
MAKEFUN_PROMPT_FILE=/path/to/trial/input/prompt.txt \
node main.ts
```

## License

MIT — see `LICENSE`.
