// free-model-aggregator-01 — one server that aggregates the trial's ten free
// providers behind BOTH the OpenAI Chat Completions API and the Anthropic
// Messages API, in JSON and in streaming (SSE).
//
// At trial start the server reads the brief (MAKEFUN_PROMPT_FILE): the ten
// providers (wire, auth, base path, key), a sealed quality table (rating,
// context length, tools/vision/JSON support per model+provider) and the
// workload manifest (each request's requirements, in arrival order). It probes
// every provider's model list, registers its port and catalog with the env
// (MAKEFUN_ENV_URL), then serves the workload: each request is routed to the
// highest-rated free model that meets the request's requirements, failing over
// to the next candidate when a provider refuses (budget spent, scheduled
// refusal, outage). When the env reports done, the server exits 0.
//
// Every choice is a pure function of the brief, the requests and the answers
// the providers give: no wall clock and no randomness in anything sent, so a
// replayed trial leaves the same state.

import { createServer } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

type Json = Record<string, unknown>;

const isRecord = (v: unknown): v is Json => typeof v === 'object' && v !== null && !Array.isArray(v);
const str = (v: unknown): string => (typeof v === 'string' ? v : '');
const numOrNull = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const stringList = (v: unknown): string[] => {
  if (typeof v === 'string') return v === '' ? [] : [v];
  if (Array.isArray(v)) return v.filter((x): x is string => typeof x === 'string');
  return [];
};

// The text of a message content value the way every wire reads it: a string
// as-is, or the text blocks of an array joined with "\n".
function blocksText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content
      .filter(isRecord)
      .filter((b) => b.type === 'text')
      .map((b) => str(b.text))
      .join('\n');
  }
  return '';
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// The brief
// ---------------------------------------------------------------------------

type Wire = 'openai' | 'anthropic' | 'gemini';

interface ProviderSpec {
  id: string;
  wire: Wire;
  auth: string; // 'bearer' | 'x-api-key' | 'x-goog-api-key' | 'none'
  keyEnv: string;
  key: string;
  base: string; // path on the env, e.g. /p/groq
}

interface RatingRow {
  provider: string;
  model: string;
  ratingMilli: number;
  contextLength: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
  free: boolean;
}

interface Requires {
  tools?: boolean;
  vision?: boolean;
  json?: boolean;
  minContext?: number;
}

interface ManifestRow {
  id: string;
  api: string;
  stream: boolean;
  requires: Requires;
}

interface Brief {
  providers: ProviderSpec[];
  ratings: RatingRow[];
  workload: ManifestRow[];
}

function asWire(v: unknown): Wire {
  return v === 'anthropic' || v === 'gemini' ? v : 'openai';
}

function asRequires(v: unknown): Requires {
  const r = isRecord(v) ? v : {};
  const out: Requires = {};
  if (r.tools === true) out.tools = true;
  if (r.vision === true) out.vision = true;
  if (r.json === true) out.json = true;
  const mc = numOrNull(r.min_context);
  if (mc !== null) out.minContext = mc;
  return out;
}

// The brief is prose with three JSON documents embedded; find each by its
// "schema" value (e.g. "free-model-aggregator-01/providers/v1") and read the
// balanced {...} that contains it.
function findJsonDoc(text: string, schema: string): Json {
  const anchor = text.indexOf(schema);
  if (anchor < 0) throw new Error(`brief is missing the ${schema} document`);
  const start = text.lastIndexOf('{', anchor);
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < text.length; i += 1) {
    const ch = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return JSON.parse(text.slice(start, i + 1)) as Json;
    }
  }
  throw new Error(`brief has an unterminated ${schema} document`);
}

function parseBrief(text: string): Brief {
  const providersDoc = findJsonDoc(text, 'providers/v1');
  const ratingsDoc = findJsonDoc(text, 'ratings/v1');
  const workloadDoc = findJsonDoc(text, 'workload/v1');
  const providers: ProviderSpec[] = (Array.isArray(providersDoc.providers) ? providersDoc.providers : [])
    .filter(isRecord)
    .map((p) => ({
      id: str(p.id),
      wire: asWire(p.wire),
      auth: str(p.auth) || 'none',
      keyEnv: str(p.key_env),
      key: str(p.key),
      base: str(p.path),
    }))
    .filter((p) => p.id !== '' && p.base !== '');
  const ratingRows = Array.isArray(ratingsDoc.ratings)
    ? ratingsDoc.ratings
    : Array.isArray(ratingsDoc.rows)
      ? ratingsDoc.rows
      : [];
  const ratings: RatingRow[] = ratingRows
    .filter(isRecord)
    .map((r) => ({
      provider: str(r.provider),
      model: str(r.model),
      ratingMilli: numOrNull(r.rating_milli) ?? 0,
      contextLength: numOrNull(r.context_length),
      tools: typeof r.tools === 'boolean' ? r.tools : null,
      vision: typeof r.vision === 'boolean' ? r.vision : null,
      json: typeof r.json === 'boolean' ? r.json : null,
      free: r.free === true,
    }));
  const manifestRows = Array.isArray(workloadDoc.manifest)
    ? workloadDoc.manifest
    : Array.isArray(workloadDoc.requests)
      ? workloadDoc.requests
      : [];
  const workload: ManifestRow[] = manifestRows
    .filter(isRecord)
    .map((r) => ({
      id: str(r.id),
      api: str(r.api),
      stream: r.stream === true,
      requires: asRequires(r.requires),
    }));
  return { providers, ratings, workload };
}

// ---------------------------------------------------------------------------
// Provider model listings and the catalog document
// ---------------------------------------------------------------------------

interface ListingModel {
  id: string;
  free: boolean;
  contextLength: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');

function joinBase(base: string): string {
  if (base.startsWith('http://') || base.startsWith('https://')) return base.replace(/\/+$/, '');
  return `${ENV_URL}${base.startsWith('/') ? '' : '/'}${base}`;
}

function authHeaders(spec: ProviderSpec): Record<string, string> {
  switch (spec.auth) {
    case 'bearer':
      return { authorization: `Bearer ${spec.key}` };
    case 'x-api-key':
      return { 'x-api-key': spec.key };
    case 'x-goog-api-key':
      return { 'x-goog-api-key': spec.key };
    default:
      return {};
  }
}

function modelsRoute(spec: ProviderSpec): { url: string; headers: Record<string, string> } {
  const base = joinBase(spec.base);
  const headers = authHeaders(spec);
  if (spec.wire === 'anthropic') headers['anthropic-version'] = '2023-06-01';
  const path = spec.wire === 'gemini' ? '/v1beta/models' : '/v1/models';
  return { url: `${base}${path}`, headers };
}

// Read a provider's own model list. Catalog probes are free: the providers do
// not count them against any budget.
async function probeProvider(spec: ProviderSpec): Promise<ListingModel[] | null> {
  try {
    const { url, headers } = modelsRoute(spec);
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(10000) });
    if (!res.ok) return null;
    const body: unknown = await res.json();
    if (!isRecord(body)) return null;
    const items: unknown[] =
      spec.wire === 'gemini'
        ? Array.isArray(body.models)
          ? body.models
          : []
        : Array.isArray(body.data)
          ? body.data
          : [];
    const models: ListingModel[] = [];
    for (const item of items) {
      if (!isRecord(item)) continue;
      let id = spec.wire === 'gemini' ? str(item.name) : str(item.id);
      if (id.startsWith('models/')) id = id.slice('models/'.length);
      if (id === '') continue;
      models.push({
        id,
        free: item.free === true,
        contextLength: numOrNull(item.context_length),
        tools: typeof item.tools === 'boolean' ? item.tools : null,
        vision: typeof item.vision === 'boolean' ? item.vision : null,
        json: typeof item.json === 'boolean' ? item.json : null,
      });
    }
    return models;
  } catch {
    return null;
  }
}

// If a probe fails, fall back to the brief's quality table for that provider
// so the provider still appears in the catalog with its free models.
function fallbackListing(brief: Brief, providerId: string): ListingModel[] {
  return brief.ratings
    .filter((r) => r.provider === providerId && r.free)
    .map((r) => ({
      id: r.model,
      free: true,
      contextLength: r.contextLength,
      tools: r.tools,
      vision: r.vision,
      json: r.json,
    }));
}

interface CatalogProvider {
  id: string;
  base_url: string;
  key_env: string;
  wire: Wire;
  models: { id: string; free: boolean; context_length: number | null }[];
}

interface CatalogDoc {
  providers: CatalogProvider[];
}

function buildCatalog(brief: Brief, listings: Map<string, ListingModel[]>): CatalogDoc {
  return {
    providers: brief.providers.map((p) => {
      const models = listings.get(p.id) ?? fallbackListing(brief, p.id);
      return {
        id: p.id,
        base_url: p.base,
        key_env: p.keyEnv,
        wire: p.wire,
        models: models.map((m) => ({ id: m.id, free: m.free, context_length: m.contextLength })),
      };
    }),
  };
}

async function probeAll(brief: Brief): Promise<Map<string, ListingModel[]>> {
  const listings = new Map<string, ListingModel[]>();
  await Promise.all(
    brief.providers.map(async (spec) => {
      const models = await probeProvider(spec);
      listings.set(spec.id, models ?? fallbackListing(brief, spec.id));
    }),
  );
  return listings;
}

// ---------------------------------------------------------------------------
// Normalized request: the client's request, wire-neutral, keeping exactly the
// fields every wire's parser reads back (system text, last user text, tool
// names, max tokens, stops, images, JSON mode) so any provider's echo of the
// translated request matches the client's original.
// ---------------------------------------------------------------------------

type Part =
  | { kind: 'text'; text: string }
  | { kind: 'image'; mediaType: string; data: string } // base64 image
  | { kind: 'imageUrl'; url: string }
  | { kind: 'toolUse'; id: string; name: string; input: unknown }
  | { kind: 'toolResult'; toolUseId: string; text: string; isError: boolean };

interface NormMessage {
  role: 'user' | 'assistant';
  parts: Part[];
}

interface NormTool {
  name: string;
  description: string;
  parameters: Json;
}

interface NormRequest {
  systemText: string | null; // null when the client sent no system prompt
  messages: NormMessage[];
  tools: NormTool[];
  rawToolChoice: unknown;
  maxTokens: number | null;
  stops: string[];
  jsonMode: boolean; // the client asked for a JSON answer (OpenAI wire only)
  responseFormat: unknown; // the client's raw response_format, for OpenAI targets
  temperature: number | null;
  topP: number | null;
  stream: boolean;
}

function parseDataUrl(url: string): { mediaType: string; data: string } | null {
  const match = /^data:([^;,]+)?(;base64)?,(.*)$/s.exec(url);
  if (!match || match[2] !== ';base64') return null;
  return { mediaType: match[1] || 'application/octet-stream', data: match[3] };
}

// Tool results travel as their own messages on the OpenAI wire but as blocks
// inside a user message on the others. Normalize them into user messages in
// order; trailing tool results (no later user message) are folded into the
// last user message so the "last user message" every parser reads keeps the
// same text on every wire.
class MessageSink {
  messages: NormMessage[] = [];
  private pendingResults: Part[] = [];

  private flushPending(): void {
    if (this.pendingResults.length > 0) {
      this.messages.push({ role: 'user', parts: this.pendingResults });
      this.pendingResults = [];
    }
  }

  toolResult(part: Part): void {
    this.pendingResults.push(part);
  }

  message(role: 'user' | 'assistant', parts: Part[]): void {
    if (role === 'user') this.flushPending();
    this.messages.push({ role, parts });
  }

  finish(): NormMessage[] {
    if (this.pendingResults.length > 0) {
      const lastUser = [...this.messages].reverse().find((m) => m.role === 'user');
      if (lastUser) lastUser.parts.push(...this.pendingResults);
      else this.messages.push({ role: 'user', parts: this.pendingResults });
      this.pendingResults = [];
    }
    return this.messages;
  }
}

function fromOpenAI(body: Json): NormRequest {
  const sink = new MessageSink();
  const systemParts: string[] = [];
  let sawSystem = false;
  const rawMessages = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of rawMessages) {
    if (!isRecord(raw)) continue;
    const role = str(raw.role);
    if (role === 'system') {
      sawSystem = true;
      systemParts.push(blocksText(raw.content));
      continue;
    }
    if (role === 'user') {
      const parts: Part[] = [];
      if (typeof raw.content === 'string') {
        if (raw.content !== '') parts.push({ kind: 'text', text: raw.content });
      } else if (Array.isArray(raw.content)) {
        for (const block of raw.content) {
          if (!isRecord(block)) continue;
          if (block.type === 'text') parts.push({ kind: 'text', text: str(block.text) });
          else if (block.type === 'image_url' && isRecord(block.image_url)) {
            parts.push({ kind: 'imageUrl', url: str(block.image_url.url) });
          }
        }
      }
      sink.message('user', parts);
      continue;
    }
    if (role === 'assistant') {
      const parts: Part[] = [];
      const text = blocksText(raw.content);
      if (text !== '') parts.push({ kind: 'text', text });
      if (Array.isArray(raw.tool_calls)) {
        for (const call of raw.tool_calls) {
          if (!isRecord(call) || !isRecord(call.function)) continue;
          let input: unknown = {};
          try {
            input = JSON.parse(str(call.function.arguments) || '{}');
          } catch {
            input = {};
          }
          parts.push({ kind: 'toolUse', id: str(call.id), name: str(call.function.name), input });
        }
      }
      sink.message('assistant', parts);
      continue;
    }
    if (role === 'tool') {
      sink.toolResult({
        kind: 'toolResult',
        toolUseId: str(raw.tool_call_id),
        text: blocksText(raw.content),
        isError: false,
      });
    }
  }
  const tools: NormTool[] = (Array.isArray(body.tools) ? body.tools : [])
    .filter(isRecord)
    .filter((t) => isRecord(t.function))
    .map((t) => {
      const fn = t.function as Json;
      return {
        name: str(fn.name),
        description: str(fn.description),
        parameters: isRecord(fn.parameters) ? fn.parameters : { type: 'object' },
      };
    })
    .filter((t) => t.name !== '');
  const responseFormat = isRecord(body.response_format) ? body.response_format : null;
  const jsonMode =
    responseFormat !== null && (responseFormat.type === 'json_object' || responseFormat.type === 'json_schema');
  return {
    systemText: sawSystem ? systemParts.join('\n') : null,
    messages: sink.finish(),
    tools,
    rawToolChoice: body.tool_choice,
    maxTokens: numOrNull(body.max_tokens) ?? numOrNull(body.max_completion_tokens),
    stops: stringList(body.stop),
    jsonMode,
    responseFormat,
    temperature: numOrNull(body.temperature),
    topP: numOrNull(body.top_p),
    stream: body.stream === true,
  };
}

function anthropicImagePart(block: Json): Part | null {
  const source = isRecord(block.source) ? block.source : null;
  if (!source) return null;
  if (source.type === 'base64') {
    return { kind: 'image', mediaType: str(source.media_type) || 'application/octet-stream', data: str(source.data) };
  }
  if (source.type === 'url') return { kind: 'imageUrl', url: str(source.url) };
  return null;
}

function fromAnthropic(body: Json): NormRequest {
  const sink = new MessageSink();
  const rawMessages = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of rawMessages) {
    if (!isRecord(raw)) continue;
    const role = raw.role === 'assistant' ? 'assistant' : 'user';
    const parts: Part[] = [];
    if (typeof raw.content === 'string') {
      if (raw.content !== '') parts.push({ kind: 'text', text: raw.content });
    } else if (Array.isArray(raw.content)) {
      for (const block of raw.content) {
        if (!isRecord(block)) continue;
        if (block.type === 'text') parts.push({ kind: 'text', text: str(block.text) });
        else if (block.type === 'image') {
          const part = anthropicImagePart(block);
          if (part) parts.push(part);
        } else if (block.type === 'tool_use') {
          parts.push({ kind: 'toolUse', id: str(block.id), name: str(block.name), input: block.input ?? {} });
        } else if (block.type === 'tool_result') {
          parts.push({
            kind: 'toolResult',
            toolUseId: str(block.tool_use_id),
            text: blocksText(block.content),
            isError: block.is_error === true,
          });
        }
      }
    }
    sink.message(role, parts);
  }
  const tools: NormTool[] = (Array.isArray(body.tools) ? body.tools : [])
    .filter(isRecord)
    .map((t) => ({
      name: str(t.name),
      description: str(t.description),
      parameters: isRecord(t.input_schema) ? t.input_schema : { type: 'object' },
    }))
    .filter((t) => t.name !== '');
  return {
    systemText: body.system === undefined ? null : blocksText(body.system),
    messages: sink.finish(),
    tools,
    rawToolChoice: body.tool_choice,
    maxTokens: numOrNull(body.max_tokens),
    stops: stringList(body.stop_sequences),
    jsonMode: false, // the Anthropic wire carries no JSON-mode flag
    responseFormat: null,
    temperature: numOrNull(body.temperature),
    topP: numOrNull(body.top_p),
    stream: body.stream === true,
  };
}

// ---------------------------------------------------------------------------
// Provider request builders: normalized request -> one provider's wire.
// ---------------------------------------------------------------------------

function openAIToolChoice(raw: unknown): unknown {
  if (typeof raw === 'string') return raw;
  if (isRecord(raw)) {
    if (typeof raw.type === 'string') {
      // Anthropic-shaped tool_choice.
      if (raw.type === 'auto') return 'auto';
      if (raw.type === 'any') return 'required';
      if (raw.type === 'none') return 'none';
      if (raw.type === 'tool') return { type: 'function', function: { name: str(raw.name) } };
    }
    return raw; // already OpenAI-shaped
  }
  return undefined;
}

function anthropicToolChoice(raw: unknown): unknown {
  if (typeof raw === 'string') {
    if (raw === 'auto') return { type: 'auto' };
    if (raw === 'required') return { type: 'any' };
    if (raw === 'none') return { type: 'none' };
    return undefined;
  }
  if (isRecord(raw)) {
    if (raw.type === 'function' && isRecord(raw.function)) return { type: 'tool', name: str(raw.function.name) };
    if (typeof raw.type === 'string') return raw; // already Anthropic-shaped
  }
  return undefined;
}

function toOpenAIBody(norm: NormRequest, model: string): Json {
  const messages: Json[] = [];
  if (norm.systemText !== null) messages.push({ role: 'system', content: norm.systemText });
  for (const msg of norm.messages) {
    if (msg.role === 'assistant') {
      const textParts = msg.parts.filter((p) => p.kind === 'text');
      const toolCalls = msg.parts
        .filter((p) => p.kind === 'toolUse')
        .map((p) => {
          const t = p as Extract<Part, { kind: 'toolUse' }>;
          return {
            id: t.id,
            type: 'function',
            function: { name: t.name, arguments: typeof t.input === 'string' ? t.input : JSON.stringify(t.input ?? {}) },
          };
        });
      const out: Json = {
        role: 'assistant',
        content: textParts.length > 0 ? textParts.map((p) => (p as { text: string }).text).join('\n') : null,
      };
      if (toolCalls.length > 0) out.tool_calls = toolCalls;
      messages.push(out);
      continue;
    }
    // user: tool results become tool messages, the rest one user message.
    for (const part of msg.parts) {
      if (part.kind === 'toolResult') {
        messages.push({ role: 'tool', tool_call_id: part.toolUseId, content: part.text });
      }
    }
    const rest = msg.parts.filter((p) => p.kind !== 'toolResult');
    if (rest.length === 1 && rest[0].kind === 'text') {
      messages.push({ role: 'user', content: (rest[0] as { text: string }).text });
    } else if (rest.length === 0) {
      messages.push({ role: 'user', content: '' });
    } else {
      messages.push({
        role: 'user',
        content: rest.map((part) => {
          if (part.kind === 'text') return { type: 'text', text: part.text };
          if (part.kind === 'image') {
            return { type: 'image_url', image_url: { url: `data:${part.mediaType};base64,${part.data}` } };
          }
          if (part.kind === 'imageUrl') return { type: 'image_url', image_url: { url: part.url } };
          return { type: 'text', text: '' };
        }),
      });
    }
  }
  const body: Json = { model, messages, stream: norm.stream };
  if (norm.stream) body.stream_options = { include_usage: true };
  if (norm.maxTokens !== null) body.max_tokens = norm.maxTokens;
  if (norm.stops.length > 0) body.stop = norm.stops;
  if (norm.jsonMode) body.response_format = norm.responseFormat ?? { type: 'json_object' };
  if (norm.tools.length > 0) {
    body.tools = norm.tools.map((t) => ({
      type: 'function',
      function: { name: t.name, description: t.description, parameters: t.parameters },
    }));
    const choice = openAIToolChoice(norm.rawToolChoice);
    if (choice !== undefined) body.tool_choice = choice;
  }
  if (norm.temperature !== null) body.temperature = norm.temperature;
  if (norm.topP !== null) body.top_p = norm.topP;
  return body;
}

function toAnthropicBody(norm: NormRequest, model: string): Json {
  const messages: Json[] = norm.messages.map((msg) => ({
    role: msg.role,
    content: msg.parts.map((part) => {
      switch (part.kind) {
        case 'text':
          return { type: 'text', text: part.text };
        case 'image':
          return { type: 'image', source: { type: 'base64', media_type: part.mediaType, data: part.data } };
        case 'imageUrl': {
          const data = parseDataUrl(part.url);
          if (data) return { type: 'image', source: { type: 'base64', media_type: data.mediaType, data: data.data } };
          return { type: 'image', source: { type: 'url', url: part.url } };
        }
        case 'toolUse':
          return { type: 'tool_use', id: part.id, name: part.name, input: part.input ?? {} };
        case 'toolResult':
          return {
            type: 'tool_result',
            tool_use_id: part.toolUseId,
            content: part.text,
            ...(part.isError ? { is_error: true } : {}),
          };
      }
    }),
  }));
  const body: Json = { model, messages, stream: norm.stream };
  if (norm.systemText !== null) body.system = norm.systemText;
  if (norm.maxTokens !== null) body.max_tokens = norm.maxTokens;
  if (norm.stops.length > 0) body.stop_sequences = norm.stops;
  if (norm.tools.length > 0) {
    body.tools = norm.tools.map((t) => ({ name: t.name, description: t.description, input_schema: t.parameters }));
    const choice = anthropicToolChoice(norm.rawToolChoice);
    if (choice !== undefined) body.tool_choice = choice;
  }
  if (norm.temperature !== null) body.temperature = norm.temperature;
  if (norm.topP !== null) body.top_p = norm.topP;
  return body;
}

function toGeminiBody(norm: NormRequest, model: string): Json {
  const toolNames = new Map<string, string>();
  for (const msg of norm.messages) {
    for (const part of msg.parts) {
      if (part.kind === 'toolUse') toolNames.set(part.id, part.name);
    }
  }
  const contents: Json[] = norm.messages.map((msg) => ({
    role: msg.role === 'assistant' ? 'model' : 'user',
    parts: msg.parts.map((part) => {
      switch (part.kind) {
        case 'text':
          return { text: part.text };
        case 'image':
          return { inlineData: { mimeType: part.mediaType, data: part.data } };
        case 'imageUrl': {
          const data = parseDataUrl(part.url);
          if (data) return { inlineData: { mimeType: data.mediaType, data: data.data } };
          return { fileData: { fileUri: part.url } };
        }
        case 'toolUse':
          return { functionCall: { name: part.name, args: isRecord(part.input) ? part.input : {} } };
        case 'toolResult':
          return {
            functionResponse: {
              name: toolNames.get(part.toolUseId) ?? 'tool',
              response: { result: part.text },
            },
          };
      }
    }),
  }));
  const body: Json = { contents };
  if (norm.systemText !== null) body.systemInstruction = { parts: [{ text: norm.systemText }] };
  if (norm.tools.length > 0) {
    body.tools = [
      {
        functionDeclarations: norm.tools.map((t) => ({
          name: t.name,
          description: t.description,
          parameters: t.parameters,
        })),
      },
    ];
  }
  const generationConfig: Json = {};
  if (norm.maxTokens !== null) generationConfig.maxOutputTokens = norm.maxTokens;
  if (norm.stops.length > 0) generationConfig.stopSequences = norm.stops;
  if (norm.jsonMode) generationConfig.responseMimeType = 'application/json';
  if (norm.temperature !== null) generationConfig.temperature = norm.temperature;
  if (norm.topP !== null) generationConfig.topP = norm.topP;
  if (Object.keys(generationConfig).length > 0) body.generationConfig = generationConfig;
  return body;
}

// ---------------------------------------------------------------------------
// Provider answers, normalized: text, tool calls, usage, raw stop token.
// ---------------------------------------------------------------------------

interface NormToolCall {
  id: string;
  name: string;
  argsJson: string;
}

interface NormAnswer {
  text: string;
  toolCalls: NormToolCall[];
  usageIn: number | null;
  usageOut: number | null;
  stopRaw: string | null;
}

function sseDataLines(text: string): string[] {
  const out: string[] = [];
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.startsWith('data:')) out.push(trimmed.slice('data:'.length).trim());
  }
  return out;
}

function parseJsonSafe(text: string): Json | null {
  try {
    const value: unknown = JSON.parse(text);
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function parseOpenAIJson(body: Json): NormAnswer {
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const first = isRecord(choices[0]) ? choices[0] : {};
  const message = isRecord(first.message) ? first.message : {};
  const toolCalls: NormToolCall[] = [];
  if (Array.isArray(message.tool_calls)) {
    for (const call of message.tool_calls) {
      if (!isRecord(call) || !isRecord(call.function)) continue;
      toolCalls.push({
        id: str(call.id),
        name: str(call.function.name),
        argsJson: str(call.function.arguments) || '{}',
      });
    }
  }
  const usage = isRecord(body.usage) ? body.usage : {};
  return {
    text: blocksText(message.content),
    toolCalls,
    usageIn: numOrNull(usage.prompt_tokens),
    usageOut: numOrNull(usage.completion_tokens),
    stopRaw: typeof first.finish_reason === 'string' ? first.finish_reason : null,
  };
}

function parseOpenAIStream(text: string): NormAnswer {
  let out = '';
  let stopRaw: string | null = null;
  let usageIn: number | null = null;
  let usageOut: number | null = null;
  const toolAcc = new Map<number, NormToolCall>();
  for (const data of sseDataLines(text)) {
    if (data === '' || data === '[DONE]') continue;
    const chunk = parseJsonSafe(data);
    if (!chunk) continue;
    if (isRecord(chunk.usage)) {
      usageIn = numOrNull(chunk.usage.prompt_tokens) ?? usageIn;
      usageOut = numOrNull(chunk.usage.completion_tokens) ?? usageOut;
    }
    const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
    const first = isRecord(choices[0]) ? choices[0] : null;
    if (!first) continue;
    if (typeof first.finish_reason === 'string') stopRaw = first.finish_reason;
    const delta = isRecord(first.delta) ? first.delta : {};
    if (typeof delta.content === 'string') out += delta.content;
    if (Array.isArray(delta.tool_calls)) {
      for (const call of delta.tool_calls) {
        if (!isRecord(call)) continue;
        const index = numOrNull(call.index) ?? 0;
        const acc = toolAcc.get(index) ?? { id: '', name: '', argsJson: '' };
        if (typeof call.id === 'string' && call.id !== '') acc.id = call.id;
        if (isRecord(call.function)) {
          if (typeof call.function.name === 'string' && call.function.name !== '') acc.name = call.function.name;
          if (typeof call.function.arguments === 'string') acc.argsJson += call.function.arguments;
        }
        toolAcc.set(index, acc);
      }
    }
  }
  return {
    text: out,
    toolCalls: [...toolAcc.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v),
    usageIn,
    usageOut,
    stopRaw,
  };
}

function parseAnthropicJson(body: Json): NormAnswer {
  let text = '';
  const toolCalls: NormToolCall[] = [];
  const content = Array.isArray(body.content) ? body.content : [];
  for (const block of content) {
    if (!isRecord(block)) continue;
    if (block.type === 'text') text += str(block.text);
    else if (block.type === 'tool_use') {
      toolCalls.push({
        id: str(block.id),
        name: str(block.name),
        argsJson: JSON.stringify(block.input ?? {}),
      });
    }
  }
  const usage = isRecord(body.usage) ? body.usage : {};
  return {
    text,
    toolCalls,
    usageIn: numOrNull(usage.input_tokens),
    usageOut: numOrNull(usage.output_tokens),
    stopRaw: typeof body.stop_reason === 'string' ? body.stop_reason : null,
  };
}

function parseAnthropicStream(text: string): NormAnswer {
  let out = '';
  let stopRaw: string | null = null;
  let usageIn: number | null = null;
  let usageOut: number | null = null;
  const blocks = new Map<number, { tool: NormToolCall | null }>();
  for (const data of sseDataLines(text)) {
    if (data === '') continue;
    const event = parseJsonSafe(data);
    if (!event) continue;
    const type = str(event.type);
    if (type === 'message_start' && isRecord(event.message) && isRecord(event.message.usage)) {
      usageIn = numOrNull(event.message.usage.input_tokens) ?? usageIn;
      usageOut = numOrNull(event.message.usage.output_tokens) ?? usageOut;
    } else if (type === 'content_block_start') {
      const index = numOrNull(event.index) ?? 0;
      const block = isRecord(event.content_block) ? event.content_block : {};
      blocks.set(index, {
        tool:
          block.type === 'tool_use'
            ? { id: str(block.id), name: str(block.name), argsJson: '' }
            : null,
      });
    } else if (type === 'content_block_delta') {
      const index = numOrNull(event.index) ?? 0;
      const delta = isRecord(event.delta) ? event.delta : {};
      if (delta.type === 'text_delta') out += str(delta.text);
      else if (delta.type === 'input_json_delta') {
        const entry = blocks.get(index);
        if (entry?.tool) entry.tool.argsJson += str(delta.partial_json);
      }
    } else if (type === 'message_delta') {
      const delta = isRecord(event.delta) ? event.delta : {};
      if (typeof delta.stop_reason === 'string') stopRaw = delta.stop_reason;
      if (isRecord(event.usage)) usageOut = numOrNull(event.usage.output_tokens) ?? usageOut;
    }
  }
  const toolCalls = [...blocks.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([, v]) => v.tool)
    .filter((t): t is NormToolCall => t !== null && t.name !== '');
  return { text: out, toolCalls, usageIn, usageOut, stopRaw };
}

function geminiPartsAnswer(parts: unknown): { text: string; toolCalls: NormToolCall[] } {
  let text = '';
  const toolCalls: NormToolCall[] = [];
  if (Array.isArray(parts)) {
    for (const part of parts) {
      if (!isRecord(part)) continue;
      if (typeof part.text === 'string') text += part.text;
      else if (isRecord(part.functionCall)) {
        toolCalls.push({
          id: '',
          name: str(part.functionCall.name),
          argsJson: JSON.stringify(part.functionCall.args ?? {}),
        });
      }
    }
  }
  return { text, toolCalls };
}

function geminiCandidate(body: Json): { candidate: Json; usage: Json } {
  const candidates = Array.isArray(body.candidates) ? body.candidates : [];
  const candidate = isRecord(candidates[0]) ? candidates[0] : {};
  const usage = isRecord(body.usageMetadata) ? body.usageMetadata : {};
  return { candidate, usage };
}

function parseGeminiJson(body: Json): NormAnswer {
  const { candidate, usage } = geminiCandidate(body);
  const content = isRecord(candidate.content) ? candidate.content : {};
  const { text, toolCalls } = geminiPartsAnswer(content.parts);
  return {
    text,
    toolCalls,
    usageIn: numOrNull(usage.promptTokenCount),
    usageOut: numOrNull(usage.candidatesTokenCount),
    stopRaw: typeof candidate.finishReason === 'string' ? candidate.finishReason : null,
  };
}

function parseGeminiStream(text: string): NormAnswer {
  let out = '';
  let stopRaw: string | null = null;
  let usageIn: number | null = null;
  let usageOut: number | null = null;
  const toolCalls: NormToolCall[] = [];
  for (const data of sseDataLines(text)) {
    if (data === '') continue;
    const frame = parseJsonSafe(data);
    if (!frame) continue;
    const { candidate, usage } = geminiCandidate(frame);
    usageIn = numOrNull(usage.promptTokenCount) ?? usageIn;
    usageOut = numOrNull(usage.candidatesTokenCount) ?? usageOut;
    if (typeof candidate.finishReason === 'string') stopRaw = candidate.finishReason;
    const content = isRecord(candidate.content) ? candidate.content : {};
    const partAnswer = geminiPartsAnswer(content.parts);
    out += partAnswer.text;
    toolCalls.push(...partAnswer.toolCalls);
  }
  return { text: out, toolCalls, usageIn, usageOut, stopRaw };
}

// Stop-reason families: 0 = finished, 1 = length, 2 = tool call, 3 = filtered.
const STOP_FAMILIES: Record<string, number> = {
  stop: 0,
  end_turn: 0,
  stop_sequence: 0,
  STOP: 0,
  length: 1,
  max_tokens: 1,
  MAX_TOKENS: 1,
  tool_calls: 2,
  tool_use: 2,
  content_filter: 3,
  refusal: 3,
  SAFETY: 3,
  RECITATION: 3,
};

function stopFamily(stopRaw: string | null, hasToolCalls: boolean): number {
  if (hasToolCalls) return 2;
  if (stopRaw !== null && stopRaw in STOP_FAMILIES) return STOP_FAMILIES[stopRaw];
  return 0;
}

const OPENAI_FINISH = ['stop', 'length', 'tool_calls', 'content_filter'];
const ANTHROPIC_STOP = ['end_turn', 'max_tokens', 'tool_use', 'refusal'];

// If a provider stream ever omits usage, recompute it the way the providers
// meter it: over the system text, the last user text and the tool names of
// the request as sent, and over the answer text.
function computedUsage(norm: NormRequest, answerText: string): { input: number; output: number } {
  const lastUser = [...norm.messages].reverse().find((m) => m.role === 'user');
  const userText = lastUser
    ? lastUser.parts
        .filter((p) => p.kind === 'text')
        .map((p) => (p as { text: string }).text)
        .join('\n')
    : '';
  const joined = (norm.systemText ?? '') + userText + norm.tools.map((t) => t.name).join(',');
  return {
    input: 1 + Math.floor(joined.length / 4),
    output: 1 + Math.floor(answerText.length / 4),
  };
}

function withUsage(norm: NormRequest, ans: NormAnswer): { input: number; output: number } {
  if (ans.usageIn !== null && ans.usageOut !== null) return { input: ans.usageIn, output: ans.usageOut };
  const computed = computedUsage(norm, ans.text);
  return { input: ans.usageIn ?? computed.input, output: ans.usageOut ?? computed.output };
}

// ---------------------------------------------------------------------------
// Client answers: normalized answer -> the client's wire.
// ---------------------------------------------------------------------------

function openAIAnswerJson(ans: NormAnswer, usage: { input: number; output: number }, model: string, seq: number): Json {
  const family = stopFamily(ans.stopRaw, ans.toolCalls.length > 0);
  const message: Json = { role: 'assistant', content: ans.text };
  if (ans.toolCalls.length > 0) {
    message.tool_calls = ans.toolCalls.map((t, i) => ({
      id: t.id !== '' ? t.id : `call_${seq}_${i}`,
      type: 'function',
      function: { name: t.name, arguments: t.argsJson || '{}' },
    }));
  }
  return {
    id: `chatcmpl-${seq}`,
    object: 'chat.completion',
    created: 0,
    model,
    choices: [{ index: 0, message, finish_reason: OPENAI_FINISH[family] }],
    usage: {
      prompt_tokens: usage.input,
      completion_tokens: usage.output,
      total_tokens: usage.input + usage.output,
    },
  };
}

function openAIAnswerStream(ans: NormAnswer, usage: { input: number; output: number }, model: string, seq: number): string {
  const family = stopFamily(ans.stopRaw, ans.toolCalls.length > 0);
  const base = { id: `chatcmpl-${seq}`, object: 'chat.completion.chunk', created: 0, model };
  const lines: string[] = [];
  const chunk = (choices: unknown, extra?: Json): void => {
    lines.push(`data: ${JSON.stringify({ ...base, choices, ...(extra ?? {}) })}`);
  };
  chunk([{ index: 0, delta: { role: 'assistant', content: ans.text }, finish_reason: null }]);
  ans.toolCalls.forEach((t, i) => {
    chunk([
      {
        index: 0,
        delta: {
          tool_calls: [
            {
              index: i,
              id: t.id !== '' ? t.id : `call_${seq}_${i}`,
              type: 'function',
              function: { name: t.name, arguments: t.argsJson || '{}' },
            },
          ],
        },
        finish_reason: null,
      },
    ]);
  });
  chunk([{ index: 0, delta: {}, finish_reason: OPENAI_FINISH[family] }]);
  chunk([], {
    usage: {
      prompt_tokens: usage.input,
      completion_tokens: usage.output,
      total_tokens: usage.input + usage.output,
    },
  });
  lines.push('data: [DONE]');
  return `${lines.join('\n\n')}\n\n`;
}

function anthropicContentBlocks(ans: NormAnswer): Json[] {
  const blocks: Json[] = [];
  if (ans.text !== '' || ans.toolCalls.length === 0) blocks.push({ type: 'text', text: ans.text });
  ans.toolCalls.forEach((t, i) => {
    let input: unknown = {};
    try {
      input = JSON.parse(t.argsJson || '{}');
    } catch {
      input = {};
    }
    blocks.push({ type: 'tool_use', id: t.id !== '' ? t.id : `toolu_${i}`, name: t.name, input });
  });
  return blocks;
}

function anthropicAnswerJson(ans: NormAnswer, usage: { input: number; output: number }, model: string, seq: number): Json {
  const family = stopFamily(ans.stopRaw, ans.toolCalls.length > 0);
  return {
    id: `msg_${seq}`,
    type: 'message',
    role: 'assistant',
    model,
    content: anthropicContentBlocks(ans),
    stop_reason: ANTHROPIC_STOP[family],
    stop_sequence: null,
    usage: { input_tokens: usage.input, output_tokens: usage.output },
  };
}

function anthropicAnswerStream(ans: NormAnswer, usage: { input: number; output: number }, model: string, seq: number): string {
  const family = stopFamily(ans.stopRaw, ans.toolCalls.length > 0);
  const events: [string, Json][] = [];
  events.push([
    'message_start',
    {
      type: 'message_start',
      message: {
        id: `msg_${seq}`,
        type: 'message',
        role: 'assistant',
        model,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: usage.input, output_tokens: 0 },
      },
    },
  ]);
  const blocks = anthropicContentBlocks(ans);
  blocks.forEach((block, index) => {
    if (block.type === 'text') {
      events.push(['content_block_start', { type: 'content_block_start', index, content_block: { type: 'text', text: '' } }]);
      events.push([
        'content_block_delta',
        { type: 'content_block_delta', index, delta: { type: 'text_delta', text: str(block.text) } },
      ]);
    } else {
      events.push([
        'content_block_start',
        {
          type: 'content_block_start',
          index,
          content_block: { type: 'tool_use', id: str(block.id), name: str(block.name), input: {} },
        },
      ]);
      events.push([
        'content_block_delta',
        {
          type: 'content_block_delta',
          index,
          delta: { type: 'input_json_delta', partial_json: JSON.stringify(block.input ?? {}) },
        },
      ]);
    }
    events.push(['content_block_stop', { type: 'content_block_stop', index }]);
  });
  events.push([
    'message_delta',
    {
      type: 'message_delta',
      delta: { stop_reason: ANTHROPIC_STOP[family], stop_sequence: null },
      usage: { output_tokens: usage.output },
    },
  ]);
  events.push(['message_stop', { type: 'message_stop' }]);
  return events.map(([name, data]) => `event: ${name}\ndata: ${JSON.stringify(data)}`).join('\n\n') + '\n\n';
}

// ---------------------------------------------------------------------------
// Routing: pick the best free model per request, fail over on refusals.
// ---------------------------------------------------------------------------

interface ProviderState {
  spec: ProviderSpec;
  index: number;
  models: ListingModel[];
  ratingByModel: Map<string, RatingRow>;
  callsMade: number;
  dead: boolean; // budget spent or outage: refuses for the rest of the trial
}

interface Candidate {
  state: ProviderState;
  model: string;
  rating: number;
}

function capsMeet(caps: { contextLength: number | null; tools: boolean | null; vision: boolean | null; json: boolean | null }, requires: Requires): boolean {
  if (requires.tools === true && caps.tools !== true) return false;
  if (requires.vision === true && caps.vision !== true) return false;
  if (requires.json === true && caps.json !== true) return false;
  if (requires.minContext !== undefined && (caps.contextLength === null || caps.contextLength < requires.minContext)) {
    return false;
  }
  return true;
}

function candidatesFor(states: ProviderState[], requires: Requires, jsonMode: boolean): Candidate[] {
  const out: Candidate[] = [];
  for (const state of states) {
    if (state.dead) continue;
    for (const model of state.models) {
      if (!model.free) continue;
      const row = state.ratingByModel.get(model.id);
      const caps = row
        ? { contextLength: row.contextLength, tools: row.tools, vision: row.vision, json: row.json }
        : { contextLength: model.contextLength, tools: model.tools, vision: model.vision, json: model.json };
      if (!capsMeet(caps, requires)) continue;
      // The Anthropic wire cannot carry a JSON-mode flag, so a JSON-mode
      // request can only be served by an OpenAI- or Gemini-wire provider.
      if (jsonMode && state.spec.wire === 'anthropic') continue;
      out.push({ state, model: model.id, rating: row ? row.ratingMilli : 0 });
    }
  }
  // Best rating first; ties spread across providers (fewest calls made so
  // far), then brief order, then model id — all derived from the brief and
  // the answers seen, never from a clock.
  out.sort(
    (a, b) =>
      b.rating - a.rating ||
      a.state.callsMade - b.state.callsMade ||
      a.state.index - b.state.index ||
      (a.model < b.model ? -1 : a.model > b.model ? 1 : 0),
  );
  return out;
}

type Attempt =
  | { ok: true; answer: NormAnswer }
  | { ok: false; fate: 'dead' | 'transient' | 'skip'; detail: string };

function errorMessage(bodyText: string): string {
  const parsed = parseJsonSafe(bodyText);
  if (parsed && isRecord(parsed.error)) return str(parsed.error.message);
  return bodyText.slice(0, 200);
}

function classifyFailure(status: number, message: string): 'dead' | 'transient' | 'skip' {
  if (message.includes('budget is spent')) return 'dead';
  if (message.includes(' is out')) return 'dead';
  if (status === 402) return 'dead';
  if (message.includes('refused this call')) return 'transient';
  if (status === 429 || status >= 500) return 'transient';
  return 'skip';
}

async function callProvider(state: ProviderState, model: string, norm: NormRequest): Promise<Attempt> {
  const spec = state.spec;
  const base = joinBase(spec.base);
  let url: string;
  let body: Json;
  if (spec.wire === 'openai') {
    url = `${base}/v1/chat/completions`;
    body = toOpenAIBody(norm, model);
  } else if (spec.wire === 'anthropic') {
    url = `${base}/v1/messages`;
    body = toAnthropicBody(norm, model);
  } else {
    url = `${base}/v1beta/models/${model}:${norm.stream ? 'streamGenerateContent' : 'generateContent'}`;
    body = toGeminiBody(norm, model);
  }
  const headers: Record<string, string> = { 'content-type': 'application/json', ...authHeaders(spec) };
  if (spec.wire === 'anthropic') headers['anthropic-version'] = '2023-06-01';
  state.callsMade += 1;
  let res: Response;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(30000),
    });
  } catch (err) {
    return { ok: false, fate: 'transient', detail: `network error: ${String(err)}` };
  }
  const text = await res.text();
  if (!res.ok) {
    const message = errorMessage(text);
    return { ok: false, fate: classifyFailure(res.status, message), detail: `${res.status} ${message}` };
  }
  try {
    if (norm.stream) {
      const answer =
        spec.wire === 'openai'
          ? parseOpenAIStream(text)
          : spec.wire === 'anthropic'
            ? parseAnthropicStream(text)
            : parseGeminiStream(text);
      return { ok: true, answer };
    }
    const parsed = parseJsonSafe(text);
    if (!parsed) return { ok: false, fate: 'skip', detail: 'unparseable provider answer' };
    const answer =
      spec.wire === 'openai' ? parseOpenAIJson(parsed) : spec.wire === 'anthropic' ? parseAnthropicJson(parsed) : parseGeminiJson(parsed);
    return { ok: true, answer };
  } catch (err) {
    return { ok: false, fate: 'skip', detail: `answer parse error: ${String(err)}` };
  }
}

interface Routed {
  answer: NormAnswer;
  model: string;
}

// Try candidates best-first. A provider whose budget is spent or that is in
// outage is marked dead and never called again; a one-off scheduled refusal
// earns the same provider one immediate retry before moving down the list.
async function route(states: ProviderState[], requires: Requires, norm: NormRequest): Promise<Routed | null> {
  const candidates = candidatesFor(states, requires, norm.jsonMode);
  let attempts = 0;
  for (const candidate of candidates) {
    if (candidate.state.dead) continue;
    let tries = 0;
    while (tries < 2 && attempts < 24) {
      tries += 1;
      attempts += 1;
      const attempt = await callProvider(candidate.state, candidate.model, norm);
      if (attempt.ok) return { answer: attempt.answer, model: candidate.model };
      if (attempt.fate === 'dead') {
        candidate.state.dead = true;
        break;
      }
      if (attempt.fate === 'skip') break;
      // transient: loop retries the same provider once, then moves on.
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

function sendJson(res: ServerResponse, status: number, value: unknown): void {
  const body = JSON.stringify(value);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

function sendText(res: ServerResponse, status: number, contentType: string, body: string): void {
  res.writeHead(status, { 'content-type': contentType, 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString('utf8');
}

interface ServerContext {
  brief: Brief;
  states: ProviderState[];
  servedModelIds: string[];
  arrival: number;
}

async function handleCompletion(ctx: ServerContext, clientApi: 'openai' | 'anthropic', body: Json, res: ServerResponse): Promise<void> {
  const seq = ctx.arrival;
  ctx.arrival += 1;
  const row = ctx.brief.workload[seq];
  const requires = row ? row.requires : {};
  const norm = clientApi === 'openai' ? fromOpenAI(body) : fromAnthropic(body);
  const routed = await route(ctx.states, requires, norm);
  if (!routed) {
    if (clientApi === 'openai') {
      sendJson(res, 502, { error: { message: 'no free provider could serve this request', type: 'server_error', code: 'no_provider' } });
    } else {
      sendJson(res, 502, { type: 'error', error: { type: 'api_error', message: 'no free provider could serve this request' } });
    }
    return;
  }
  const usage = withUsage(norm, routed.answer);
  if (clientApi === 'openai') {
    if (norm.stream) sendText(res, 200, 'text/event-stream', openAIAnswerStream(routed.answer, usage, routed.model, seq));
    else sendJson(res, 200, openAIAnswerJson(routed.answer, usage, routed.model, seq));
  } else {
    if (norm.stream) sendText(res, 200, 'text/event-stream', anthropicAnswerStream(routed.answer, usage, routed.model, seq));
    else sendJson(res, 200, anthropicAnswerJson(routed.answer, usage, routed.model, seq));
  }
}

function modelsDocument(ctx: ServerContext, anthropicShape: boolean): Json {
  if (anthropicShape) {
    return {
      data: ctx.servedModelIds.map((id) => ({ type: 'model', id, display_name: id, created_at: '2026-01-01T00:00:00Z' })),
      has_more: false,
      first_id: ctx.servedModelIds[0] ?? null,
      last_id: ctx.servedModelIds[ctx.servedModelIds.length - 1] ?? null,
    };
  }
  return {
    object: 'list',
    data: ctx.servedModelIds.map((id) => ({ id, object: 'model', created: 0, owned_by: 'free-model-aggregator' })),
  };
}

function makeHandler(ctx: ServerContext) {
  return (req: IncomingMessage, res: ServerResponse): void => {
    void (async () => {
      try {
        const path = (req.url ?? '').split('?')[0];
        if (req.method === 'GET' && path === '/v1/models') {
          const authorization = str(req.headers.authorization);
          const anthropicShape = !authorization.startsWith('Bearer') && req.headers['x-api-key'] !== undefined;
          sendJson(res, 200, modelsDocument(ctx, anthropicShape));
          return;
        }
        if (req.method === 'POST' && (path === '/v1/chat/completions' || path === '/v1/messages')) {
          const parsed = parseJsonSafe(await readBody(req));
          if (!parsed) {
            sendJson(res, 400, { error: { message: 'request body is not valid JSON', type: 'invalid_request_error' } });
            return;
          }
          await handleCompletion(ctx, path === '/v1/chat/completions' ? 'openai' : 'anthropic', parsed, res);
          return;
        }
        sendJson(res, 404, { error: { message: 'not found', type: 'invalid_request_error' } });
      } catch (err) {
        if (!res.headersSent) {
          sendJson(res, 500, { error: { message: `internal error: ${String(err)}`, type: 'server_error' } });
        } else {
          res.end();
        }
      }
    })();
  };
}

// ---------------------------------------------------------------------------
// Trial driver: register, serve, poll /work, exit 0 when done.
// ---------------------------------------------------------------------------

async function loadBrief(): Promise<Brief | null> {
  const path = process.env.MAKEFUN_PROMPT_FILE;
  if (!path) return null;
  return parseBrief(await readFile(path, 'utf8'));
}

async function register(port: number, catalog: CatalogDoc): Promise<void> {
  // The env is up before the entry starts; retry briefly in case of a race.
  for (let i = 0; i < 100; i += 1) {
    try {
      const res = await fetch(`${ENV_URL}/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ protocol_version: 1, port, catalog }),
        signal: AbortSignal.timeout(5000),
      });
      if (res.ok) return;
    } catch {
      // fall through and retry
    }
    await sleep(200);
  }
  throw new Error('could not register with the env');
}

async function waitForDone(): Promise<void> {
  for (;;) {
    try {
      const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(5000) });
      if (res.ok) {
        const body: unknown = await res.json();
        if (isRecord(body) && body.done === true) return;
      }
    } catch {
      // keep polling
    }
    await sleep(100);
  }
}

async function main(): Promise<void> {
  const brief = await loadBrief();
  if (process.argv.includes('--list')) {
    if (!brief || !ENV_URL) {
      process.stdout.write('{"providers":[]}\n');
      return;
    }
    const listings = await probeAll(brief);
    process.stdout.write(`${JSON.stringify(buildCatalog(brief, listings))}\n`);
    return;
  }
  if (!brief) throw new Error('MAKEFUN_PROMPT_FILE is not set or unreadable');
  if (!ENV_URL) throw new Error('MAKEFUN_ENV_URL is not set');

  const listings = await probeAll(brief);
  const catalog = buildCatalog(brief, listings);
  const states: ProviderState[] = brief.providers.map((spec, index) => ({
    spec,
    index,
    models: listings.get(spec.id) ?? [],
    ratingByModel: new Map(brief.ratings.filter((r) => r.provider === spec.id).map((r) => [r.model, r])),
    callsMade: 0,
    dead: false,
  }));
  const servedModelIds: string[] = [];
  const seen = new Set<string>();
  for (const provider of catalog.providers) {
    for (const model of provider.models) {
      if (model.free && !seen.has(model.id)) {
        seen.add(model.id);
        servedModelIds.push(model.id);
      }
    }
  }
  const ctx: ServerContext = { brief, states, servedModelIds, arrival: 0 };
  const server = createServer(makeHandler(ctx));
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const address = server.address();
  const port = isRecord(address) ? numOrNull(address.port) : null;
  if (port === null) throw new Error('server did not bind a port');
  await register(port, catalog);
  await waitForDone();
  server.close();
  process.exit(0);
}

void main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
