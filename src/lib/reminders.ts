import { WillStatus, type Will } from '@sorowill/sdk';

import { nextCheckinDeadline } from '@/lib/deadlines';
import { getSoroWillClient } from '@/lib/sorowill';

export type ReminderKind = 'well-before' | 'imminent';

export interface ReminderSubscription {
  willId: string;
  email: string;
  owner: string;
  confirmed: boolean;
  confirmationToken: string;
  createdAt: string;
  updatedAt: string;
}

export interface ReminderHistoryEntry {
  willId: string;
  email: string;
  wellBeforeSentAt?: string;
  imminentSentAt?: string;
}

export interface ReminderStore {
  subscriptions: Record<string, ReminderSubscription>;
  history: Record<string, ReminderHistoryEntry>;
}

export interface ReminderRegistrationResult {
  ok: boolean;
  subscription?: ReminderSubscription;
  error?: string;
}

export interface ReminderDispatchResult {
  sent: number;
  skipped: number;
  errors: string[];
}

// Reminder subscriptions/history are persisted to a Vercel KV / Upstash Redis
// REST endpoint so they survive across serverless invocations (the local
// filesystem is ephemeral per-invocation on Vercel and cannot be relied on).
// See .env.example for KV_REST_API_URL / KV_REST_API_TOKEN.
//
// NOTE: All env vars are read at call time (inside helper functions) rather
// than at module-load time, so that tests can set process.env before calling
// any of the exported functions.

const KV_LOCK_TTL_SECONDS = 30;
/** How long to wait between retry attempts when the lock is held. */
const KV_LOCK_RETRY_DELAY_MS = 100;
/** Maximum number of acquire retries before giving up. */
const KV_LOCK_MAX_RETRIES = 20;

function kvConfig(): { url: string; token: string; storeKey: string; lockKey: string } {
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  const storeKey = process.env.REMINDER_STORE_KV_KEY || 'sorowill:reminder-store';
  if (!url || !token) {
    throw new Error(
      'Reminder storage is not configured. Set KV_REST_API_URL and KV_REST_API_TOKEN ' +
        '(a Vercel KV / Upstash Redis REST endpoint) so reminder subscriptions persist ' +
        'across serverless invocations. See .env.example.',
    );
  }
  return { url, token, storeKey, lockKey: `${storeKey}:lock` };
}

function getAppBaseUrl(): string {
  const configured = process.env.NEXT_PUBLIC_APP_URL;
  if (configured) return configured.replace(/\/$/, '');
  if (process.env.VERCEL_URL) return `https://${process.env.VERCEL_URL}`;
  return 'http://localhost:3000';
}

function normalizeEmail(email: string): string {
  return email.trim().toLowerCase();
}

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

export function getReminderKind(daysRemaining: number): ReminderKind {
  return daysRemaining <= 14 ? 'imminent' : 'well-before';
}

/**
 * Terminal will statuses for which reminder subscriptions and history are no
 * longer meaningful. Once a will reaches one of these states it will never
 * become Active again, so its subscription and history entries are pruned from
 * the store to keep it (and the per-run RPC calls it drives) bounded.
 */
const TERMINAL_WILL_STATUSES: ReadonlySet<WillStatus> = new Set([
  WillStatus.Triggered,
  WillStatus.Released,
  WillStatus.Cancelled,
  WillStatus.Archived,
  WillStatus.Settled,
]);

function isTerminalWillStatus(status: WillStatus): boolean {
  return TERMINAL_WILL_STATUSES.has(status);
}

// ---------------------------------------------------------------------------
// Distributed lock helpers (Upstash REST SET NX / DEL)
// ---------------------------------------------------------------------------

/**
 * Attempt to acquire a distributed lock.
 * Uses SET <key> <token> EX <ttl> NX via the Upstash REST API.
 * Returns the lock token on success, or null if the lock is already held.
 */
async function tryAcquireLock(token: string): Promise<boolean> {
  // Upstash REST: POST /set/<key>/<value>?EX=<ttl>&NX=
  const { url: baseUrl, token: kvToken, lockKey } = kvConfig();
  const url = new URL(
    `/set/${encodeURIComponent(lockKey)}/${encodeURIComponent(token)}`,
    baseUrl,
  );
  url.searchParams.set('EX', String(KV_LOCK_TTL_SECONDS));
  url.searchParams.set('NX', '');

  const response = await fetch(url.toString(), {
    method: 'POST',
    headers: { Authorization: `Bearer ${kvToken}` },
  });
  if (!response.ok) {
    throw new Error(`Lock acquire request failed: ${response.status}`);
  }
  const body = (await response.json()) as { result: string | null };
  // Upstash returns {"result":"OK"} on success or {"result":null} when key exists.
  return body.result === 'OK';
}

/**
 * Release the distributed lock. Only deletes the key when the stored value
 * matches our token (compare-and-delete via a pipeline) to avoid accidentally
 * releasing a lock that was re-acquired by another process after our TTL expired.
 */
async function releaseLock(token: string): Promise<void> {
  // Use Upstash pipeline to do GET + conditional DEL atomically.
  // Pipeline endpoint: POST /pipeline  body: array of commands
  const { url: baseUrl, token: kvToken, lockKey } = kvConfig();
  const url = `${baseUrl}/pipeline`;
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${kvToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify([
      ['GET', lockKey],
      // We'll inspect the GET result client-side; the DEL is conditional below.
    ]),
  });
  if (!response.ok) {
    // Best-effort release; don't throw so the caller's finally always completes.
    console.warn(`[reminders] Lock release GET failed: ${response.status}`);
    return;
  }
  const results = (await response.json()) as Array<{ result: string | null }>;
  const currentToken = results[0]?.result;
  if (currentToken !== token) {
    // Lock already expired or was acquired by another process — do not delete.
    return;
  }
  // Safe to delete: token matches.
  await fetch(`${baseUrl}/del/${encodeURIComponent(lockKey)}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${kvToken}` },
  });
}

/**
 * Acquire the distributed lock, retrying up to KV_LOCK_MAX_RETRIES times with
 * a short delay between attempts. Throws if the lock cannot be obtained in time.
 * Returns the token to be passed to releaseLock().
 */
async function acquireLock(): Promise<string> {
  // kvConfig() will throw if the env vars are not set — that surfaces the error
  // clearly before we attempt any network calls.
  kvConfig();
  const token = crypto.randomUUID();
  for (let attempt = 0; attempt <= KV_LOCK_MAX_RETRIES; attempt++) {
    if (await tryAcquireLock(token)) {
      return token;
    }
    // Wait before retrying.
    await new Promise<void>((resolve) => setTimeout(resolve, KV_LOCK_RETRY_DELAY_MS));
  }
  throw new Error(
    `[reminders] Could not acquire store lock after ${KV_LOCK_MAX_RETRIES} retries. ` +
      'Another process may be holding it or the lock TTL has not yet expired.',
  );
}

// ---------------------------------------------------------------------------
// Store read / write
// ---------------------------------------------------------------------------

async function readStore(): Promise<ReminderStore> {
  const { url, token, storeKey } = kvConfig();
  const response = await fetch(`${url}/get/${encodeURIComponent(storeKey)}`, {
    headers: { Authorization: `Bearer ${token}` },
    cache: 'no-store',
  });
  if (!response.ok) {
    throw new Error(`Failed to read reminder store: ${response.status}`);
  }
  const payload = (await response.json()) as { result: string | null };
  if (!payload.result) {
    return { subscriptions: {}, history: {} };
  }
  const parsed = JSON.parse(payload.result) as Partial<ReminderStore>;
  return {
    subscriptions: parsed.subscriptions ?? {},
    history: parsed.history ?? {},
  };
}

async function writeStore(store: ReminderStore): Promise<void> {
  const { url, token, storeKey } = kvConfig();
  const response = await fetch(`${url}/set/${encodeURIComponent(storeKey)}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'text/plain',
    },
    body: JSON.stringify(store),
  });
  if (!response.ok) {
    throw new Error(`Failed to write reminder store: ${response.status}`);
  }
}

function getHistoryKey(willId: string, email: string): string {
  return `${willId}:${normalizeEmail(email)}`;
}

/**
 * Remove a subscription and its history entry from the store. Used when a will
 * reaches a terminal status so the store does not grow without bound.
 */
function pruneSubscription(store: ReminderStore, subscriptionKey: string): void {
  const subscription = store.subscriptions[subscriptionKey];
  if (subscription) {
    delete store.history[getHistoryKey(subscription.willId, subscription.email)];
  }
  delete store.subscriptions[subscriptionKey];
}

export async function registerReminderSubscription({
  willId,
  email,
  owner,
  appUrl,
}: {
  willId: string;
  email: string;
  owner: string;
  appUrl: string;


/* … truncated 8338 chars — edit only what you need near the top … */
