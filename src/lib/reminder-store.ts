import { promises as fs } from "fs";
import path from "path";
import { getWill } from "./will-registry";
import { sendReminderEmail } from "./email";

const STORE_PATH = path.join(process.cwd(), ".reminder-store.json");

const TERMINAL_STATUSES = new Set([
  "Triggered",
  "Released",
  "Cancelled",
  "Archived",
  "Settled",
]);

export interface ReminderSubscription {
  id: string;
  willId: string;
  email: string;
  createdAt: string;
  confirmed: boolean;
}

export interface ReminderHistoryEntry {
  id: string;
  willId: string;
  email: string;
  sentAt: string;
}

export interface ReminderStore {
  subscriptions: ReminderSubscription[];
  history: ReminderHistoryEntry[];
}

async function readStore(): Promise<ReminderStore> {
  try {
    const raw = await fs.readFile(STORE_PATH, "utf8");
    const parsed = JSON.parse(raw) as Partial<ReminderStore>;
    return {
      subscriptions: parsed.subscriptions ?? [],
      history: parsed.history ?? [],
    };
  } catch {
    return { subscriptions: [], history: [] };
  }
}

async function writeStore(store: ReminderStore): Promise<void> {
  await fs.writeFile(STORE_PATH, JSON.stringify(store, null, 2), "utf8");
}

export async function registerReminderSubscription(
  willId: string,
  email: string,
): Promise<ReminderSubscription> {
  const store = await readStore();
  const subscription: ReminderSubscription = {
    id: `${willId}:${email}`,
    willId,
    email,
    createdAt: new Date().toISOString(),
    confirmed: false,
  };
  const existingIndex = store.subscriptions.findIndex(
    (s) => s.id === subscription.id,
  );
  if (existingIndex >= 0) {
    store.subscriptions[existingIndex] = subscription;
  } else {
    store.subscriptions.push(subscription);
  }
  await writeStore(store);
  return subscription;
}

export async function confirmReminderSubscription(
  id: string,
): Promise<boolean> {
  const store = await readStore();
  const subscription = store.subscriptions.find((s) => s.id === id);
  if (!subscription) {
    return false;
  }
  subscription.confirmed = true;
  await writeStore(store);
  return true;
}

export async function unsubscribeReminder(id: string): Promise<boolean> {
  const store = await readStore();
  const nextSubscriptions = store.subscriptions.filter((s) => s.id !== id);
  if (nextSubscriptions.length === store.subscriptions.length) {
    return false;
  }
  store.subscriptions = nextSubscriptions;
  await writeStore(store);
  return true;
}

export async function dispatchReminderEmails(): Promise<number> {
  const store = await readStore();
  let sent = 0;
  const prunedWillIds = new Set<string>();

  for (const subscription of store.subscriptions) {
    const will = await getWill(subscription.willId);
    if (!will || TERMINAL_STATUSES.has(will.status)) {
      prunedWillIds.add(subscription.willId);
      continue;
    }
    if (will.status !== "Active" || !subscription.confirmed) {
      continue;
    }
    await sendReminderEmail(subscription.email, will);
    store.history.push({
      id: `${subscription.id}:${Date.now()}`,
      willId: subscription.willId,
      email: subscription.email,
      sentAt: new Date().toISOString(),
    });
    sent += 1;
  }

  if (prunedWillIds.size > 0) {
    store.subscriptions = store.subscriptions.filter(
      (s) => !prunedWillIds.has(s.willId),
    );
    store.history = store.history.filter(
      (h) => !prunedWillIds.has(h.willId),
    );
    await writeStore(store);
  }

  return sent;
}
