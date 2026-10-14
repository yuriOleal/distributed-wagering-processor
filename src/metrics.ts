export const metrics = {
  transactions: { PROCESSED: 0, REJECTED: 0, PENDING_REFERENCE: 0 },
  duplicates: 0,
  retries: 0,
  dlq: 0,
  lockConflicts: 0,
  reconciliationDivergences: 0,
  processingCount: 0,
  processingSumMs: 0,
};

// Database exceptions can contain SQL and parameter values; never log their message.
export function errorCode(error: unknown): string {
  const code = (error as { code?: unknown })?.code;
  return typeof code === 'string' && /^[A-Z0-9_]{1,80}$/.test(code) ? code : 'INFRASTRUCTURE_ERROR';
}

export function log(event: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ level: 'info', event, at: new Date().toISOString(),
    correlationId: fields.correlationId ?? null, messageId: fields.messageId ?? null,
    transactionId: fields.transactionId ?? null, walletId: fields.walletId ?? null,
    providerId: fields.providerId ?? null, ...fields }));
}
