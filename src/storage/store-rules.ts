import type {
  CompleteActivationRequest,
  WorkflowInstance,
  WorkflowStatus,
} from './workflow-store.js';

/**
 * Pure state-transition rules shared by every store adapter, so that adapters only deal
 * with persistence and atomicity, never with workflow semantics.
 */

export function isTerminal(status: WorkflowStatus): boolean {
  return status === 'completed' || status === 'failed';
}

export function isClaimable(
  instance: Pick<WorkflowInstance, 'status' | 'wakeAt' | 'lease'>,
  now: number,
): boolean {
  return (
    !isTerminal(instance.status) &&
    instance.wakeAt !== null &&
    instance.wakeAt <= now &&
    (instance.lease === null || instance.lease.expiresAt <= now)
  );
}

/**
 * Prevents lost wake-ups: if a signal arrived while the activation was running, the
 * activation could not have seen it, so the instance must run again immediately instead
 * of going to sleep until a timer (or forever, when waiting on that very signal).
 */
export function nextWakeAt(
  request: CompleteActivationRequest,
  currentSignalCount: number,
): number | null {
  if (isTerminal(request.next.status)) return null;
  if (currentSignalCount !== request.observedSignalCount) return request.now;
  return request.next.wakeAt;
}

/** A delivered signal makes a waiting instance due now, without delaying an earlier wake-up. */
export function wakeAtAfterSignal(currentWakeAt: number | null, now: number): number {
  return currentWakeAt === null ? now : Math.min(currentWakeAt, now);
}
