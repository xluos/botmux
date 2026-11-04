import { CURRENT_EXECUTION_SCHEMA, isCurrentExecutionDocument, type CurrentExecutionDocument } from '../cli/current-execution.js';
import { createHash } from 'node:crypto';
import { runtimeBuildIdentity } from '../utils/runtime-build-id.js';
import { readProcessStartIdentity } from '../utils/process-identity.js';
import { attestCurrentExecutionLoopbackPeer, type CurrentTurnPeerAttestationInput, type ProcessIdentity, type LoopbackPeerResolution } from './current-actor-attestation.js';

/** Synchronous, read-only snapshot. Caller/permission, business success and
 * durable input acceptance are deliberately not implied by an execution proof. */
export function resolveDaemonCurrentExecution(input: CurrentTurnPeerAttestationInput): CurrentExecutionDocument | null {
  const proof = attestCurrentExecutionLoopbackPeer(input);
  if (!proof) return null;
  if (proof.ds.session.sessionId !== input.sessionId) { input.onRejection?.('session_id_mismatch'); return null; }
  if (proof.ds.worker?.connected === false) { input.onRejection?.('worker_disconnected'); return null; }
  const identityTurnId = proof.ds.managedTurnOrigin?.identityTurnId;
  const document = {
    schema: CURRENT_EXECUTION_SCHEMA, status: 'verified' as const,
    larkAppId: proof.ds.larkAppId, sessionId: input.sessionId, chatId: proof.ds.chatId,
    turnId: proof.turnId, workerGeneration: proof.generation,
    ...(proof.dispatchAttempt !== undefined ? { dispatchAttempt: proof.dispatchAttempt } : {}),
    ...(identityTurnId ? { identityTurnId } : {}),
  };
  if (!isCurrentExecutionDocument(document)) { input.onRejection?.('document_invalid'); return null; }
  return document;
}

/** Safe, read-only projection of one actual native request. Credentials and
 * caller identity are deliberately omitted; this is never a verified proof. */
export function inspectDaemonCurrentExecution(input: {
  sessionId: string;
  peer: LoopbackPeerResolution;
  findSession: CurrentTurnPeerAttestationInput['findSession'];
  procRoot?: string;
  now?: number;
}) {
  const ds = input.findSession(input.sessionId);
  let reason = input.peer.ok ? 'verified' : `socket_${input.peer.reason}`;
  let rejectedAncestor: ProcessIdentity | undefined;
  const document = input.peer.ok ? resolveDaemonCurrentExecution({
    sessionId: input.sessionId, peer: input.peer.peer, findSession: () => ds, procRoot: input.procRoot,
    onRejection: (r, ancestor) => { reason = r; rejectedAncestor = ancestor; },
  }) : null;
  const origin = ds?.managedTurnOrigin;
  const attestation = ds?.localProcessAttestation;
  const diagnostic = {
    schema: 'botmux.execution-attestation-diagnostic.v1',
    observedAt: input.now ?? Date.now(), sessionId: input.sessionId,
    daemon: { pid: process.pid, procStart: readProcessStartIdentity(process.pid) ?? null, build: runtimeBuildIdentity() },
    reason, status: document ? 'verified' : 'blocked',
    peer: input.peer.ok ? { pid: input.peer.peer.pid, procStart: input.peer.peer.procStart } : null,
    ...(rejectedAncestor ? { rejectedAncestor } : {}),
    live: ds ? {
      larkAppId: ds.larkAppId, chatId: ds.chatId, sessionStatus: ds.session.status,
      turnId: origin?.turnId ?? null, workerGeneration: ds.workerGeneration ?? null,
      workerPid: ds.worker?.pid ?? null, workerProcStart: ds.worker?.pid ? readProcessStartIdentity(ds.worker.pid) ?? null : null, workerKilled: ds.worker?.killed === true,
      workerConnected: ds.worker?.connected !== false,
      attestationGeneration: attestation?.workerGeneration ?? null,
      cliPid: attestation?.cliPid ?? null, cliProcStart: attestation?.cliProcStart ?? null,
      enginePid: attestation?.enginePid ?? null, engineProcStart: attestation?.engineProcStart ?? null,
      capabilityPresent: !!origin?.capability,
      processSnapshotCount: origin?.preexistingProcessIdentities?.length ?? 0,
      processSnapshotSha256: origin?.preexistingProcessIdentities
        ? createHash('sha256').update(JSON.stringify(origin.preexistingProcessIdentities)).digest('hex') : null,
    } : null,
  };
  return { document, diagnostic };
}

/** Bounded ephemeral observations; GET never re-evaluates another peer or
 * manufactures a current tuple. Entries expire even while a session is idle. */
export class ExecutionDiagnosticCache {
  private entries = new Map<string, ReturnType<typeof inspectDaemonCurrentExecution>['diagnostic']>();
  constructor(private readonly limit = 64, private readonly ttlMs = 120_000) {}
  set(diagnostic: ReturnType<typeof inspectDaemonCurrentExecution>['diagnostic']) {
    this.entries.delete(diagnostic.sessionId);
    this.entries.set(diagnostic.sessionId, structuredClone(diagnostic));
    while (this.entries.size > this.limit) this.entries.delete(this.entries.keys().next().value!);
  }
  get(sessionId: string, now = Date.now()) {
    const d = this.entries.get(sessionId);
    if (!d || now - d.observedAt > this.ttlMs || now < d.observedAt) { this.entries.delete(sessionId); return null; }
    return structuredClone(d);
  }
}
