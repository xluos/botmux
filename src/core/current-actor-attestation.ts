import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, readdirSync, readlinkSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';

import { CURRENT_ACTOR_SCHEMA, normalizeActorEmail, type CurrentActorDocument } from '../cli/current-actor.js';
import { resolveVerifiedUserIdentity } from '../im/lark/identity-cache.js';
import { collectSessionLineagePids } from './preview-port-owner.js';
import { larkTransportEnabled, type DaemonSession } from './types.js';
import { parseScheduledTurnId } from './scheduled-turn-provenance.js';
import {
  PROC_STAT_STARTTIME_INDEX,
  procStatFieldsAfterComm,
  procStatStartTimeDigits,
} from '../utils/proc-stat.js';
import { isLoopbackPeer } from '../utils/loopback-peers.js';

const TCP_ESTABLISHED_STATE = '01';

export interface ProcessIdentity {
  pid: number;
  procStart: string;
}

export type LoopbackPeerResolution =
  | { ok: true; peer: ProcessIdentity }
  | { ok: false; reason: 'platform_unsupported' | 'not_loopback' | 'socket_unavailable' | 'peer_unresolved' };

function readProcStart(pid: number, procRoot: string): string | undefined {
  if (!Number.isSafeInteger(pid) || pid <= 1) return undefined;
  try {
    return procStatStartTimeDigits(readFileSync(join(procRoot, String(pid), 'stat'), 'utf8'));
  } catch { /* use the hardened ps fallback below when procfs is unavailable */ }
  if (procRoot !== '/proc') return undefined;
  const ps = ['/usr/bin/ps', '/bin/ps'].find(existsSync);
  if (!ps) return undefined;
  try {
    const started = execFileSync(ps, ['-o', 'lstart=', '-p', String(pid)], {
      encoding: 'utf8', timeout: 2_000, stdio: ['ignore', 'pipe', 'ignore'],
      env: { PATH: '/usr/bin:/bin', LANG: 'C' },
    }).trim();
    return started || undefined;
  } catch {
    return undefined;
  }
}

export function snapshotProcessIdentities(
  cliPid: number,
  procRoot = '/proc',
): string[] | undefined {
  if (procRoot === '/proc' && process.platform !== 'linux') return undefined;
  const lineage = collectSessionLineagePids(procRoot, [cliPid]);
  if (!lineage || lineage.size === 0) return undefined;
  const identities: string[] = [];
  for (const pid of lineage) {
    const raw = readProcStart(pid, procRoot);
    if (raw) identities.push(`${pid}:${raw}`);
  }
  return identities.length > 0 ? identities.sort() : undefined;
}

function peerSocketInodes(procRoot: string, clientPort: number, serverPort: number): Set<string> | undefined {
  const out = new Set<string>();
  let tableRead = false;
  for (const relative of ['net/tcp', 'net/tcp6']) {
    let text: string;
    try {
      text = readFileSync(join(procRoot, relative), 'utf8');
      tableRead = true;
    } catch {
      continue;
    }
    for (const line of text.split('\n')) {
      const fields = line.trim().split(/\s+/);
      if (fields.length < 10 || fields[3] !== TCP_ESTABLISHED_STATE) continue;
      const localPort = Number.parseInt(fields[1]?.split(':').at(-1) ?? '', 16);
      const remotePort = Number.parseInt(fields[2]?.split(':').at(-1) ?? '', 16);
      if (localPort !== clientPort || remotePort !== serverPort) continue;
      if (/^\d+$/.test(fields[9])) out.add(fields[9]);
    }
  }
  return tableRead ? out : undefined;
}

/** Resolve the process that owns the client half of one live loopback TCP request. */
export function resolveLoopbackPeerProcesses(input: {
  remoteAddress?: string;
  remotePort?: number;
  localPort?: number;
  procRoot?: string;
}): LoopbackPeerResolution {
  const procRoot = input.procRoot ?? '/proc';
  // First ring: the peer address must be local (default loopback set, plus
  // BOTMUX_LOOPBACK_PEERS for hosts whose kernel reports a same-machine peer
  // as a LAN IP). This is NOT the trust boundary by itself: a remote peer can
  // never produce a client-side row in THIS host's /proc/net/tcp, so the
  // socket-inode → fd → process lineage resolution below still pins the peer
  // to a local process. Relaxing the address ring only stops a local peer with
  // a non-standard reported address from failing before that resolution.
  if (!isLoopbackPeer(input.remoteAddress)) return { ok: false, reason: 'not_loopback' };
  if (procRoot === '/proc' && process.platform !== 'linux') {
    return { ok: false, reason: 'platform_unsupported' };
  }
  if (!Number.isSafeInteger(input.remotePort) || !input.remotePort
    || !Number.isSafeInteger(input.localPort) || !input.localPort) {
    return { ok: false, reason: 'socket_unavailable' };
  }
  const inodes = peerSocketInodes(procRoot, input.remotePort, input.localPort);
  if (!inodes || inodes.size !== 1) return { ok: false, reason: 'peer_unresolved' };
  const inode = [...inodes][0];
  const needle = `socket:[${inode}]`;
  let entries: string[];
  try { entries = readdirSync(procRoot); } catch { return { ok: false, reason: 'peer_unresolved' }; }
  const peers: ProcessIdentity[] = [];
  for (const entry of entries) {
    if (!/^\d+$/.test(entry)) continue;
    const pid = Number(entry);
    let fds: string[];
    try { fds = readdirSync(join(procRoot, entry, 'fd')); } catch { continue; }
    let ownsSocket = false;
    for (const fd of fds) {
      try {
        if (readlinkSync(join(procRoot, entry, 'fd', fd)) === needle) {
          ownsSocket = true;
          break;
        }
      } catch { /* process/fd raced away */ }
    }
    if (!ownsSocket) continue;
    const procStart = readProcStart(pid, procRoot);
    if (procStart) peers.push({ pid, procStart });
  }
  return peers.length === 1
    ? { ok: true, peer: peers[0] }
    : { ok: false, reason: 'peer_unresolved' };
}

/** Codex keeps its native tool dispatcher alive across turns. It is part of
 * the attested engine, not an old user command. Never recognize it from argv
 * alone: require a direct attested Codex parent and the release sibling exe. */
function isNativeCodexToolHost(
  pid: number, parent: number, procRoot: string, trustedRootPids: ReadonlySet<number>,
): boolean {
  if (!trustedRootPids.has(parent)) return false;
  try {
    const parentExe = readlinkSync(join(procRoot, String(parent), 'exe'));
    const hostExe = readlinkSync(join(procRoot, String(pid), 'exe'));
    const cmdline = readFileSync(join(procRoot, String(pid), 'cmdline'), 'utf8');
    const argv = cmdline.endsWith('\0') ? cmdline.slice(0, -1).split('\0') : [];
    return basename(parentExe) === 'codex'
      && hostExe === join(dirname(parentExe), 'codex-code-mode-host')
      && argv.length === 1 && argv[0].length > 0;
  } catch {
    return false;
  }
}

function peerBelongsToCurrentTurn(input: {
  peer: ProcessIdentity;
  trustedRootPids: ReadonlySet<number>;
  procRoot: string;
  preexistingProcessIdentities: ReadonlySet<string>;
  onRejection?: (reason: string, ancestor?: ProcessIdentity) => void;
}): boolean {
  if (input.procRoot === '/proc' && process.platform !== 'linux') { input.onRejection?.('lineage_platform_unsupported'); return false; }
  let pid = input.peer.pid;
  for (let depth = 0; depth < 32 && pid > 1; depth++) {
    if (input.trustedRootPids.has(pid)) return true;
    try {
      const fields = procStatFieldsAfterComm(readFileSync(join(input.procRoot, String(pid), 'stat'), 'utf8'));
      if (!fields) { input.onRejection?.('lineage_stat_unreadable'); return false; }
      const parent = Number(fields[1]);
      const started = fields[PROC_STAT_STARTTIME_INDEX];
      if (!Number.isSafeInteger(parent) || !/^\d+$/.test(started ?? '')) { input.onRejection?.('lineage_stat_invalid'); return false; }
      if (input.preexistingProcessIdentities.has(`${pid}:${started}`)) {
        // Only traverse the native dispatcher as an ancestor of a new tool.
        // It cannot itself claim this turn, nor can old tool descendants.
        if (pid === input.peer.pid
          || !isNativeCodexToolHost(pid, parent, input.procRoot, input.trustedRootPids)
          || readProcStart(pid, input.procRoot) !== started) {
          input.onRejection?.('preexisting_process', { pid, procStart: started });
          return false;
        }
      }
      pid = parent;
    } catch {
      input.onRejection?.('lineage_stat_unreadable');
      return false;
    }
  }
  input.onRejection?.('trusted_root_not_reached');
  return false;
}

export type CurrentActorDaemonResult =
  | { ok: true; document: CurrentActorDocument }
  | { ok: false; error: 'current_actor_unverified' };

export interface CurrentTurnPeerAttestation {
  ds: DaemonSession;
  turnId: string;
  generation: number;
  dispatchAttempt?: number;
  callerOpenId: string;
  capability: string;
  cliPid?: number;
  cliProcStart?: string;
  enginePid?: number;
  engineProcStart?: string;
  workerPid: number;
  workerProcStart: string;
  expectedScheduledTurnId?: string;
  processIdentities: string[];
}

export type CurrentExecutionPeerAttestation = Omit<CurrentTurnPeerAttestation, 'callerOpenId'> & {
  callerOpenId?: string;
};

export interface CurrentTurnPeerAttestationInput {
  sessionId: string;
  peer: ProcessIdentity;
  findSession: (sessionId: string) => DaemonSession | undefined;
  procRoot?: string;
  expectedScheduledTurnId?: string;
  /** Internal diagnostics only. Never changes the proof or grants authority. */
  onRejection?: (reason: string, ancestor?: ProcessIdentity) => void;
}

/**
 * Prove that one resolved loopback peer belongs to the exact live turn of a
 * session, reading the caller/turn tuple straight from in-memory daemon state.
 * Shared by `/api/current-actor` and the agent authorization routes so the
 * host-session lineage proof (no rotating capability, no channel env) has a
 * single source of truth.
 */
export function attestCurrentExecutionLoopbackPeer(
  input: CurrentTurnPeerAttestationInput,
): CurrentExecutionPeerAttestation | null {
  const procRoot = input.procRoot ?? '/proc';
  const ds = input.findSession(input.sessionId);
  const turnId = ds?.managedTurnOrigin?.turnId;
  const generation = ds?.workerGeneration;
  const dispatchAttempt = ds?.managedTurnOrigin?.dispatchAttempt;
  const attestation = ds?.localProcessAttestation;
  const cliPid = attestation?.cliPid;
  const cliProcStart = attestation?.cliProcStart;
  const enginePid = attestation?.enginePid;
  const engineProcStart = attestation?.engineProcStart;
  const processIdentities = ds?.managedTurnOrigin?.preexistingProcessIdentities;
  const workerPid = ds?.worker?.pid;
  const workerProcStart = workerPid ? readProcStart(workerPid, procRoot) : undefined;
  const callerOpenId = ds?.managedTurnOrigin?.callerOpenId;
  const capability = ds?.managedTurnOrigin?.capability;
  const scheduledCaller = input.expectedScheduledTurnId
    ? ds?.scheduledTurnCallers?.get(input.expectedScheduledTurnId)
    : undefined;
  const reject = (reason: string): null => { input.onRejection?.(reason); return null; };
  // Keep the original short-circuit order and predicates. Diagnostics are not
  // an alternative attestation path and never repair missing proof material.
  if (!ds) return reject('session_missing');
  if (ds.session.status !== 'active') return reject('session_inactive');
  if (!larkTransportEnabled({ chatId: ds.chatId, apiOnly: ds.initConfig?.apiOnly })) return reject('transport_disabled');
  if (!turnId) return reject('turn_missing');
  if (generation === undefined) return reject('generation_missing');
  if (!workerPid || !workerProcStart || ds.worker?.killed === true) return reject('worker_unavailable');
  if (attestation?.workerGeneration !== generation) return reject('attestation_generation_mismatch');
  if ((cliPid === undefined) !== (cliProcStart === undefined)) return reject('cli_identity_incomplete');
  if ((enginePid === undefined) !== (engineProcStart === undefined)) return reject('engine_identity_incomplete');
  if (cliPid === undefined && enginePid === undefined) return reject('execution_roots_missing');
  if (!processIdentities || processIdentities.length === 0) return reject('process_snapshot_missing');
  if (!capability) return reject('capability_missing');
  if (dispatchAttempt !== undefined && (!Number.isSafeInteger(dispatchAttempt) || dispatchAttempt <= 0)) return reject('dispatch_attempt_invalid');
  if (cliPid !== undefined && readProcStart(cliPid, procRoot) !== cliProcStart) return reject('cli_start_mismatch');
  if (enginePid !== undefined && readProcStart(enginePid, procRoot) !== engineProcStart) return reject('engine_start_mismatch');
  if (input.expectedScheduledTurnId
    && (!parseScheduledTurnId(input.expectedScheduledTurnId)
      || turnId !== input.expectedScheduledTurnId
      || !scheduledCaller
      || scheduledCaller.requestUserOpenId !== callerOpenId)) return reject('scheduled_turn_mismatch');
  const preexistingProcessIdentities = new Set(processIdentities);
  // The worker reports both roots over its private IPC channel. Binding each
  // PID to its proc start time prevents PID reuse, while the descendant walk
  // keeps a tool process inside this exact live turn instead of trusting uid.
  // RPC tools can start before the viewer CLI exists, so the engine is an
  // independent root rather than a fallback identity claim.
  const trustedRootPids = new Set([
    ...(cliPid !== undefined ? [cliPid] : []),
    ...(enginePid !== undefined ? [enginePid] : []),
  ]);
  if (!peerBelongsToCurrentTurn({
      peer: input.peer, trustedRootPids, procRoot,
      preexistingProcessIdentities, onRejection: input.onRejection,
    })) return null;
  if (readProcStart(input.peer.pid, procRoot) !== input.peer.procStart) return reject('peer_start_mismatch');
  return {
    ds, turnId, generation, callerOpenId, capability,
    ...(dispatchAttempt !== undefined ? { dispatchAttempt } : {}),
    workerPid, workerProcStart, cliPid, cliProcStart, enginePid, engineProcStart,
    ...(input.expectedScheduledTurnId
      ? { expectedScheduledTurnId: input.expectedScheduledTurnId }
      : {}),
    processIdentities: [...processIdentities],
  };
}

/** Human identity remains a separate requirement for actor callers.
 * Machine-triggered executions can prove their lineage without it. */
export function attestCurrentTurnLoopbackPeer(
  input: CurrentTurnPeerAttestationInput,
): CurrentTurnPeerAttestation | null {
  const proof = attestCurrentExecutionLoopbackPeer(input);
  if (!proof || typeof proof.callerOpenId !== 'string' || !proof.callerOpenId.startsWith('ou_')) return null;
  return { ...proof, callerOpenId: proof.callerOpenId };
}

/** Re-run the peer attestation and confirm the live turn is byte-for-byte the
 *  one an earlier attestation froze, so an await in between cannot smuggle in a
 *  rotated turn, replaced worker, or changed sender. */
export function currentTurnPeerAttestationStable(
  frozen: CurrentExecutionPeerAttestation,
  input: CurrentTurnPeerAttestationInput,
): boolean {
  const again = attestCurrentExecutionLoopbackPeer(input);
  return !!again
    && again.ds === frozen.ds && again.turnId === frozen.turnId
    && again.generation === frozen.generation && again.callerOpenId === frozen.callerOpenId
    && again.dispatchAttempt === frozen.dispatchAttempt
    && again.capability === frozen.capability && again.cliPid === frozen.cliPid
    && again.cliProcStart === frozen.cliProcStart && again.workerPid === frozen.workerPid
    && again.workerProcStart === frozen.workerProcStart
    && again.enginePid === frozen.enginePid
    && again.engineProcStart === frozen.engineProcStart
    && again.expectedScheduledTurnId === frozen.expectedScheduledTurnId
    && JSON.stringify(again.processIdentities) === JSON.stringify(frozen.processIdentities);
}

/** Daemon-owned authorization and identity lookup for the current live turn. */
export async function resolveDaemonCurrentActor(input: {
  sessionId: string;
  peer: ProcessIdentity;
  findSession: (sessionId: string) => DaemonSession | undefined;
  resolveIdentity?: typeof resolveVerifiedUserIdentity;
  procRoot?: string;
  expectedScheduledTurnId?: string;
}): Promise<CurrentActorDaemonResult> {
  const procRoot = input.procRoot ?? '/proc';
  const attestInput = {
    sessionId: input.sessionId, peer: input.peer,
    findSession: input.findSession, procRoot,
    ...(input.expectedScheduledTurnId
      ? { expectedScheduledTurnId: input.expectedScheduledTurnId }
      : {}),
  };
  const frozen = attestCurrentTurnLoopbackPeer(attestInput);
  if (!frozen) return { ok: false, error: 'current_actor_unverified' };

  const identity = await (input.resolveIdentity ?? resolveVerifiedUserIdentity)(frozen.ds.larkAppId, frozen.callerOpenId);
  if (!identity || identity.type !== 'user' || identity.openId !== frozen.callerOpenId) {
    return { ok: false, error: 'current_actor_unverified' };
  }
  let email: string;
  try { email = normalizeActorEmail(identity.email); }
  catch { return { ok: false, error: 'current_actor_unverified' }; }

  if (!currentTurnPeerAttestationStable(frozen, attestInput)) {
    return { ok: false, error: 'current_actor_unverified' };
  }

  return {
    ok: true,
    document: {
      schema: CURRENT_ACTOR_SCHEMA,
      status: 'verified',
      actor: { email },
      // Both values are already in scope here and are the daemon's own state,
      // not anything the caller supplied: `ds.chatId` was read a few lines
      // above by the transport check, and `frozen.turnId` is the turn the
      // attestation just re-verified as byte-identical. Publishing them lets a
      // consumer record WHICH conversation and WHICH turn a human act came
      // from — without them, "this actor is verified" is true of every turn and
      // a consumer has no attested way to bind one act to one effect.
      chatId: frozen.ds.chatId,
      turnId: frozen.turnId,
    },
  };
}
