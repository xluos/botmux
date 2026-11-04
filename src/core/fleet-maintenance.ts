/** Task-bound maintenance protocol. No updater, timer or process killing.
 * The driver adapter must attest an existing bootstrap before publishing drain.
 * Old releases without that capability are deliberately refused. */
import { createHash } from 'node:crypto';
import { closeSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { z } from 'zod';

const id = z.string().min(1).max(160).regex(/^[A-Za-z0-9_-]+$/);
const hash = z.string().regex(/^[a-f0-9]{64}$/);
const bindingSchema = z.object({ taskId: id, workId: id, executionId: id, actorId: id,
  appId: id, sessionId: id, scopeRevision: z.number().int().positive(), triggerId: id,
  askId: id, approvalHash: hash, auditHash: hash }).strict();
export type MaintenanceBinding = z.infer<typeof bindingSchema>;
const memberSchema = z.object({ appId: id, pid: z.number().int().positive(), processStart: id,
  generation: z.number().int().positive(), workerGeneration: z.number().int().positive(),
  heartbeatAt: z.number().finite(), state: z.enum(['idle', 'busy', 'queued', 'locked', 'quarantined', 'unknown']) }).strict();
const snapshotSchema = z.object({ supervisorGeneration: id, members: z.array(memberSchema).min(1),
  configHash: hash, launcherHash: hash, distHash: hash, interpreterHash: hash }).strict();
export type MaintenanceSnapshot = z.infer<typeof snapshotSchema>;
const planSchema = z.object({ schemaVersion: z.literal(1), operationId: id, binding: bindingSchema,
  baseline: snapshotSchema, candidateDistHash: hash, rollbackDistHash: hash,
  candidateLauncherHash: hash, rollbackLauncherHash: hash,
  driverHash: hash, unit: id, expiresAt: z.number().finite(), heartbeatMaxAgeMs: z.number().int().positive().max(30_000),
  stepTimeoutMs: z.number().int().positive().max(300_000) }).strict();
export type MaintenancePlan = z.infer<typeof planSchema>;
export type MaintenanceStep = 'activate' | 'rollback' | 'reenable';
const steps: MaintenanceStep[] = ['activate', 'rollback', 'reenable'];
const driverSchema = z.object({ unit: id, invocationId: id, pid: z.number().int().positive(),
  processStart: id, cgroup: z.string().min(1), daemonCgroups: z.array(z.string().min(1)).min(1),
  sourceHash: hash, manager: z.literal('user-systemd') }).strict();
export type MaintenanceDriver = z.infer<typeof driverSchema>;
const receiptSchema = z.object({ operationId: id, step: z.enum(['activate', 'rollback', 'reenable']),
  driver: driverSchema, snapshot: snapshotSchema, processExited: z.literal(true) }).strict();
export type MaintenanceReceipt = z.infer<typeof receiptSchema>;
const stateSchema = z.object({ plan: planSchema, planHash: hash,
  phase: z.enum(['prepared', 'draining', 'ready', 'running', 'unknown', 'aborting', 'aborted', 'completed']),
  driver: driverSchema.optional(), issued: z.boolean(), acknowledgements: z.array(id), released: z.array(id),
  drainPending: z.array(id),
  intents: z.array(z.object({ step: z.enum(['activate', 'rollback', 'reenable']), operationId: id,
    at: z.number().finite(), reconciled: z.boolean(), receipt: receiptSchema.optional() }).strict()),
  error: z.string().optional() }).strict();
export type MaintenanceState = z.infer<typeof stateSchema>;
export interface MaintenanceAdapter {
  /** Must attest protection already loaded in EVERY old/candidate/rollback
   * process plus all ingress paths. Installing it by an unprotected restart
   * is not a bootstrap. Host installation does not currently supply this. */
  bootstrap(plan: MaintenancePlan): Promise<boolean>;
  terminal(binding: MaintenanceBinding): Promise<{ triggerId: string; natural: boolean; state: string }>;
  snapshot(): Promise<MaintenanceSnapshot>;
  attestDriver(): Promise<MaintenanceDriver>;
  drain(appId: string, operationId: string, generation: number): Promise<void>;
  release(appId: string, operationId: string, generation: number): Promise<void>;
  dispatch(step: MaintenanceStep, plan: MaintenancePlan, driver: MaintenanceDriver): Promise<MaintenanceReceipt>;
  reconcile(step: MaintenanceStep, plan: MaintenancePlan, driver: MaintenanceDriver): Promise<MaintenanceReceipt | undefined>;
  drainStatus?(appId: string, operationId: string, generation: number): Promise<{ settled: boolean; drained: boolean }>;
}

export function maintenanceDirectory(): string { return join(homedir(), '.botmux', 'fleet-maintenance'); }
export function maintenanceDigest(value: unknown): string {
  const canonical = (v: unknown): unknown => Array.isArray(v) ? v.map(canonical)
    : v !== null && typeof v === 'object' ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => a.localeCompare(b)).map(([k, item]) => [k, canonical(item)])) : v;
  return createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
}
function refuse(code: string): never { throw new Error(`fleet_maintenance_${code}`); }
function same(a: unknown, b: unknown): boolean { return maintenanceDigest(a) === maintenanceDigest(b); }
function generation(snapshot: MaintenanceSnapshot): unknown {
  return { ...snapshot, members: snapshot.members.map(m => ({ ...m, heartbeatAt: 0 })) };
}
function privateFile(path: string): void {
  const st = lstatSync(path);
  if (!st.isFile() || st.isSymbolicLink() || (st.mode & 0o077) || st.size > 1024 * 1024
    || (process.getuid && st.uid !== process.getuid())) refuse('unsafe_file');
}
function readState(directory: string, ownLock = false): MaintenanceState | undefined {
  const path = join(directory, 'active.json');
  let activeExists = false;
  try {
    privateFile(path);
    activeExists = true;
    const state = stateSchema.parse(JSON.parse(readFileSync(path, 'utf8')));
    const journal = join(directory, `${state.plan.operationId}.json`);
    privateFile(journal);
    if (!same(state, JSON.parse(readFileSync(journal, 'utf8')))
      || state.planHash !== maintenanceDigest(state.plan)) refuse('partial_persistence');
    return state;
  }
  catch (error) {
    if (!activeExists && (error as NodeJS.ErrnoException).code === 'ENOENT') {
      // A missing active pointer is empty only if no journal or abandoned
      // writer exists. Partial publication cannot reopen admission.
      try {
        const files = readdirSync(directory);
        if (files.some(name => name.endsWith('.json') || name.endsWith('.tmp') || (!ownLock && name === 'lock'))) refuse('state_unreadable');
        return undefined;
      } catch (missing) { if ((missing as NodeJS.ErrnoException).code === 'ENOENT') return undefined; }
    }
    refuse('state_unreadable');
  }
}
function durableWrite(path: string, value: unknown): void {
  const temporary = `${path}.${process.pid}.tmp`;
  const fd = openSync(temporary, 'wx', 0o600);
  try { writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(temporary, path);
  const dir = openSync(join(path, '..'), 'r');
  try { fsyncSync(dir); } finally { closeSync(dir); }
}

/** Every new root admission consults the durable fence; errors fail closed.
 * Existing admitted descendants may finish naturally under their old lease. */
export function assertFleetMaintenanceAdmission(appId: string, directory = maintenanceDirectory()): void {
  const state = readState(directory);
  if (state && !['prepared', 'aborted', 'completed'].includes(state.phase)) {
    // Unexpected fleet members also refuse: a config change cannot escape drain.
    refuse(`draining_${appId}`);
  }
}

export class FleetMaintenance {
  constructor(private readonly directory: string, private readonly now = () => Date.now()) {}

  private change<T>(fn: (state: MaintenanceState | undefined) => { state: MaintenanceState; value: T }): T {
    mkdirSync(this.directory, { recursive: true, mode: 0o700 });
    const st = lstatSync(this.directory);
    if (!st.isDirectory() || st.isSymbolicLink() || (st.mode & 0o077)
      || (process.getuid && st.uid !== process.getuid())) refuse('unsafe_directory');
    const lock = join(this.directory, 'lock');
    let fd: number;
    try { fd = openSync(lock, 'wx', 0o600); } catch { refuse('locked'); }
    // Never steal an abandoned lock on a timer. A crash requires explicit
    // operator reconciliation; no second driver can infer that it never ran.
    try {
      const { state, value } = fn(readState(this.directory, true));
      stateSchema.parse(state);
      durableWrite(join(this.directory, `${state.plan.operationId}.json`), state);
      durableWrite(join(this.directory, 'active.json'), state);
      return value;
    } finally { closeSync(fd); unlinkSync(lock); }
  }

  prepare(raw: unknown, grant: MaintenancePlan): MaintenanceState {
    const plan = planSchema.parse(raw);
    planSchema.parse(grant);
    if (!same(plan, grant)) refuse('grant_mismatch');
    this.checkPlan(plan);
    return this.change(old => {
      if (old?.plan.operationId === plan.operationId) {
        if (!same(old.plan, plan)) refuse('operation_conflict');
        return { state: old, value: old };
      }
      if (old && !['aborted', 'completed'].includes(old.phase)) refuse('operation_active');
      // Tombstones prevent reuse of an already completed/aborted operation.
      try { privateFile(join(this.directory, `${plan.operationId}.json`)); refuse('operation_used'); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
      const state: MaintenanceState = { plan, planHash: maintenanceDigest(plan), phase: 'prepared',
        issued: false, acknowledgements: [], released: [], drainPending: [], intents: [] };
      return { state, value: state };
    });
  }

  read(binding: MaintenanceBinding, operationId: string): MaintenanceState {
    const state = readState(this.directory);
    if (!state || state.plan.operationId !== operationId) refuse('operation_mismatch');
    bindingSchema.parse(binding);
    if (!same(state.plan.binding, binding)) refuse('binding_mismatch');
    if (state.planHash !== maintenanceDigest(state.plan)) refuse('plan_drift');
    return state;
  }
  private update(binding: MaintenanceBinding, operationId: string, fn: (state: MaintenanceState) => void): MaintenanceState {
    return this.change(() => {
      const state = this.read(binding, operationId);
      fn(state);
      return { state, value: state };
    });
  }
  private checkPlan(plan: MaintenancePlan): void {
    if (plan.expiresAt <= this.now() || plan.expiresAt - this.now() > 900_000) refuse('expired');
    const apps = plan.baseline.members.map(member => member.appId);
    if (new Set(apps).size !== apps.length || !apps.includes(plan.binding.appId)) refuse('fleet_mismatch');
    if (plan.rollbackDistHash !== plan.baseline.distHash || plan.rollbackLauncherHash !== plan.baseline.launcherHash) refuse('rollback_mismatch');
  }
  private idle(snapshot: MaintenanceSnapshot, plan: MaintenancePlan): void {
    snapshotSchema.parse(snapshot);
    if (new Set(snapshot.members.map(m => m.appId)).size !== snapshot.members.length
      || snapshot.members.length !== plan.baseline.members.length
      || !same(snapshot.members.map(m => m.appId).sort(), plan.baseline.members.map(m => m.appId).sort())) refuse('fleet_mismatch');
    for (const m of snapshot.members) {
      if (m.state !== 'idle' || m.heartbeatAt > this.now() || this.now() - m.heartbeatAt > plan.heartbeatMaxAgeMs) refuse('not_idle');
    }
  }
  private driver(value: MaintenanceDriver, plan: MaintenancePlan): void {
    driverSchema.parse(value);
    if (value.unit !== plan.unit || value.sourceHash !== plan.driverHash
      || value.daemonCgroups.some(group => value.cgroup === group || value.cgroup.startsWith(`${group}/`))) refuse('driver_origin');
  }
  private async bounded<T>(plan: MaintenancePlan, action: () => Promise<T>, recoveryRead = false): Promise<T> {
    if (!recoveryRead) this.checkPlan(plan);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([action(), new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('maintenance_wait_timeout')),
          recoveryRead ? plan.stepTimeoutMs : Math.min(plan.stepTimeoutMs, plan.expiresAt - this.now()));
      })]);
    } finally { if (timer) clearTimeout(timer); }
  }
  private async beforeEffect(plan: MaintenancePlan, adapter: MaintenanceAdapter): Promise<void> {
    this.checkPlan(plan);
    const terminal = await this.bounded(plan, () => adapter.terminal(plan.binding));
    if (terminal.triggerId !== plan.binding.triggerId || terminal.state !== 'completed' || terminal.natural !== true) refuse('turn_not_terminal');
    if (!await this.bounded(plan, () => adapter.bootstrap(plan))) refuse('bootstrap_unsupported');
  }

  /** One bounded driver invocation. No retry of a dispatched step. Timeout
   * leaves its intent unknown and never cancels or kills the pending process. */
  async run(binding: MaintenanceBinding, operationId: string, adapter: MaintenanceAdapter): Promise<MaintenanceState> {
    const initial = this.read(binding, operationId);
    await this.beforeEffect(initial.plan, adapter);
    const driver = await this.bounded(initial.plan, () => adapter.attestDriver());
    this.driver(driver, initial.plan);
    const snapshot = await this.bounded(initial.plan, () => adapter.snapshot());
    this.idle(snapshot, initial.plan);
    if (!same(snapshot, initial.plan.baseline)) refuse('baseline_drift');
    this.update(binding, operationId, state => {
      if (state.phase !== 'prepared' || state.issued) refuse('already_issued');
      this.checkPlan(state.plan);
      state.issued = true; state.driver = driver; state.phase = 'draining';
    });
    try {
      for (const member of initial.plan.baseline.members) {
        this.update(binding, operationId, state => {
          this.checkPlan(state.plan);
          if (state.phase !== 'draining') refuse('drain_interrupted');
          state.drainPending.push(member.appId);
        });
        await this.bounded(initial.plan, () => adapter.drain(member.appId, operationId, member.generation));
        this.update(binding, operationId, state => {
          if (!['draining', 'aborting'].includes(state.phase)) refuse('drain_interrupted');
          state.drainPending = state.drainPending.filter(app => app !== member.appId);
          state.acknowledgements.push(member.appId);
        });
      }
      await this.beforeEffect(initial.plan, adapter);
      const drained = await this.bounded(initial.plan, () => adapter.snapshot()); this.idle(drained, initial.plan);
      // Heartbeats can advance while all process/source generations stay fixed.
      if (!same(generation(drained), generation(snapshot))) refuse('baseline_drift');
      this.update(binding, operationId, state => { if (state.phase !== 'draining') refuse('drain_interrupted'); state.phase = 'ready'; });
      await this.advance(binding, operationId, adapter, driver);
      // Release is a separate durable state. Partial release remains fenced.
      await this.release(binding, operationId, adapter, true);
    } catch (error) {
      this.update(binding, operationId, state => {
        if (state.phase === 'running') state.phase = 'unknown';
        state.error = error instanceof Error ? error.message : 'failed';
      });
    }
    return this.read(binding, operationId);
  }

  /** Resume after exact-operation reconciliation, using the original driver
   * invocation. Only steps without an intent may be dispatched. */
  async resume(binding: MaintenanceBinding, operationId: string, adapter: MaintenanceAdapter): Promise<MaintenanceState> {
    const state = this.read(binding, operationId);
    if (state.phase !== 'ready' || !state.driver || !state.issued || state.intents.some(i => !i.receipt)) refuse('resume_unavailable');
    await this.advance(binding, operationId, adapter, state.driver);
    return this.release(binding, operationId, adapter, true);
  }
  private async advance(binding: MaintenanceBinding, operationId: string, adapter: MaintenanceAdapter, driver: MaintenanceDriver): Promise<void> {
    for (const step of steps) {
      const current = this.read(binding, operationId);
      if (current.intents.some(i => i.step === step && i.receipt)) continue;
      await this.beforeEffect(current.plan, adapter);
      const actualDriver = await this.bounded(current.plan, () => adapter.attestDriver());
      if (!same(actualDriver, driver)) refuse('driver_origin');
      const observed = await this.bounded(current.plan, () => adapter.snapshot()); this.idle(observed, current.plan);
      const previous = current.intents.filter(i => i.receipt).at(-1)?.receipt?.snapshot ?? current.plan.baseline;
      if (!same(generation(observed), generation(previous))) refuse('baseline_drift');
      this.update(binding, operationId, state => {
        this.checkPlan(state.plan);
        if (state.phase !== 'ready' || state.intents.some(i => i.step === step)) refuse('step_issued');
        state.phase = 'running'; state.intents.push({ step, operationId, at: this.now(), reconciled: false });
      });
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const receipt = await Promise.race([adapter.dispatch(step, current.plan, driver),
          new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error('step_timeout')), current.plan.stepTimeoutMs); })]);
        this.acceptReceipt(binding, operationId, step, receipt);
      } catch (error) {
        this.update(binding, operationId, state => { state.phase = 'unknown'; state.error = error instanceof Error ? error.message : 'failed'; });
        throw error;
      } finally { if (timer) clearTimeout(timer); }
    }
  }

  private acceptReceipt(binding: MaintenanceBinding, operationId: string, step: MaintenanceStep, receipt: MaintenanceReceipt): void {
    receiptSchema.parse(receipt);
    const state = this.read(binding, operationId); const plan = state.plan;
    const expectedDist = step === 'rollback' ? plan.rollbackDistHash : plan.candidateDistHash;
    const expectedLauncher = step === 'rollback' ? plan.rollbackLauncherHash : plan.candidateLauncherHash;
    const previous = state.intents.filter(i => i.receipt).at(-1)?.receipt?.snapshot ?? plan.baseline;
    this.idle(receipt.snapshot, plan);
    if (receipt.operationId !== operationId || receipt.step !== step || !same(receipt.driver, state.driver)
      || receipt.snapshot.distHash !== expectedDist || receipt.snapshot.launcherHash !== expectedLauncher
      || receipt.snapshot.configHash !== plan.baseline.configHash || receipt.snapshot.interpreterHash !== plan.baseline.interpreterHash
      || receipt.snapshot.supervisorGeneration === previous.supervisorGeneration
      || receipt.snapshot.members.some(m => previous.members.some(old => old.appId === m.appId
        && (m.processStart === old.processStart || m.workerGeneration === old.workerGeneration)))) refuse('receipt_mismatch');
    this.update(binding, operationId, current => {
      const intent = current.intents.find(i => i.step === step);
      if (!intent || intent.receipt || !['running', 'unknown'].includes(current.phase)) refuse('receipt_conflict');
      intent.receipt = receipt; current.phase = 'ready';
    });
  }

  async reconcile(binding: MaintenanceBinding, operationId: string, adapter: MaintenanceAdapter): Promise<MaintenanceState> {
    const state = this.read(binding, operationId);
    const intent = state.intents.at(-1);
    if (!state.driver || state.phase !== 'unknown' || !intent || intent.reconciled) refuse('reconcile_unavailable');
    const driver = await this.bounded(state.plan, () => adapter.attestDriver(), true);
    if (!same(driver, state.driver)) refuse('driver_origin');
    this.update(binding, operationId, current => {
      const last = current.intents.at(-1)!;
      if (current.phase !== 'unknown' || last.reconciled) refuse('reconcile_unavailable');
      last.reconciled = true;
    });
    const receipt = await this.bounded(state.plan, () => adapter.reconcile(intent.step, state.plan, driver), true);
    if (receipt) this.acceptReceipt(binding, operationId, intent.step, receipt);
    return this.read(binding, operationId);
  }

  async abort(binding: MaintenanceBinding, operationId: string, adapter?: MaintenanceAdapter): Promise<MaintenanceState> {
    const state = this.read(binding, operationId);
    if (state.intents.length || state.phase === 'running' || state.phase === 'unknown') refuse('effect_in_flight');
    if (state.phase === 'aborted') return state;
    if (state.phase === 'prepared') return this.update(binding, operationId, current => {
      if (current.phase !== 'prepared') refuse('abort_conflict'); current.phase = 'aborted';
    });
    if (!adapter) refuse('release_adapter_required');
    this.update(binding, operationId, current => { current.phase = 'aborting'; });
    if (this.read(binding, operationId).drainPending.length) refuse('drain_in_flight');
    return this.release(binding, operationId, adapter, false);
  }
  async reconcileDrain(binding: MaintenanceBinding, operationId: string, adapter: MaintenanceAdapter): Promise<MaintenanceState> {
    const state = this.read(binding, operationId);
    if (!adapter.drainStatus || state.intents.length || !['draining', 'aborting'].includes(state.phase)) refuse('drain_reconcile_unavailable');
    for (const app of state.drainPending) {
      const member = state.plan.baseline.members.find(m => m.appId === app)!;
      const observed = await this.bounded(state.plan, () => adapter.drainStatus!(app, operationId, member.generation), true);
      if (observed.settled !== true) refuse('drain_in_flight');
      this.update(binding, operationId, current => {
        current.drainPending = current.drainPending.filter(id => id !== app);
        if (observed.drained && !current.acknowledgements.includes(app)) current.acknowledgements.push(app);
      });
    }
    return this.read(binding, operationId);
  }
  /** Resume only the idempotent per-daemon release acknowledgements after a
   * partial final release. All source transitions must already have receipts. */
  async finishRelease(binding: MaintenanceBinding, operationId: string, adapter: MaintenanceAdapter): Promise<MaintenanceState> {
    return this.release(binding, operationId, adapter, true);
  }
  private async release(binding: MaintenanceBinding, operationId: string, adapter: MaintenanceAdapter, complete: boolean): Promise<MaintenanceState> {
    const state = this.update(binding, operationId, current => {
      if (complete ? !['ready', 'aborting'].includes(current.phase) || current.intents.length !== 3 || current.intents.some(i => !i.receipt)
        : !['draining', 'ready', 'aborting'].includes(current.phase) || current.intents.length > 0) refuse('release_conflict');
      current.phase = 'aborting';
      if (current.drainPending.length) refuse('drain_in_flight');
    });
    const members = complete ? state.intents.at(-1)!.receipt!.snapshot.members : state.plan.baseline.members;
    for (const member of members) {
      if (this.read(binding, operationId).released.includes(member.appId)) continue;
      // All members, including uncertain/partial acknowledgements, must ack.
      await this.bounded(state.plan, () => adapter.release(member.appId, operationId, member.generation), true);
      this.update(binding, operationId, current => { current.released.push(member.appId); });
    }
    return this.update(binding, operationId, current => { current.phase = complete ? 'completed' : 'aborted'; });
  }
}

/** Host-only endpoint grant. This must be provisioned from the independently
 * reviewed installation contract, never from a model request or environment.
 * No grant is installed by merely building this source. */
export function readMaintenanceGrant(directory: string, operationId: string): MaintenancePlan {
  id.parse(operationId);
  const path = join(directory, 'grants', `${operationId}.json`);
  privateFile(path);
  return planSchema.parse(JSON.parse(readFileSync(path, 'utf8')));
}

/** The old supervisor has no safe bootstrap adapter. Connect the public
 * protocol to an explicit refusal rather than treating a callback supplied
 * by a CLI/model as proof of loaded host protection. */
export function unavailableMaintenanceAdapter(): MaintenanceAdapter {
  const unavailable = async (): Promise<never> => { throw new Error('bootstrap_adapter_unavailable'); };
  return { bootstrap: async () => false, terminal: unavailable, snapshot: unavailable,
    attestDriver: unavailable, drain: unavailable, release: unavailable, dispatch: unavailable,
    reconcile: unavailable, drainStatus: unavailable };
}
