import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { FleetMaintenance, assertFleetMaintenanceAdmission, type MaintenanceAdapter, type MaintenanceDriver,
  type MaintenancePlan, type MaintenanceReceipt, type MaintenanceSnapshot } from '../src/core/fleet-maintenance.js';

const h = (n: number) => n.toString(16).padStart(64, '0');
const time = 10_000;
function fixture(): MaintenancePlan {
  return { schemaVersion: 1, operationId: 'operation-a', binding: { taskId: 'task-a', workId: 'work-a', executionId: 'execution-a',
    actorId: 'actor-a', appId: 'app-a', sessionId: 'session-a', scopeRevision: 1, triggerId: 'trigger-a',
    askId: 'ask-a', approvalHash: h(1), auditHash: h(2) }, baseline: { supervisorGeneration: 'old-supervisor',
    members: ['app-a', 'app-b'].map((appId, index) => ({ appId, pid: index + 100, processStart: `start-${index}`,
      generation: 1, workerGeneration: 1, heartbeatAt: time, state: 'idle' as const })),
    configHash: h(3), launcherHash: h(4), distHash: h(5), interpreterHash: h(6) },
    candidateDistHash: h(7), rollbackDistHash: h(5), candidateLauncherHash: h(8), rollbackLauncherHash: h(4),
    driverHash: h(9), unit: 'maintenance-a', expiresAt: time + 600_000, heartbeatMaxAgeMs: 10_000, stepTimeoutMs: 1000 };
}
const driver: MaintenanceDriver = { unit: 'maintenance-a', invocationId: 'invocation-a', pid: 500, processStart: 'driver-start',
  cgroup: '/user.slice/maintenance-a', daemonCgroups: ['/user.slice/botmux.service'], sourceHash: h(9), manager: 'user-systemd' };
function adapter(plan: MaintenancePlan): MaintenanceAdapter {
  let snapshot = structuredClone(plan.baseline);
  return {
    bootstrap: vi.fn(async () => true), terminal: vi.fn(async () => ({ triggerId: plan.binding.triggerId, natural: true, state: 'completed' })),
    snapshot: vi.fn(async () => structuredClone(snapshot)), attestDriver: vi.fn(async () => structuredClone(driver)),
    drain: vi.fn(async () => {}), release: vi.fn(async () => {}),
    dispatch: vi.fn(async (step, _plan, originalDriver) => {
      const sequence = { activate: 2, rollback: 3, reenable: 4 }[step];
      snapshot = { ...snapshot, supervisorGeneration: `supervisor-${sequence}`,
        distHash: step === 'rollback' ? plan.rollbackDistHash : plan.candidateDistHash,
        launcherHash: step === 'rollback' ? plan.rollbackLauncherHash : plan.candidateLauncherHash,
        members: snapshot.members.map(m => ({ ...m, pid: m.pid + 100, generation: sequence,
          workerGeneration: sequence, processStart: `start-${m.appId}-${sequence}` })) };
      return { operationId: plan.operationId, step, driver: originalDriver, snapshot: structuredClone(snapshot), processExited: true };
    }), reconcile: vi.fn(async () => undefined),
  };
}

describe('task-bound fleet maintenance', () => {
  let directory: string; let runtime: FleetMaintenance; let plan: MaintenancePlan;
  beforeEach(() => { directory = mkdtempSync(join(tmpdir(), 'fleet-maintenance-')); runtime = new FleetMaintenance(directory, () => time); plan = fixture(); });
  afterEach(() => { vi.useRealTimers(); rmSync(directory, { recursive: true, force: true }); });

  it.each(['taskId', 'workId', 'executionId', 'actorId', 'appId', 'sessionId', 'triggerId', 'askId', 'approvalHash', 'auditHash'] as const)(
    'rejects a mismatched %s grant before persistence', key => {
      const wrong = structuredClone(plan); wrong.binding[key] = key.endsWith('Hash') ? h(99) : 'foreign';
      expect(() => runtime.prepare(wrong, plan)).toThrow('grant_mismatch');
      expect(() => runtime.read(plan.binding, plan.operationId)).toThrow('operation_mismatch');
    });
  it('persists exact input, refuses conflicting operations and preserves abort tombstones', async () => {
    const a = runtime.prepare(plan, plan); expect(runtime.prepare(plan, plan)).toEqual(a);
    const different = { ...plan, operationId: 'operation-b' };
    expect(() => runtime.prepare(different, different)).toThrow('operation_active');
    await runtime.abort(plan.binding, plan.operationId);
    runtime.prepare(different, different); await runtime.abort(different.binding, different.operationId);
    expect(() => runtime.prepare(plan, plan)).toThrow('operation_used');
  });
  it('fails closed for corrupt state, partial persistence and abandoned lock', () => {
    runtime.prepare(plan, plan);
    const archive = join(directory, `${plan.operationId}.json`);
    const stored = JSON.parse(readFileSync(archive, 'utf8')); stored.phase = 'draining'; writeFileSync(archive, JSON.stringify(stored));
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).toThrow('state_unreadable');
    expect(() => runtime.prepare(plan, plan)).toThrow('state_unreadable');
    writeFileSync(join(directory, 'lock'), 'crashed', { mode: 0o600 });
    expect(() => runtime.prepare(plan, plan)).toThrow('locked');
  });
  it('refuses old-host missing bootstrap with zero drain/dispatch', async () => {
    runtime.prepare(plan, plan); const host = adapter(plan); vi.mocked(host.bootstrap).mockResolvedValue(false);
    await expect(runtime.run(plan.binding, plan.operationId, host)).rejects.toThrow('bootstrap_unsupported');
    expect(host.drain).not.toHaveBeenCalled(); expect(host.dispatch).not.toHaveBeenCalled();
    expect(runtime.read(plan.binding, plan.operationId).phase).toBe('prepared');
  });
  it.each([{ triggerId: 'foreign', natural: true, state: 'completed' }, { triggerId: 'trigger-a', natural: false, state: 'completed' },
    { triggerId: 'trigger-a', natural: true, state: 'running' }])('refuses nonmatching natural terminal %j', async terminal => {
    runtime.prepare(plan, plan); const host = adapter(plan); vi.mocked(host.terminal).mockResolvedValue(terminal);
    await expect(runtime.run(plan.binding, plan.operationId, host)).rejects.toThrow('turn_not_terminal');
    expect(host.drain).not.toHaveBeenCalled();
  });
  it.each(['busy', 'queued', 'locked', 'quarantined', 'unknown', 'stale', 'missing'])(
    'rejects %s idle evidence before effects', async state => {
      runtime.prepare(plan, plan); const host = adapter(plan); const snapshot: MaintenanceSnapshot = structuredClone(plan.baseline);
      if (state === 'stale') snapshot.members[0].heartbeatAt = -1;
      else if (state === 'missing') snapshot.members.pop();
      else snapshot.members[0].state = state as 'busy';
      vi.mocked(host.snapshot).mockResolvedValue(snapshot);
      await expect(runtime.run(plan.binding, plan.operationId, host)).rejects.toThrow();
      expect(host.drain).not.toHaveBeenCalled(); expect(host.dispatch).not.toHaveBeenCalled();
    });
  it.each(['configHash', 'launcherHash', 'distHash', 'interpreterHash'] as const)('rejects %s drift', async key => {
    runtime.prepare(plan, plan); const host = adapter(plan); const snapshot = structuredClone(plan.baseline); snapshot[key] = h(99);
    vi.mocked(host.snapshot).mockResolvedValue(snapshot);
    await expect(runtime.run(plan.binding, plan.operationId, host)).rejects.toThrow('baseline_drift');
    expect(host.dispatch).not.toHaveBeenCalled();
  });
  it.each([{ ...driver, cgroup: '/user.slice/botmux.service/child' }, { ...driver, sourceHash: h(99) }, { ...driver, unit: 'foreign' }])(
    'rejects wrong driver source/unit/cgroup %j', async value => {
      runtime.prepare(plan, plan); const host = adapter(plan); vi.mocked(host.attestDriver).mockResolvedValue(value);
      await expect(runtime.run(plan.binding, plan.operationId, host)).rejects.toThrow('driver_origin');
      expect(host.dispatch).not.toHaveBeenCalled();
    });
  it('publishes fence before first ack, preserves partial ack and resumes partial abort', async () => {
    runtime.prepare(plan, plan); const host = adapter(plan);
    vi.mocked(host.drain).mockImplementation(async app => {
      expect(() => assertFleetMaintenanceAdmission(app, directory)).toThrow('draining');
      if (app === 'app-b') throw new Error('ack_lost');
    });
    expect((await runtime.run(plan.binding, plan.operationId, host)).acknowledgements).toEqual(['app-a']);
    host.drainStatus = vi.fn(async () => ({ settled: true, drained: true }));
    await runtime.reconcileDrain(plan.binding, plan.operationId, host);
    vi.mocked(host.release).mockImplementationOnce(async () => {}).mockRejectedValueOnce(new Error('abort_ack_lost'));
    await expect(runtime.abort(plan.binding, plan.operationId, host)).rejects.toThrow('abort_ack_lost');
    expect(() => assertFleetMaintenanceAdmission('unexpected-app', directory)).toThrow('draining');
    expect(runtime.read(plan.binding, plan.operationId).released).toEqual(['app-a']);
    vi.mocked(host.release).mockResolvedValue(undefined);
    expect((await runtime.abort(plan.binding, plan.operationId, host)).phase).toBe('aborted');
    expect(vi.mocked(host.release).mock.calls.map(call => call[0])).toEqual(['app-a', 'app-b', 'app-b']);
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).not.toThrow();
  });
  it('supports manual abort during drain without advancing the source', async () => {
    runtime.prepare(plan, plan); const host = adapter(plan);
    vi.mocked(host.drain).mockImplementationOnce(async () => { await expect(runtime.abort(plan.binding, plan.operationId, host)).rejects.toThrow('drain_in_flight'); });
    const result = await runtime.run(plan.binding, plan.operationId, host);
    expect(result.phase).toBe('aborting'); expect(host.dispatch).not.toHaveBeenCalled();
    expect((await runtime.abort(plan.binding, plan.operationId, host)).phase).toBe('aborted');
  });
  it('runs exact three intents once, verifies actual generations and releases final generations', async () => {
    runtime.prepare(plan, plan); const host = adapter(plan);
    const dispatch = host.dispatch;
    host.dispatch = vi.fn(async (step, p, d) => {
      const persisted = runtime.read(plan.binding, plan.operationId);
      expect(persisted.phase).toBe('running'); expect(persisted.intents.at(-1)?.step).toBe(step);
      expect(() => assertFleetMaintenanceAdmission('app-a', directory)).toThrow('draining');
      return dispatch(step, p, d);
    });
    expect((await runtime.run(plan.binding, plan.operationId, host)).phase).toBe('completed');
    expect(vi.mocked(host.dispatch).mock.calls.map(call => call[0])).toEqual(['activate', 'rollback', 'reenable']);
    expect(vi.mocked(host.release).mock.calls.map(call => call[2])).toEqual([4, 4]);
    await expect(runtime.run(plan.binding, plan.operationId, host)).rejects.toThrow();
    expect(host.dispatch).toHaveBeenCalledTimes(3);
  });
  it.each(['activate', 'rollback', 'reenable'] as const)('retains %s intent on crash and reconciles once without redispatch', async step => {
    runtime.prepare(plan, plan); const host = adapter(plan); const dispatch = host.dispatch; let receipt: MaintenanceReceipt | undefined;
    host.dispatch = vi.fn(async (s, p, d) => {
      const actual = await dispatch(s, p, d);
      if (s === step) { receipt = actual; throw new Error('connection_lost'); } return actual;
    });
    const result = await runtime.run(plan.binding, plan.operationId, host); expect(result.phase).toBe('unknown');
    expect(result.intents.at(-1)?.step).toBe(step);
    await expect(runtime.abort(plan.binding, plan.operationId, host)).rejects.toThrow('effect_in_flight');
    await expect(runtime.run(plan.binding, plan.operationId, host)).rejects.toThrow();
    vi.mocked(host.reconcile).mockResolvedValue(receipt);
    expect((await runtime.reconcile(plan.binding, plan.operationId, host)).phase).toBe('ready');
    await expect(runtime.reconcile(plan.binding, plan.operationId, host)).rejects.toThrow('reconcile_unavailable');
    expect(host.reconcile).toHaveBeenCalledOnce(); expect(host.dispatch).toHaveBeenCalledTimes(result.intents.length);
  });
  it('does not kill or redispatch a slow source transition when bounded wait expires', async () => {
    vi.useFakeTimers(); runtime.prepare(plan, plan); const host = adapter(plan);
    let finish!: (receipt: MaintenanceReceipt) => void;
    const actual = await adapter(plan).dispatch('activate', plan, driver);
    host.dispatch = vi.fn(() => new Promise(resolve => { finish = resolve; }));
    const run = runtime.run(plan.binding, plan.operationId, host);
    await vi.advanceTimersByTimeAsync(plan.stepTimeoutMs + 1);
    expect((await run).phase).toBe('unknown');
    finish(actual); await Promise.resolve();
    expect(runtime.read(plan.binding, plan.operationId).phase).toBe('unknown'); expect(host.dispatch).toHaveBeenCalledOnce();
  });
  it('rejects a wrong operation receipt and refuses another reconciliation', async () => {
    runtime.prepare(plan, plan); const host = adapter(plan); vi.mocked(host.dispatch).mockRejectedValue(new Error('lost'));
    await runtime.run(plan.binding, plan.operationId, host);
    vi.mocked(host.reconcile).mockResolvedValue({ operationId: 'foreign', step: 'activate', driver,
      snapshot: plan.baseline, processExited: true });
    await expect(runtime.reconcile(plan.binding, plan.operationId, host)).rejects.toThrow('receipt_mismatch');
    await expect(runtime.reconcile(plan.binding, plan.operationId, host)).rejects.toThrow('reconcile_unavailable');
  });
  it('serializes simultaneous driver issuance', async () => {
    runtime.prepare(plan, plan); const host = adapter(plan);
    const both = await Promise.allSettled([runtime.run(plan.binding, plan.operationId, host), runtime.run(plan.binding, plan.operationId, host)]);
    expect(both.filter(result => result.status === 'fulfilled')).toHaveLength(1); expect(host.dispatch).toHaveBeenCalledTimes(3);
  });
});
