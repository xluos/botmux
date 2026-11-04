import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, readFileSync, rmSync, unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { FleetMaintenance, assertFleetMaintenanceAdmission, type MaintenanceAdapter,
  type MaintenanceDriver, type MaintenancePlan, type MaintenanceReceipt } from '../src/core/fleet-maintenance.js';
import { withBotTurnMutation, tryWithBotTurnMutation } from '../src/core/bot-turn-mutation-gate.js';

const fault = vi.hoisted(() => ({ activeRename: false, directory: '' }));
// The real admission component receives only this test's private directory.
vi.mock('../src/core/fleet-maintenance.js', async importOriginal => {
  const actual = await importOriginal<typeof import('../src/core/fleet-maintenance.js')>();
  return { ...actual, assertFleetMaintenanceAdmission: (app: string, directory = fault.directory) => actual.assertFleetMaintenanceAdmission(app, directory) };
});
vi.mock('node:fs', async importOriginal => {
  const fs = await importOriginal<typeof import('node:fs')>();
  return { ...fs, renameSync: (from: string, to: string) => {
    if (fault.activeRename && to.endsWith('/active.json')) {
      fault.activeRename = false;
      throw Object.assign(new Error('isolated active rename failure'), { code: 'EIO' });
    }
    return fs.renameSync(from, to);
  } };
});
const h = (n: number) => n.toString(16).padStart(64, '0');
function fixture(): MaintenancePlan {
  return { schemaVersion: 1, operationId: 'adversarial-a', binding: { taskId: 'task-a', workId: 'work-a', executionId: 'execution-a',
    actorId: 'actor-a', appId: 'app-a', sessionId: 'session-a', scopeRevision: 1, triggerId: 'trigger-a', askId: 'ask-a', approvalHash: h(1), auditHash: h(2) },
    baseline: { supervisorGeneration: 'old-supervisor', members: ['app-a', 'app-b'].map((appId, i) => ({ appId, pid: 100 + i,
      processStart: `start-${i}`, generation: 1, workerGeneration: 1, heartbeatAt: Date.now(), state: 'idle' })),
      configHash: h(3), launcherHash: h(4), distHash: h(5), interpreterHash: h(6) }, candidateDistHash: h(7), rollbackDistHash: h(5),
    candidateLauncherHash: h(8), rollbackLauncherHash: h(4), driverHash: h(9), unit: 'maintenance-a', expiresAt: Date.now() + 1000,
    heartbeatMaxAgeMs: 10_000, stepTimeoutMs: 100 };
}
const driver: MaintenanceDriver = { unit: 'maintenance-a', invocationId: 'invocation-a', pid: 500, processStart: 'driver-start',
  cgroup: '/user.slice/maintenance-a', daemonCgroups: ['/user.slice/botmux.service'], sourceHash: h(9), manager: 'user-systemd' };
function host(plan: MaintenancePlan): MaintenanceAdapter {
  let snapshot = structuredClone(plan.baseline);
  return { bootstrap: vi.fn(async () => true), terminal: vi.fn(async () => ({ triggerId: plan.binding.triggerId, natural: true, state: 'completed' })),
    snapshot: vi.fn(async () => structuredClone(snapshot)), attestDriver: vi.fn(async () => structuredClone(driver)),
    drain: vi.fn(async () => {}), release: vi.fn(async () => {}), reconcile: vi.fn(async () => undefined),
    dispatch: vi.fn(async (step, _p, d) => {
      const n = { activate: 2, rollback: 3, reenable: 4 }[step];
      snapshot = { ...snapshot, supervisorGeneration: `supervisor-${n}`, distHash: step === 'rollback' ? plan.rollbackDistHash : plan.candidateDistHash,
        launcherHash: step === 'rollback' ? plan.rollbackLauncherHash : plan.candidateLauncherHash,
        members: snapshot.members.map(m => ({ ...m, pid: m.pid + 100, generation: n, workerGeneration: n, processStart: `start-${m.appId}-${n}`, heartbeatAt: Date.now() })) };
      return { operationId: plan.operationId, step, driver: d, snapshot: structuredClone(snapshot), processExited: true };
    }) };
}
const wait = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

describe('independent maintenance adversarial probes', () => {
  let directory: string; let runtime: FleetMaintenance; let plan: MaintenancePlan;
  beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(10_000); fault.activeRename = false;
    directory = mkdtempSync(join(tmpdir(), 'maintenance-adversarial-')); fault.directory = directory; runtime = new FleetMaintenance(directory); plan = fixture(); runtime.prepare(plan, plan); });
  afterEach(() => { fault.activeRename = false; vi.useRealTimers(); rmSync(directory, { recursive: true, force: true }); });

  it('keeps durable admission fenced until a pending drain cannot reappear after abort', async () => {
    const adapter = host(plan); let daemonDrained = false;
    vi.mocked(adapter.drain).mockImplementationOnce(async () => { await wait(100); daemonDrained = true; });
    vi.mocked(adapter.release).mockImplementation(async () => { daemonDrained = false; });
    const running = runtime.run(plan.binding, plan.operationId, adapter);
    await vi.advanceTimersByTimeAsync(1);
    await expect(runtime.abort(plan.binding, plan.operationId, adapter)).rejects.toThrow('drain_in_flight');
    expect(adapter.release).not.toHaveBeenCalled();
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).toThrow();
    await vi.advanceTimersByTimeAsync(100);
    const result = await running;
    expect(adapter.dispatch).not.toHaveBeenCalled();
    expect(result.phase).toBe('aborting');
    expect(daemonDrained).toBe(true);
    expect((await runtime.abort(plan.binding, plan.operationId, adapter)).phase).toBe('aborted');
    expect(daemonDrained).toBe(false);
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).not.toThrow();
  });

  it('fails closed when only active draining state remains after its journal is lost', async () => {
    const adapter = host(plan); vi.mocked(adapter.drain).mockImplementationOnce(async () => { await wait(100); });
    const running = runtime.run(plan.binding, plan.operationId, adapter).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(1);
    unlinkSync(join(directory, `${plan.operationId}.json`));
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).toThrow();
    await vi.advanceTimersByTimeAsync(100); await running;
  });

  it('fails closed when active state is lost but a draining journal remains', async () => {
    const adapter = host(plan); vi.mocked(adapter.drain).mockImplementationOnce(async () => { await wait(100); });
    const running = runtime.run(plan.binding, plan.operationId, adapter).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(1);
    unlinkSync(join(directory, 'active.json'));
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).toThrow();
    await vi.advanceTimersByTimeAsync(100); await running;
  });

  it('does not issue drain after its approval expires during asynchronous preflight', async () => {
    await runtime.abort(plan.binding, plan.operationId);
    plan = { ...plan, operationId: 'expiry-window-a', expiresAt: Date.now() + 50 };
    runtime.prepare(plan, plan);
    const adapter = host(plan);
    // Stay inside the adapter's 100ms bound while crossing the 50ms grant expiry.
    vi.mocked(adapter.terminal).mockImplementationOnce(async () => { await wait(75); return { triggerId: plan.binding.triggerId, natural: true, state: 'completed' }; });
    const running = runtime.run(plan.binding, plan.operationId, adapter).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(76); await running;
    expect(adapter.drain).not.toHaveBeenCalled(); expect(adapter.dispatch).not.toHaveBeenCalled();
  });

  it('blocks config drift introduced while first source preflight waits', async () => {
    const adapter = host(plan); const snapshot = structuredClone(plan.baseline);
    vi.mocked(adapter.snapshot).mockImplementation(async () => structuredClone(snapshot));
    vi.mocked(adapter.terminal).mockImplementationOnce(async () => ({ triggerId: plan.binding.triggerId, natural: true, state: 'completed' }))
      .mockImplementationOnce(async () => ({ triggerId: plan.binding.triggerId, natural: true, state: 'completed' }))
      .mockImplementationOnce(async () => { await wait(25); snapshot.configHash = h(99); return { triggerId: plan.binding.triggerId, natural: true, state: 'completed' }; });
    const running = runtime.run(plan.binding, plan.operationId, adapter);
    await vi.advanceTimersByTimeAsync(26);
    expect((await running).phase).toBe('ready'); expect(adapter.dispatch).not.toHaveBeenCalled();
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).toThrow();
  });

  it('fails closed when durable journal succeeds and active rename fails', async () => {
    const adapter = host(plan); fault.activeRename = true;
    await expect(runtime.run(plan.binding, plan.operationId, adapter)).rejects.toThrow('isolated active rename failure');
    const active = JSON.parse(readFileSync(join(directory, 'active.json'), 'utf8'));
    const journal = JSON.parse(readFileSync(join(directory, `${plan.operationId}.json`), 'utf8'));
    expect(active.phase).toBe('prepared'); expect(journal.phase).toBe('draining');
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).toThrow();
    expect(adapter.drain).not.toHaveBeenCalled(); expect(adapter.dispatch).not.toHaveBeenCalled();
  });

  it('does not redispatch a timed out intent under concurrent run attempts or a late receipt', async () => {
    const adapter = host(plan); const original = adapter.dispatch; let complete!: (r: MaintenanceReceipt) => void;
    const receipt = await original('activate', plan, driver);
    // Restore the baseline after preparing the deliberately late receipt.
    const baselineHost = host(plan); adapter.snapshot = baselineHost.snapshot;
    adapter.dispatch = vi.fn(() => new Promise(resolve => { complete = resolve; }));
    const running = runtime.run(plan.binding, plan.operationId, adapter);
    await vi.advanceTimersByTimeAsync(101); expect((await running).phase).toBe('unknown');
    const attempts = await Promise.allSettled([runtime.run(plan.binding, plan.operationId, adapter), runtime.run(plan.binding, plan.operationId, adapter)]);
    expect(attempts.every(r => r.status === 'rejected')).toBe(true);
    complete(receipt); await vi.advanceTimersByTimeAsync(1);
    expect(runtime.read(plan.binding, plan.operationId).phase).toBe('unknown'); expect(adapter.dispatch).toHaveBeenCalledTimes(1);
    expect(() => assertFleetMaintenanceAdmission('app-a', directory)).toThrow();
  });

  it.each(['unbounded', 'bounded'] as const)('rechecks durable fence after %s root mutation waits for another lease', async kind => {
    const appId = `queued-${kind}`; const adapter = host(plan); const action = vi.fn(async () => {});
    const first = withBotTurnMutation(appId, async () => { await wait(100); });
    await vi.advanceTimersByTimeAsync(1);
    const second = (kind === 'unbounded' ? withBotTurnMutation(appId, action) : tryWithBotTurnMutation(appId, 500, action)).catch(() => undefined);
    vi.mocked(adapter.drain).mockRejectedValueOnce(new Error('stop_before_source'));
    expect((await runtime.run(plan.binding, plan.operationId, adapter)).phase).toBe('draining');
    await vi.advanceTimersByTimeAsync(101); await first; await second;
    expect(action).not.toHaveBeenCalled();
  });
});
