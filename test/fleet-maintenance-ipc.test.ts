import { afterEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setIpcAuthSecret, setLarkAppId, startIpcServer, type IpcServerHandle } from '../src/core/dashboard-ipc-server.js';
import { daemonIpcAuthHeaders } from '../src/core/daemon-ipc-auth.js';
import * as maintenance from '../src/core/fleet-maintenance.js';

let server: IpcServerHandle | undefined;
let directory: string | undefined;
afterEach(async () => {
  await server?.close(); server = undefined;
  if (directory) rmSync(directory, { recursive: true, force: true });
  setIpcAuthSecret(null); setLarkAppId(''); vi.restoreAllMocks(); vi.unstubAllEnvs();
});
async function request(action: string, trusted: boolean, body: unknown = { operationId: 'operation-a' }) {
  directory ??= mkdtempSync(join(tmpdir(), 'fleet-maintenance-ipc-'));
  vi.stubEnv('SESSION_DATA_DIR', directory);
  vi.spyOn(maintenance, 'maintenanceDirectory').mockReturnValue(directory);
  setIpcAuthSecret('maintenance-test-secret'); setLarkAppId('app-a');
  server ??= await startIpcServer({ port: 0, host: '127.0.0.1', authRequired: true });
  const path = `/api/fleet-maintenance/${action}`;
  const data = JSON.stringify(body);
  const headers = trusted ? daemonIpcAuthHeaders({ secret: 'maintenance-test-secret', method: 'POST', path, port: server.port }) : new Headers();
  headers.set('content-type', 'application/json');
  return fetch(`http://127.0.0.1:${server.port}${path}`, { method: 'POST', body: data, headers });
}
describe('host authenticated fleet maintenance endpoint', () => {
  it.each(['prepare', 'read', 'abort', 'run'])('rejects unauthenticated %s before opening a grant', async action => {
    const grant = vi.spyOn(maintenance, 'readMaintenanceGrant');
    const response = await request(action, false);
    expect(response.status).toBe(401); expect(grant).not.toHaveBeenCalled();
  });
  it('rejects a missing host-installed grant despite valid host HMAC', async () => {
    const response = await request('prepare', true);
    expect(response.status).toBe(409); expect((await response.json()).ok).toBe(false);
  });
  it('rejects a foreign App grant before maintenance state writes', async () => {
    vi.spyOn(maintenance, 'readMaintenanceGrant').mockReturnValue({ binding: { appId: 'foreign' } } as maintenance.MaintenancePlan);
    const prepare = vi.spyOn(maintenance.FleetMaintenance.prototype, 'prepare');
    const response = await request('prepare', true);
    expect(response.status).toBe(403); expect(prepare).not.toHaveBeenCalled();
  });
  it('rejects wrong task/session/operation request binding', async () => {
    vi.spyOn(maintenance, 'readMaintenanceGrant').mockReturnValue({ binding: { appId: 'app-a', taskId: 'task-a', sessionId: 'session-a' } } as maintenance.MaintenancePlan);
    const response = await request('read', true, { operationId: 'operation-a', binding: { appId: 'app-a', taskId: 'foreign', sessionId: 'session-a' } });
    expect(response.status).toBe(403); expect((await response.json()).error).toBe('maintenance_binding_mismatch');
  });
  it('refuses production dispatch without an attested bootstrap adapter', async () => {
    const binding = { appId: 'app-a', taskId: 'task-a', sessionId: 'session-a' };
    vi.spyOn(maintenance, 'readMaintenanceGrant').mockReturnValue({ binding } as maintenance.MaintenancePlan);
    const run = vi.spyOn(maintenance.FleetMaintenance.prototype, 'run').mockRejectedValue(new Error('bootstrap_adapter_unavailable'));
    const response = await request('run', true, { operationId: 'operation-a', binding });
    expect(response.status).toBe(409); expect((await response.json()).error).toBe('bootstrap_adapter_unavailable');
    expect(run).toHaveBeenCalledOnce();
  });
});
