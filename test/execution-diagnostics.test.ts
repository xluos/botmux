import { afterEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { inspectDaemonCurrentExecution, ExecutionDiagnosticCache, resolveDaemonCurrentExecution } from '../src/core/current-execution.js';
import { startIpcServer, setIpcAuthSecret, setLarkAppId, type IpcServerHandle } from '../src/core/dashboard-ipc-server.js';
import { fetchDaemonIpc } from '../src/core/daemon-ipc-auth.js';
import { readProcessStartIdentity } from '../src/utils/process-identity.js';
import * as workerPool from '../src/core/worker-pool.js';

const roots: string[] = [];
let ipc: IpcServerHandle | null = null;
afterEach(async () => { if (ipc) await ipc.close(); ipc = null; setIpcAuthSecret(null); setLarkAppId(''); vi.restoreAllMocks(); for (const p of roots.splice(0)) rmSync(p, {recursive:true,force:true}); });
function fixture() {
  const procRoot = mkdtempSync(join(tmpdir(),'execution-diagnostic-')); roots.push(procRoot);
  const proc = (pid: number, parent: number, start: string) => { mkdirSync(join(procRoot,String(pid)),{recursive:true});const f=Array<string>(20).fill('0');f[0]='S';f[1]=String(parent);f[19]=start;writeFileSync(join(procRoot,String(pid),'stat'),`${pid} (process) ${f.join(' ')}`); };
  proc(90,1,'900');proc(100,90,'1000');proc(110,100,'1100');proc(120,110,'1200');
  const ds:any={session:{sessionId:'s',status:'active'},larkAppId:'cli_diag',chatId:'oc_diag',workerGeneration:7,worker:{pid:90,killed:false},localProcessAttestation:{workerGeneration:7,cliPid:100,cliProcStart:'1000'},managedTurnOrigin:{turnId:'trg_diag',capability:'never-return-this-capability',callerOpenId:'never-return-this-caller',preexistingProcessIdentities:['100:1000']}};
  const input={sessionId:'s',procRoot,peer:{ok:true as const,peer:{pid:120,procStart:'1200'}},findSession:()=>ds,now:10};
  return {ds,input,proc};
}
describe('read-only native request diagnostics',()=>{
  it.each([
    ['session_inactive',(d:any)=>d.session.status='idle'],['turn_missing',(d:any)=>delete d.managedTurnOrigin.turnId],
    ['generation_missing',(d:any)=>delete d.workerGeneration],['worker_unavailable',(d:any)=>d.worker.killed=true],
    ['attestation_generation_mismatch',(d:any)=>d.localProcessAttestation.workerGeneration=8],
    ['cli_identity_incomplete',(d:any)=>delete d.localProcessAttestation.cliProcStart],
    ['engine_identity_incomplete',(d:any)=>d.localProcessAttestation.enginePid=110],
    ['execution_roots_missing',(d:any)=>{delete d.localProcessAttestation.cliPid;delete d.localProcessAttestation.cliProcStart;}],
    ['process_snapshot_missing',(d:any)=>d.managedTurnOrigin.preexistingProcessIdentities=[]],
    ['capability_missing',(d:any)=>delete d.managedTurnOrigin.capability],
    ['dispatch_attempt_invalid',(d:any)=>d.managedTurnOrigin.dispatchAttempt=0],
    ['cli_start_mismatch',(d:any)=>d.localProcessAttestation.cliProcStart='changed'],
    ['engine_start_mismatch',(d:any)=>{d.localProcessAttestation.enginePid=110;d.localProcessAttestation.engineProcStart='changed';}],
    ['worker_disconnected',(d:any)=>d.worker.connected=false],
    ['document_invalid',(d:any)=>d.workerGeneration=d.localProcessAttestation.workerGeneration=0],
  ] as const)('reports exact %s without changing native denial', (reason, mutate)=>{
    const {ds,input}=fixture();mutate(ds);const before=JSON.stringify(ds);
    const r=inspectDaemonCurrentExecution(input);expect(r.document).toBeNull();expect(r.diagnostic.reason).toBe(reason);
    expect(resolveDaemonCurrentExecution({...input,peer:input.peer.peer})).toBeNull();expect(JSON.stringify(ds)).toBe(before);
    const raw=JSON.stringify(r.diagnostic);expect(raw).not.toContain('never-return-this-capability');expect(raw).not.toContain('never-return-this-caller');expect(raw).not.toContain('preexistingProcessIdentities');
  });
  it('separates old shell/Aiden ancestry from a genuinely trusted native or engine root',()=>{
    const {ds,input}=fixture();ds.managedTurnOrigin.preexistingProcessIdentities.push('110:1100');
    const bad=inspectDaemonCurrentExecution(input);expect(bad.diagnostic.reason).toBe('preexisting_process');expect(bad.diagnostic.rejectedAncestor).toEqual({pid:110,procStart:'1100'});
    ds.localProcessAttestation.cliPid=110;ds.localProcessAttestation.cliProcStart='1100';expect(inspectDaemonCurrentExecution(input).document?.turnId).toBe('trg_diag');
    delete ds.localProcessAttestation.cliPid;delete ds.localProcessAttestation.cliProcStart;ds.localProcessAttestation.enginePid=110;ds.localProcessAttestation.engineProcStart='1100';expect(inspectDaemonCurrentExecution(input).document?.workerGeneration).toBe(7);
  });
  it('records socket failure without inventing a peer and reports recycled peer starts',()=>{
    const {input,proc}=fixture();expect(inspectDaemonCurrentExecution({...input,peer:{ok:false,reason:'peer_unresolved'}}).diagnostic).toMatchObject({reason:'socket_peer_unresolved',peer:null});
    proc(120,110,'1212');expect(inspectDaemonCurrentExecution(input).diagnostic.reason).toBe('peer_start_mismatch');
  });
  it('bounds and expires snapshots and cannot mutate a stored observation through GET',()=>{
    const {input}=fixture();const cache=new ExecutionDiagnosticCache(1,50),r=inspectDaemonCurrentExecution(input).diagnostic;cache.set(r);
    const copy=cache.get('s',20)!;copy.reason='forged';expect(cache.get('s',20)?.reason).toBe('verified');
    cache.set({...r,sessionId:'other'});expect(cache.get('s',20)).toBeNull();expect(cache.get('other',61)).toBeNull();
  });
  it.skipIf(process.platform!=='linux')('requires host authentication, owning App and a real preceding request for the diagnostic route',async()=>{
    const secret='diagnostic-test-only';setIpcAuthSecret(secret);setLarkAppId('cli_diag');
    const ds:any={session:{sessionId:'s-route',status:'active'},larkAppId:'cli_diag',chatId:'oc_diag',workerGeneration:7,worker:{pid:process.pid,killed:false},localProcessAttestation:{workerGeneration:8,cliPid:process.pid,cliProcStart:readProcessStartIdentity(process.pid)},managedTurnOrigin:{turnId:'trg_real_request',capability:'never-return-this-capability',preexistingProcessIdentities:[`${process.pid}:${readProcessStartIdentity(process.pid)}`]}};
    vi.spyOn(workerPool,'findActiveBySessionId').mockImplementation(id=>id==='s-route'?ds:undefined);
    ipc=await startIpcServer({port:0,host:'127.0.0.1',authRequired:true});const path='/api/sessions/s-route/execution-attestation';
    expect((await fetchDaemonIpc(ipc.port,path,{},secret)).status).toBe(404);
    expect((await fetch(`http://127.0.0.1:${ipc.port}${path}`)).status).toBe(401);
    const native=await fetch(`http://127.0.0.1:${ipc.port}/api/current-execution`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({sessionId:'s-route',turnId:'forged'})});expect(native.status).toBe(403);
    const res=await fetchDaemonIpc(ipc.port,path,{},secret);expect(res.status).toBe(200);const body=await res.json();expect(body.diagnostic).toMatchObject({reason:'attestation_generation_mismatch',live:{turnId:'trg_real_request',workerGeneration:7,attestationGeneration:8}});expect(JSON.stringify(body)).not.toContain('never-return-this-capability');
    ds.larkAppId='cli_foreign';expect((await fetchDaemonIpc(ipc.port,path,{},secret)).status).toBe(404);
  });
});
