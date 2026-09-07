"""Explicit, baseline-gated history-only native release. No terminal controls.

Run stage, qualify, deploy, verify separately. The deploy requires prior
Windows qualification, zero history owners/unfinished jobs and an existing
responsive terminal fleet. No service other than MT5 History Worker is changed.
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = "d8c67c6"
TARGETS = ("src/history_main.py", "src/history_authority.py", "src/history_supervisor.py",
           "src/mt5/history_connection.py", "src/mt5/native_budget.py", "scripts/install_history_service.ps1")
TESTS = ("test_history_recovery.py", "test_history_isolation.py", "test_history_reconnect.py",
         "test_native_budget.py", "conftest.py", "__init__.py")
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6", "-i",
       "/Users/migce/.ssh/id_ed25519_windows", "developer@192.168.1.4",
       r"C:\pyProjects\MT_Connector\.venv\Scripts\python.exe -"]

COMMON = r'''
import base64,hashlib,io,json,os,subprocess,time,zipfile
from pathlib import Path
ROOT=Path(r'C:\pyProjects\MT_Connector')
STAGE=ROOT/'.codex/releases/history-recovery-20260907'
CANDIDATE=STAGE/'candidate'
PYTHON=ROOT/'.venv/Scripts/python.exe'
def run(args,timeout=12,**kwargs):
    r=subprocess.run(args,capture_output=True,timeout=timeout,**kwargs)
    if r.returncode: raise RuntimeError('command_failed:'+str(r.returncode))
    return r.stdout
def ps(source):
    prefix="$ErrorActionPreference='Stop';"
    return run(['powershell','-NoProfile','-Command',prefix+source],text=True).strip()
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def snapshot():
    raw=ps(r"""Get-CimInstance Win32_Process | Where-Object {$_.Name -in @('python.exe','pythonw.exe','terminal64.exe')} | ForEach-Object { $c=$_.CommandLine; $role=if($_.Name -eq 'terminal64.exe'){'terminal'}elseif($c -match 'src\.poller_main'){'poller'}elseif($c -match 'src\.trader_main'){'trader'}elseif($c -match 'src\.history_supervisor'){'history_supervisor'}elseif($c -match 'src\.history_main'){'history'}else{'other'}; if($role -ne 'other'){$p=Get-Process -Id $_.ProcessId -ErrorAction SilentlyContinue; [pscustomobject]@{pid=$_.ProcessId;created=$_.CreationDate.ToUniversalTime().ToString('o');role=$role;executable=$_.ExecutablePath;responding=[bool]$p.Responding}}} | ConvertTo-Json -Compress""")
    return json.loads(raw)
def protected(rows):
    return sorted([{k:r[k] for k in ('pid','created','role','executable')} for r in rows
                   if r['role'] in ('terminal','poller','trader')],key=lambda r:r['pid'])
def task_state():
    return json.loads(ps(r"""$t=Get-ScheduledTask -TaskName 'MT5 History Worker';$i=$t|Get-ScheduledTaskInfo;[pscustomobject]@{state=[string]$t.State;result=$i.LastTaskResult;old_action=[bool]($t.Actions.Arguments -eq '-m src.history_main');new_action=[bool]($t.Actions.Arguments -eq '-m src.history_supervisor');action_count=$t.Actions.Count}|ConvertTo-Json -Compress"""))
def stores():
    code=r"""
import asyncio,contextlib,io,json
async def main():
 with contextlib.redirect_stdout(io.StringIO()):
  from src.redis_bus.pool import get_redis_pool,close_redis_pool
  from src.db.engine import get_engine
  from sqlalchemy import text
  r=get_redis_pool()
  async with asyncio.timeout(5):
   raw=await r.get('poller:status');p=json.loads(raw) if raw else {}
   raw=await r.get('history:status');h=json.loads(raw) if raw else {}
   data={'isolation':p.get('history_isolated'),'poller_ttl':await r.ttl('poller:status'),
         'queue':await r.llen('backfill:queue'),'history':{k:h.get(k) for k in
         ('pid','generation','phase','connected','observed_at','authority_reason','native')}}
   await close_redis_pool()
  e=get_engine()
  async with asyncio.timeout(5):
   async with e.connect() as c:
    await c.execute(text('SET TRANSACTION READ ONLY'));await c.execute(text("SET LOCAL statement_timeout='2000ms'"))
    data['owners']=(await c.execute(text("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=771205069 AND granted"))).scalar()
    data['unfinished']=(await c.execute(text("SELECT count(*) FROM backfill_jobs WHERE status IN ('queued','pending','running','cancelling')"))).scalar()
  await e.dispose()
 print(json.dumps(data))
asyncio.run(main())
"""
    return json.loads(run(['docker','exec','-i','mt5_api','python','-'],input=code,text=True,timeout=15))
'''


def remote(source, *, timeout=50):
    r = subprocess.run(SSH, input=(COMMON + source).encode(), capture_output=True, timeout=timeout)
    if r.stdout:
        print(r.stdout.decode(errors="replace"), end="", flush=True)
    if r.returncode:
        # Never forward command lines, exception locals or remote stderr.
        raise RuntimeError(f"remote_release_step_failed:{r.returncode}")


def stage():
    files = {p.relative_to(ROOT).as_posix(): p.read_bytes() for p in (ROOT / "src").rglob("*.py")}
    files.update({"tests/" + n: (ROOT / "tests" / n).read_bytes() for n in TESTS})
    files["pyproject.toml"] = (ROOT / "pyproject.toml").read_bytes()
    files["scripts/install_history_service.ps1"] = (ROOT / "scripts/install_history_service.ps1").read_bytes()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    baseline = {}
    for name in TARGETS:
        result = subprocess.run(["git", "show", BASE + ":" + name], cwd=ROOT, capture_output=True)
        baseline[name] = hashlib.sha256(result.stdout).hexdigest() if result.returncode == 0 else None
    expected = {name: hashlib.sha256(files[name]).hexdigest() for name in TARGETS}
    remote("\nBASELINE=" + repr(baseline) + "\nEXPECTED=" + repr(expected) + "\nDATA=" + repr(base64.b64encode(buf.getvalue()).decode()) + r'''
assert not (STAGE/'baseline.json').exists(),'release_already_prepared'
for name,value in BASELINE.items():
    p=ROOT/name
    assert (digest(p) if p.exists() else None)==value,'native_source_drift:'+name
STAGE.mkdir(parents=True,exist_ok=True)
with zipfile.ZipFile(io.BytesIO(base64.b64decode(DATA))) as archive:
    assert all(not n.startswith('/') and '..' not in Path(n).parts for n in archive.namelist())
    archive.extractall(CANDIDATE)
(STAGE/'manifest.json').write_text(json.dumps({'baseline':BASELINE,'expected':EXPECTED}),encoding='utf8')
print(json.dumps({'staged':True,'target_files':len(EXPECTED),'production_sources_unchanged':True}))
''')


def qualify():
    remote(r'''
manifest=json.loads((STAGE/'manifest.json').read_text())
assert all(digest(CANDIDATE/n)==h for n,h in manifest['expected'].items()),'candidate_drift'
env={k:v for k,v in os.environ.items() if not k.startswith(('MT5_','DB_','REDIS_','CONTROL_','HISTORY_','RELIABILITY_'))}
env.update(MT5_LOGIN='0',MT5_PASSWORD='fixture',MT5_SERVER='fixture',DB_PASSWORD='fixture')
args=[str(PYTHON),'-m','pytest','tests/test_history_recovery.py','tests/test_history_isolation.py',
      'tests/test_history_reconnect.py','tests/test_native_budget.py','-q','--tb=short']
r=subprocess.run(args,cwd=CANDIDATE,env=env,capture_output=True,text=True,timeout=45)
print(json.dumps({'windows_test_exit':r.returncode,'summary':r.stdout.splitlines()[-1:]}))
if r.returncode:
    print(r.stdout[-6000:]);raise RuntimeError('windows_qualification_failed')
# Actual bounded executable-path query, no MT5 module initialization.
out=run([str(PYTHON),'-c',r"from src.mt5.history_connection import existing_history_terminal;print(existing_history_terminal(r'c:\mt5_history\terminal64.exe'))"],cwd=CANDIDATE,env=env,text=True)
assert out.strip().isdigit(),'existing_terminal_not_verified'
(STAGE/'qualified.json').write_text(json.dumps({'expected':manifest['expected'],'tests':r.stdout.splitlines()[-1:],
                                               'existing_terminal_pid':int(out.strip())}),encoding='utf8')
print(json.dumps({'qualified':True,'existing_terminal_pid':int(out.strip()),'native_initializations':0}))
''', timeout=65)


def deploy():
    remote(r'''
assert not (STAGE/'baseline.json').exists(),'release_already_attempted'
m=json.loads((STAGE/'manifest.json').read_text());q=json.loads((STAGE/'qualified.json').read_text())
assert q['expected']==m['expected'],'qualification_mismatch'
for n,h in m['baseline'].items():assert (digest(ROOT/n) if (ROOT/n).exists() else None)==h,'source_drift:'+n
for n,h in m['expected'].items():assert digest(CANDIDATE/n)==h,'candidate_drift:'+n
state=task_state();before=snapshot();proof=stores()
assert state['state']=='Ready' and state['old_action'] and state['action_count']==1,'task_not_expected'
assert not any(r['role'].startswith('history') for r in before),'history_process_present'
assert len([r for r in before if r['role']=='terminal'])==5 and all(r['responding'] for r in before),'fleet_not_ready'
assert len([r for r in before if r['role']=='poller'])==1 and len([r for r in before if r['role']=='trader'])==1,'live_owners_not_expected'
assert any(r['pid']==q['existing_terminal_pid'] and r['role']=='terminal' for r in before),'history_terminal_changed'
assert proof['owners']==0 and proof['queue']==0 and proof['unfinished']==0 and proof['isolation'] is True and proof['poller_ttl']>0,'history_store_gate_failed'
assert not (ROOT/'.runtime/history-supervisor.json').exists(),'supervisor_state_requires_review'
backup=STAGE/'backup';backup.mkdir()
task_xml=run(['schtasks','/Query','/TN','MT5 History Worker','/XML'])
(backup/'task.xml').write_bytes(task_xml)
for n,h in m['baseline'].items():
    if h is not None:
        p=backup/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((ROOT/n).read_bytes())
(STAGE/'baseline.json').write_text(json.dumps({'protected':protected(before),'task':state,'stores':proof,'manifest':m}),encoding='utf8')
# Disable only the already-stopped history task while its module set is replaced.
ps("Disable-ScheduledTask -TaskName 'MT5 History Worker' | Out-Null")
assert task_state()['state']=='Disabled','history_task_not_disabled'
assert not any(r['role'].startswith('history') for r in snapshot()),'history_started_during_prepare'
for n,h in m['expected'].items():
    target=ROOT/n;temporary=target.with_suffix('.recovery-tmp');temporary.write_bytes((CANDIDATE/n).read_bytes());temporary.replace(target)
assert all(digest(ROOT/n)==h for n,h in m['expected'].items()),'publication_hash_failed'
ps(r"$a=New-ScheduledTaskAction -Execute 'C:\pyProjects\MT_Connector\.venv\Scripts\python.exe' -Argument '-m src.history_supervisor' -WorkingDirectory 'C:\pyProjects\MT_Connector'; Set-ScheduledTask -TaskName 'MT5 History Worker' -Action $a | Out-Null; Enable-ScheduledTask -TaskName 'MT5 History Worker' | Out-Null")
assert task_state()['new_action'],'task_action_not_installed'
assert protected(snapshot())==protected(before),'protected_identity_changed_before_start'
ps("Start-ScheduledTask -TaskName 'MT5 History Worker'")
print(json.dumps({'history_only_started':True,'source_files':len(m['expected']),'task_action':'history_supervisor','protected_identities_unchanged':True}))
''', timeout=65)


def verify():
    remote(r'''
m=json.loads((STAGE/'manifest.json').read_text());b=json.loads((STAGE/'baseline.json').read_text())
now=snapshot();proof=stores();task=task_state()
supervisor=json.loads((ROOT/'.runtime/history-supervisor.json').read_text())
assert all(digest(ROOT/n)==h for n,h in m['expected'].items()),'published_source_drift'
assert protected(now)==b['protected'],'protected_identity_changed'
assert all(r['responding'] for r in now),'unresponsive_process'
assert len([r for r in now if r['role']=='history'])==1,'history_owner_count'
assert len([r for r in now if r['role']=='history_supervisor'])==1,'supervisor_owner_count'
assert proof['owners']==1 and proof['isolation'] is True,'lease_or_isolation_failed'
assert proof['history']['connected'] is True and -2<=time.time()-proof['history']['observed_at']<10,'history_not_fresh'
assert supervisor['state']=='running' and supervisor['failures']==0,'supervisor_degraded'
assert task['state']=='Running' and task['new_action'],'scheduler_not_running'
result={'verified':True,'protected_count':len(b['protected']),'task':task,'stores':proof,
        'supervisor':supervisor,'history_processes':[r for r in now if r['role'].startswith('history')]}
(STAGE/'verified.json').write_text(json.dumps(result),encoding='utf8')
print(json.dumps(result))
''')


if __name__ == "__main__":
    {"stage": stage, "qualify": qualify, "deploy": deploy, "verify": verify}[sys.argv[1]]()
