"""Reviewed, one-shot correction of the first Windows venv PID mismatch.

Preserves the original deployment and erroneous verification as evidence.
Only history_supervisor.py is replaced. No terminal, job or trading mutation.
"""
import base64
import hashlib
import io
import json
import sys
import zipfile

from history_recovery_release import ROOT, TESTS, remote

CONTEXT = r'''
ORIGINAL=STAGE
STAGE=ORIGINAL/'attempt2'
CANDIDATE=STAGE/'candidate'
def all_processes():
    raw=ps(r"""Get-CimInstance Win32_Process | Where-Object {$_.Name -like 'python*.exe' -or $_.Name -eq 'terminal64.exe'} | ForEach-Object {$c=$_.CommandLine;$role=if($_.Name -eq 'terminal64.exe'){'terminal'}elseif($c -match 'src\.poller_main'){'poller'}elseif($c -match 'src\.trader_main'){'trader'}elseif($c -match 'src\.history_supervisor'){'history_supervisor'}elseif($c -match 'src\.history_main'){'history'}else{'other'};$p=Get-Process -Id $_.ProcessId -ErrorAction SilentlyContinue;[pscustomobject]@{pid=$_.ProcessId;parent=$_.ParentProcessId;created=$_.CreationDate.ToUniversalTime().ToString('o');role=$role;executable=$_.ExecutablePath;responding=[bool]$p.Responding}} | ConvertTo-Json -Compress""")
    return json.loads(raw)
def protected_all(rows):
    traders={r['pid'] for r in rows if r['role']=='trader'}
    return sorted([{k:r[k] for k in ('pid','parent','created','role','executable')} for r in rows
                  if r['role'] in ('terminal','poller','trader') or r['parent'] in traders],key=lambda r:r['pid'])
def original_protected_unchanged(rows):
    previous=json.loads((ORIGINAL/'baseline.json').read_text())['protected']
    ids={r['pid'] for r in previous}
    return protected([r for r in rows if r['pid'] in ids])==previous
'''


def stage():
    files = {p.relative_to(ROOT).as_posix(): p.read_bytes() for p in (ROOT / "src").rglob("*.py")}
    files.update({"tests/" + n: (ROOT / "tests" / n).read_bytes() for n in TESTS})
    files["pyproject.toml"] = (ROOT / "pyproject.toml").read_bytes()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name, content in files.items():
            z.writestr(name, content)
    expected = hashlib.sha256(files["src/history_supervisor.py"]).hexdigest()
    remote(CONTEXT + "\nDATA=" + repr(base64.b64encode(buf.getvalue()).decode()) +
           "\nEXPECTED=" + repr(expected) + r'''
assert not STAGE.exists(),'attempt2_already_staged'
old=json.loads((ORIGINAL/'manifest.json').read_text())['expected']
assert all(digest(ROOT/n)==h for n,h in old.items()),'installed_source_drift'
assert task_state()['state']=='Ready','task_not_stopped'
rows=all_processes()
assert not any(r['role'].startswith('history') for r in rows),'actual_history_process_present'
assert original_protected_unchanged(rows),'original_protected_changed'
STAGE.mkdir()
with zipfile.ZipFile(io.BytesIO(base64.b64decode(DATA))) as z:
    assert all(not n.startswith('/') and '..' not in Path(n).parts for n in z.namelist())
    z.extractall(CANDIDATE)
new=dict(old);new['src/history_supervisor.py']=EXPECTED
(STAGE/'manifest.json').write_text(json.dumps({'before':old,'expected':new,'protected':protected_all(rows)}))
print(json.dumps({'attempt2_staged':True,'protected_count':len(protected_all(rows)),'runtime_changes':0}))
''')


def qualify():
    remote(CONTEXT + r'''
m=json.loads((STAGE/'manifest.json').read_text())
assert digest(CANDIDATE/'src/history_supervisor.py')==m['expected']['src/history_supervisor.py']
env={k:v for k,v in os.environ.items() if not k.startswith(('MT5_','DB_','REDIS_','CONTROL_','HISTORY_','RELIABILITY_'))}
env.update(MT5_LOGIN='0',MT5_PASSWORD='fixture',MT5_SERVER='fixture',DB_PASSWORD='fixture')
r=subprocess.run([str(PYTHON),'-m','pytest','tests/test_history_recovery.py','tests/test_history_isolation.py',
 'tests/test_history_reconnect.py','tests/test_native_budget.py','-q','--tb=short'],cwd=CANDIDATE,env=env,
 capture_output=True,text=True,timeout=45)
print(json.dumps({'test_exit':r.returncode,'summary':r.stdout.splitlines()[-1:]}))
if r.returncode:
    print(r.stdout[-6000:]);raise RuntimeError('qualification_failed')
(STAGE/'qualified.json').write_text(json.dumps({'expected':m['expected'],'tests':r.stdout.splitlines()[-1:]}))
''', timeout=60)


def deploy():
    remote(CONTEXT + r'''
assert not (STAGE/'started.json').exists(),'attempt2_already_attempted'
m=json.loads((STAGE/'manifest.json').read_text());q=json.loads((STAGE/'qualified.json').read_text())
assert q['expected']==m['expected'],'qualification_mismatch'
assert all(digest(ROOT/n)==h for n,h in m['before'].items()),'installed_source_drift'
assert digest(CANDIDATE/'src/history_supervisor.py')==m['expected']['src/history_supervisor.py']
assert task_state()['state']=='Ready' and task_state()['new_action'],'task_not_expected'
rows=all_processes();proof=stores()
assert protected_all(rows)==m['protected'] and original_protected_unchanged(rows),'protected_changed'
assert not any(r['role'].startswith('history') for r in rows),'actual_history_present'
assert len([r for r in rows if r['role']=='terminal'])==5
assert all(r['responding'] for r in rows if r['role']=='terminal'),'terminal_not_responding'
assert proof['owners']==0 and proof['queue']==0 and proof['unfinished']==0,'history_not_empty'
assert proof['isolation'] is True and proof['poller_ttl']>0,'authority_absent'
state_path=ROOT/'.runtime/history-supervisor.json'
state=json.loads(state_path.read_text())
assert state['state']=='backoff' and state['failures']==1 and state['supervisor_pid']==46808,'unexpected_supervisor_state_requires_review'
backup=STAGE/'backup';backup.mkdir()
(backup/'history_supervisor.py').write_bytes((ROOT/'src/history_supervisor.py').read_bytes())
(backup/'supervisor-state.json').write_bytes(state_path.read_bytes())
(STAGE/'started.json').write_text(json.dumps({'at':time.time(),'proof':proof,
 'review':'First-attempt venv redirector PID mismatch, actual owned processes absent, one false timeout. Archived reviewed retry state before corrected launch.'}))
ps("Disable-ScheduledTask -TaskName 'MT5 History Worker' | Out-Null")
assert task_state()['state']=='Disabled'
assert not any(r['role'].startswith('history') for r in all_processes())
target=ROOT/'src/history_supervisor.py';temporary=target.with_suffix('.attempt2-tmp')
temporary.write_bytes((CANDIDATE/'src/history_supervisor.py').read_bytes());temporary.replace(target)
# Recoverable archival of explicitly reviewed faulty-attempt state, not silent circuit reset.
state_path.replace(backup/'supervisor-state-reviewed.json')
assert all(digest(ROOT/n)==h for n,h in m['expected'].items())
ps("Enable-ScheduledTask -TaskName 'MT5 History Worker' | Out-Null")
assert protected_all(all_processes())==m['protected']
ps("Start-ScheduledTask -TaskName 'MT5 History Worker'")
print(json.dumps({'history_only_started':True,'replaced_files':1,'reviewed_state_archived':True}))
''', timeout=65)


def verify():
    remote(CONTEXT + r'''
m=json.loads((STAGE/'manifest.json').read_text());rows=all_processes();proof=stores()
state=json.loads((ROOT/'.runtime/history-supervisor.json').read_text())
health=json.loads((ROOT/'.runtime/history-worker-health.json').read_text())
assert all(digest(ROOT/n)==h for n,h in m['expected'].items()),'source_drift'
assert protected_all(rows)==m['protected'] and original_protected_unchanged(rows),'protected_changed'
children=[r for r in rows if r['role']=='history'];sup=[r for r in rows if r['role']=='history_supervisor']
assert len(children)==1 and children[0]['pid']==state['child_pid']==health['pid']==proof['history']['pid'],'actual_worker_pid_mismatch'
actual=[r for r in sup if r['pid']==state['supervisor_pid']]
assert len(actual)==1 and children[0]['parent']==actual[0]['pid'],'ownership_chain_mismatch'
launcher=[r for r in sup if r['pid']==actual[0]['parent']]
assert len(sup)==2 and len(launcher)==1 and launcher[0]['executable'].lower()==str(PYTHON).replace('/','\\').lower(),'supervisor_launcher_chain_unexpected'
assert state['state']=='running' and state['failures']==0,'supervisor_degraded'
assert -2<=time.time()-health['observed_at']<10 and health['connected'] is True,'local_progress_stale'
assert proof['owners']==1 and proof['isolation'] is True and proof['history']['connected'] is True
assert -2<=time.time()-proof['history']['observed_at']<10,'central_history_stale'
assert task_state()['state']=='Running' and task_state()['new_action']
assert all(r['responding'] for r in rows if r['role']=='terminal')
result={'at':time.time(),'verified':True,'protected_count':len(m['protected']),
        'supervisor':state,'health':health,'stores':proof,'processes':children+sup}
(STAGE/'verified.json').write_text(json.dumps(result))
print(json.dumps(result))
''')


if __name__ == "__main__":
    {"stage": stage, "qualify": qualify, "deploy": deploy, "verify": verify}[sys.argv[1]]()
