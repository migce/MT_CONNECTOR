"""Scoped deployment UI helper for the new history terminal, never the fleet."""
import ctypes as c
from ctypes import wintypes as w
import json
from pathlib import Path
import sys
import subprocess

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import get_settings
from src.mt5.history_connection import validate_history_path
from src.mt5.portable import _terminal_processes

s = get_settings()
validate_history_path(s)
pids = {pid for pid, _ in _terminal_processes(s.history_mt5_path)}
assert len(pids) == 1, "History process identity ambiguous"
u = c.windll.user32
u.GetMenu.restype = w.HMENU
u.GetSubMenu.restype = w.HMENU
u.GetMenu.argtypes = [w.HWND]
u.GetSubMenu.argtypes = [w.HMENU, c.c_int]
u.GetMenuItemCount.argtypes = [w.HMENU]
u.GetMenuItemID.argtypes = [w.HMENU, c.c_int]
u.GetMenuStringW.argtypes = [w.HMENU, c.c_uint, w.LPWSTR, c.c_int, c.c_uint]
u.GetWindowTextW.argtypes = [w.HWND, w.LPWSTR, c.c_int]
u.GetClassNameW.argtypes = [w.HWND, w.LPWSTR, c.c_int]
u.GetDlgCtrlID.argtypes = [w.HWND]
u.SendMessageTimeoutW.argtypes = [w.HWND, c.c_uint, w.WPARAM, w.LPARAM, c.c_uint, c.c_uint, c.POINTER(c.c_size_t)]
callback = c.WINFUNCTYPE(w.BOOL, w.HWND, w.LPARAM)
windows = []

@callback
def top(hwnd, param):
    pid = w.DWORD()
    u.GetWindowThreadProcessId(hwnd, c.byref(pid))
    if pid.value in pids:
        windows.append(hwnd)
    return True

u.EnumWindows(top, 0)
result = []

def send(hwnd, message, wp=0, lp=0):
    out = c.c_size_t()
    assert u.SendMessageTimeoutW(hwnd, message, wp, lp, 2, 2000, c.byref(out))
    return out.value

def menus(menu):
    for i in range(u.GetMenuItemCount(menu)):
        text = c.create_unicode_buffer(256)
        u.GetMenuStringW(menu, i, text, 256, 0x400)
        label = text.value.replace('&', '')
        if any(word in label.lower() for word in ('option', 'настрой', 'tools', 'сервис')):
            result.append(dict(menu_label=label, command=u.GetMenuItemID(menu, i)))
        sub = u.GetSubMenu(menu, i)
        if sub: menus(sub)

@callback
def child(hwnd, param):
    cls, label = c.create_unicode_buffer(128), c.create_unicode_buffer(256)
    u.GetClassNameW(hwnd, cls, 128)
    # Never read edit fields, titles or account/password controls.
    if cls.value.lower() in ('button', 'systabcontrol32'):
        u.GetWindowTextW(hwnd, label, 256)
        result.append(dict(handle=hwnd, control=u.GetDlgCtrlID(hwnd), cls=cls.value, label=label.value))
    return True

mode = sys.argv[1] if len(sys.argv) > 1 else 'inspect'
if mode == 'native-tabs':
    handle = int(sys.argv[2])
    pid = w.DWORD()
    u.GetWindowThreadProcessId(handle, c.byref(pid))
    assert pid.value in pids
    k = c.windll.kernel32
    k.OpenProcess.restype = w.HANDLE
    k.VirtualAllocEx.restype = c.c_void_p
    k.VirtualAllocEx.argtypes = [w.HANDLE, c.c_void_p, c.c_size_t, w.DWORD, w.DWORD]
    k.WriteProcessMemory.argtypes = [w.HANDLE,c.c_void_p,c.c_void_p,c.c_size_t,c.c_void_p]
    k.ReadProcessMemory.argtypes = [w.HANDLE,c.c_void_p,c.c_void_p,c.c_size_t,c.c_void_p]
    k.VirtualFreeEx.argtypes = [w.HANDLE,c.c_void_p,c.c_size_t,w.DWORD]
    k.CloseHandle.argtypes = [w.HANDLE]
    class Tab(c.Structure):
        _fields_=[('mask',w.UINT),('state',w.DWORD),('stateMask',w.DWORD),
                  ('text',c.c_void_p),('length',c.c_int),('image',c.c_int),('param',w.LPARAM)]
    process = k.OpenProcess(0x38, False, pid.value)
    assert process
    memory = k.VirtualAllocEx(process,None,1024,0x3000,4)
    assert memory
    try:
        for index in range(send(handle,0x1304)):
            item=Tab(mask=1,text=memory+128,length=256)
            assert k.WriteProcessMemory(process,memory,c.byref(item),c.sizeof(item),None)
            send(handle,0x133C,index,memory)
            label=c.create_unicode_buffer(256)
            assert k.ReadProcessMemory(process,memory+128,label,512,None)
            result.append(dict(index=index,label=label.value))
    finally:
        k.VirtualFreeEx(process,memory,0,0x8000)
        k.CloseHandle(process)
elif mode == 'select-tab':
    handle,index=int(sys.argv[2]),int(sys.argv[3])
    pid=w.DWORD()
    u.GetWindowThreadProcessId(handle,c.byref(pid))
    assert pid.value in pids
    send(handle,0x1330,index)
elif mode in ('tabs', 'expert-tab'):
    pid = next(iter(pids))
    script = f'''Add-Type -AssemblyName UIAutomationClient
    $root=[System.Windows.Automation.AutomationElement]::RootElement
    $condition=New-Object System.Windows.Automation.PropertyCondition([System.Windows.Automation.AutomationElement]::ProcessIdProperty, {pid})
    $windows=$root.FindAll([System.Windows.Automation.TreeScope]::Children,$condition)
    $tabCondition=New-Object System.Windows.Automation.PropertyCondition([System.Windows.Automation.AutomationElement]::ControlTypeProperty,[System.Windows.Automation.ControlType]::TabItem)
    $names=@()
    foreach($window in $windows) {{
      foreach($tab in $window.FindAll([System.Windows.Automation.TreeScope]::Descendants,$tabCondition)) {{
        $name=$tab.Current.Name
        $names+=$name
        if ('{mode}' -eq 'expert-tab' -and ($name -eq 'Expert Advisors' -or $name -eq 'Советники')) {{
          $pattern=$tab.GetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern)
          $pattern.Select()
        }}
      }}
    }}
    ConvertTo-Json -InputObject $names -Compress'''
    p = subprocess.run(['powershell', '-NoProfile', '-Command', script], capture_output=True, timeout=20)
    result = dict(returncode=p.returncode, tabs=p.stdout.decode('cp866', errors='replace'))
elif mode == 'open':
    command = int(sys.argv[2])
    main = next(hwnd for hwnd in windows if u.GetMenu(hwnd))
    send(main, 0x111, command)
elif mode == 'disable-python':
    handle = int(sys.argv[2])
    pid = w.DWORD()
    u.GetWindowThreadProcessId(handle, c.byref(pid))
    assert pid.value in pids and u.GetDlgCtrlID(handle) == 11072
    before = send(handle, 0xF0)
    if before == 0:
        send(handle, 0xF5)
    after = send(handle, 0xF0)
    assert after == 1, "Python trade disable checkbox did not become checked"
    result = dict(python_trade_disable_before=before, python_trade_disable_after=after)
elif mode == 'click':
    handle = int(sys.argv[2])
    pid = w.DWORD()
    u.GetWindowThreadProcessId(handle, c.byref(pid))
    assert pid.value in pids
    send(handle, 0xF5)  # BM_CLICK on an observed control only
else:
    for hwnd in windows:
        menu = u.GetMenu(hwnd)
        if menu: menus(menu)
        u.EnumChildWindows(hwnd, child, 0)
(ROOT / 'history-ui.json').write_text(json.dumps(result, ensure_ascii=False), encoding='utf-8')
