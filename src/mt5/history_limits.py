"""OS memory fence for the history process, never a live/trading PID."""
import os


def apply_history_memory_limit(megabytes=1024):
    if os.name != "nt":
        raise RuntimeError("The native history service requires Windows")
    import ctypes as c
    from ctypes import wintypes as w
    class Basic(c.Structure):
        _fields_=[('process_time',c.c_longlong),('job_time',c.c_longlong),('flags',w.DWORD),
                  ('min_ws',c.c_size_t),('max_ws',c.c_size_t),('active',w.DWORD),
                  ('affinity',c.c_size_t),('priority',w.DWORD),('scheduling',w.DWORD)]
    class IO(c.Structure):
        _fields_=[(name,c.c_ulonglong) for name in ('read_ops','write_ops','other_ops','read_bytes','write_bytes','other_bytes')]
    class Extended(c.Structure):
        _fields_=[('basic',Basic),('io',IO),('process_memory',c.c_size_t),('job_memory',c.c_size_t),
                  ('peak_process',c.c_size_t),('peak_job',c.c_size_t)]
    k=c.WinDLL('kernel32',use_last_error=True)
    k.CreateJobObjectW.restype=w.HANDLE
    k.GetCurrentProcess.restype=w.HANDLE
    k.SetInformationJobObject.argtypes=[w.HANDLE,c.c_int,c.c_void_p,w.DWORD]
    k.AssignProcessToJobObject.argtypes=[w.HANDLE,w.HANDLE]
    job=k.CreateJobObjectW(None,None)
    limit=Extended()
    limit.basic.flags=0x100  # JOB_OBJECT_LIMIT_PROCESS_MEMORY; no kill-on-close
    limit.process_memory=megabytes*1024*1024
    if not job or not k.SetInformationJobObject(job,9,c.byref(limit),c.sizeof(limit)):
        raise c.WinError(c.get_last_error())
    if not k.AssignProcessToJobObject(job,k.GetCurrentProcess()):
        raise c.WinError(c.get_last_error())
    return job  # Keep alive for this process generation.
