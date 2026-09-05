"""Startup diagnostic while live Poller still owns history; cannot consume jobs."""
import asyncio,json,os,sys,traceback
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
os.environ['HISTORY_WORKER_ENABLED']='true'

async def guard():
    from src.redis_bus.pool import get_redis_pool,close_redis_pool
    raw=await get_redis_pool().get('poller:status')
    assert raw and json.loads(raw).get('history_isolated') is not True
    await close_redis_pool()

try:
    asyncio.run(guard())
    if '--without-memory-fence' in sys.argv:
        import src.mt5.history_limits as limits
        limits.apply_history_memory_limit=lambda: None
    from src.history_main import main
    main()
except BaseException as exc:
    data={'error_type':type(exc).__name__,
          'message':str(exc) if isinstance(exc,(OSError,RuntimeError)) else None,
          'frames':[(Path(f.filename).name,f.lineno,f.name) for f in traceback.extract_tb(exc.__traceback__)]}
    (ROOT/'history-startup-proof.json').write_text(json.dumps(data),encoding='utf-8')
