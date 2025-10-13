from datetime import datetime
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from utils.config import TZ
from storage.db import get_conn

tz = pytz.timezone(TZ)

def setup_scheduler(dispatch_post_callable):
    sched = AsyncIOScheduler(timezone=tz)
    sched.start()
    with get_conn() as con:
        rows = con.execute("SELECT * FROM schedules WHERE status='scheduled'").fetchall()
        now = datetime.now(tz)
        for r in rows:
            dt = tz.localize(datetime.fromisoformat(r["run_at"]))
            if dt > now:
                sched.add_job(dispatch_post_callable, trigger=DateTrigger(run_date=dt), kwargs={"job_id": r["id"]})
            else:
                con.execute("UPDATE schedules SET status='failed' WHERE id=?", (r["id"],))
        con.commit()
    return sched

def add_schedule(sched, run_at_dt, dispatch_post_callable, job_id):
    trigger = DateTrigger(run_date=run_at_dt.astimezone(tz))
    sched.add_job(dispatch_post_callable, trigger=trigger, kwargs={"job_id": job_id})
