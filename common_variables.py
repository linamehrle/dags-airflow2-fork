import hashlib

from airflow.models import Variable

# Variables are set in the Airflow UI under Admin -> Variables
PATH_TO_CODE = Variable.get("PATH_TO_CODE")
PATH_TO_DATASETTE_FILES = Variable.get("PATH_TO_DATASETTE_FILES")
COMMON_ENV_VARS = {
    "https_proxy": Variable.get("https_proxy"),
    "http_proxy": Variable.get("http_proxy"),
    "no_proxy": Variable.get("no_proxy"),
    "EMAIL_RECEIVERS": Variable.get("EMAIL_RECEIVERS"),
    "EMAIL_SERVER": Variable.get("EMAIL_SERVER"),
    "EMAIL": Variable.get("EMAIL"),
    "FTP_SERVER": Variable.get("FTP_SERVER"),
    "FTP_USER": Variable.get("FTP_USER"),
    "FTP_PASS": Variable.get("FTP_PASS"),
    "ODS_API_KEY": Variable.get("ODS_API_KEY"),
    "HUWISE_API_KEY": Variable.get("HUWISE_API_KEY"),
}


def _stable_offset(key: str, modulus: int) -> int:
    """Deterministic pseudo-random offset in [0, modulus) derived from `key`.

    Same key always maps to the same offset (stable across deploys/restarts),
    but different keys spread roughly uniformly across the range.
    """
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % modulus


def hourly_schedule(dag_id: str, step_minutes: int = 60) -> str:
    """Cron expression that runs every `step_minutes` minutes (60 = once per hour),
    at a minute offset derived deterministically from `dag_id`.

    Use this instead of hardcoding "0 * * * *" or "*/15 * * * *" directly: many DAGs
    doing that independently all land on the same round minute (:00, :15, ...) and fire
    at once, causing load spikes. This spreads them out automatically without requiring
    every DAG author to hand-pick a free minute.

    `step_minutes` must evenly divide 60 (1, 2, 3, 5, 6, 10, 12, 15, 20, 30, 60).

    NOTE: this only spaces out *trigger times*, not actual run time overlap - it doesn't
    account for how long each DAG takes to run. See README.md "Scheduling & avoiding
    load spikes" for a runtime-aware approach if collisions are still a problem.
    """
    offset = _stable_offset(dag_id, step_minutes)
    if step_minutes >= 60:
        return f"{offset} * * * *"
    return f"{offset}/{step_minutes} * * * *"


def daily_schedule(dag_id: str, start_hour: int = 1, end_hour: int = 6) -> str:
    """Cron expression for a daily DAG, landing at a minute-of-day derived
    deterministically from `dag_id` within [start_hour, end_hour).

    Use this instead of hardcoding e.g. "0 4 * * *" directly: many daily DAGs doing
    that independently all land on the same round hour and fire at once. This spreads
    them across a configurable low-traffic window automatically.

    NOTE: like `hourly_schedule`, this only spaces out trigger times, not run time
    overlap. See README.md "Scheduling & avoiding load spikes" for a runtime-aware
    approach if collisions are still a problem.
    """
    window_minutes = (end_hour - start_hour) * 60
    offset = _stable_offset(dag_id, window_minutes)
    minute_of_day = start_hour * 60 + offset
    return f"{minute_of_day % 60} {minute_of_day // 60} * * *"
