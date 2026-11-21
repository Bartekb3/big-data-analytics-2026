"""Settings of the batch service, read from the environment (set in docker-compose.yml)."""

import os
from dataclasses import dataclass
from datetime import timedelta

from ..common.retention import GAME_METRICS_RETENTION, MIN_RETENTION

DATABASE = "gametrend"
# Bounds one micro-batch of the sink: the first run after a long pause (up to the 72 h Kafka retention)
# is cut into several micro-batches instead of one task over millions of events. With coalesce(1) each
# micro-batch writes one file per (platform, date), so larger means fewer files.
DEFAULT_MAX_OFFSETS_PER_TRIGGER = 500_000
# Wall-clock pause between cycles (scheduling, not processing logic). For an accelerated replay set
# about 3600 / acceleration by hand (architecture/RUNNING.md).
DEFAULT_INTERVAL_S = 3600
# Hours recomputed behind the latest hourly aggregate, for late events (architecture/batch/README.md)
DEFAULT_RECOMPUTE_HOURS = 2
DEFAULT_RETENTION_HOURS = int(GAME_METRICS_RETENTION / timedelta(hours=1))


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    mongo_uri: str
    checkpoint_dir: str
    master_dir: str
    max_offsets_per_trigger: int = DEFAULT_MAX_OFFSETS_PER_TRIGGER
    interval_s: float = DEFAULT_INTERVAL_S
    recompute_hours: int = DEFAULT_RECOMPUTE_HOURS
    retention_hours: int = DEFAULT_RETENTION_HOURS
    database: str = DATABASE

    def __post_init__(self):
        if self.max_offsets_per_trigger < 1:
            raise ValueError(f"max_offsets_per_trigger must be >= 1, got {self.max_offsets_per_trigger}")
        if self.interval_s <= 0:
            raise ValueError(f"interval_s must be > 0, got {self.interval_s}")
        if self.recompute_hours < 0:
            raise ValueError(f"recompute_hours must be >= 0, got {self.recompute_hours}")
        if self.retention < MIN_RETENTION:
            raise ValueError(f"retention_hours {self.retention_hours} is below the minimum {MIN_RETENTION}")

    @property
    def recompute(self) -> timedelta:
        return timedelta(hours=self.recompute_hours)

    @property
    def retention(self) -> timedelta:
        return timedelta(hours=self.retention_hours)


def _required(env, name: str) -> str:
    value = env.get(name)
    if not value:
        raise ValueError(f"environment variable {name} is not set")
    return value


def _optional(env, name: str, default, convert=int):
    value = env.get(name)
    return convert(value) if value else default


def settings_from_env(env=None) -> Settings:
    env = os.environ if env is None else env
    return Settings(
        kafka_bootstrap=_required(env, "KAFKA_BOOTSTRAP"),
        mongo_uri=_required(env, "MONGO_URI"),
        checkpoint_dir=_required(env, "ARCHIVE_CHECKPOINT_DIR"),
        master_dir=_required(env, "MASTER_DIR"),
        max_offsets_per_trigger=_optional(env, "BATCH_MAX_OFFSETS_PER_TRIGGER", DEFAULT_MAX_OFFSETS_PER_TRIGGER),
        interval_s=_optional(env, "BATCH_INTERVAL_S", DEFAULT_INTERVAL_S, float),
        recompute_hours=_optional(env, "BATCH_RECOMPUTE_HOURS", DEFAULT_RECOMPUTE_HOURS),
        retention_hours=_optional(env, "BATCH_RETENTION_HOURS", DEFAULT_RETENTION_HOURS),
    )
