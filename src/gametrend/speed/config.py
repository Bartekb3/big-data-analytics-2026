"""Settings of the speed job, read from the environment (set in docker-compose.yml)."""

import os
from dataclasses import dataclass

DATABASE = "gametrend"
DEFAULT_MAX_OFFSETS_PER_TRIGGER = 50_000  # bounds the micro-batch collected on the driver (architecture/speed/README.md)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    mongo_uri: str
    checkpoint_dir: str
    max_offsets_per_trigger: int = DEFAULT_MAX_OFFSETS_PER_TRIGGER
    database: str = DATABASE

    def __post_init__(self):
        if self.max_offsets_per_trigger < 1:
            raise ValueError(f"max_offsets_per_trigger must be >= 1, got {self.max_offsets_per_trigger}")


def _required(env, name: str) -> str:
    value = env.get(name)
    if not value:
        raise ValueError(f"environment variable {name} is not set")
    return value


def settings_from_env(env=None) -> Settings:
    env = os.environ if env is None else env
    max_offsets = env.get("SPEED_MAX_OFFSETS_PER_TRIGGER")
    return Settings(
        kafka_bootstrap=_required(env, "KAFKA_BOOTSTRAP"),
        mongo_uri=_required(env, "MONGO_URI"),
        checkpoint_dir=_required(env, "SPEED_CHECKPOINT_DIR"),
        max_offsets_per_trigger=int(max_offsets) if max_offsets else DEFAULT_MAX_OFFSETS_PER_TRIGGER,
    )
