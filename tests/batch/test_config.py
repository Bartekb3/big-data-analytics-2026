from datetime import timedelta

import pytest

from gametrend.batch.config import (DEFAULT_INTERVAL_S, DEFAULT_MAX_OFFSETS_PER_TRIGGER, DEFAULT_RECOMPUTE_HOURS,
                                    Settings, settings_from_env)

ENV = {"KAFKA_BOOTSTRAP": "kafka:9092", "MONGO_URI": "mongodb://m", "ARCHIVE_CHECKPOINT_DIR": "/c",
       "MASTER_DIR": "/m"}


def test_settings_from_env_with_defaults():
    s = settings_from_env(ENV)
    assert (s.kafka_bootstrap, s.mongo_uri, s.checkpoint_dir, s.master_dir) == ("kafka:9092", "mongodb://m", "/c", "/m")
    assert s.max_offsets_per_trigger == DEFAULT_MAX_OFFSETS_PER_TRIGGER
    assert s.interval_s == DEFAULT_INTERVAL_S == 3600
    assert s.recompute == timedelta(hours=DEFAULT_RECOMPUTE_HOURS)
    assert s.retention == timedelta(days=14)


def test_overrides():
    s = settings_from_env({**ENV, "BATCH_MAX_OFFSETS_PER_TRIGGER": "1000", "BATCH_INTERVAL_S": "60.5",
                           "BATCH_RECOMPUTE_HOURS": "0", "BATCH_RETENTION_HOURS": "48"})
    assert (s.max_offsets_per_trigger, s.interval_s, s.recompute_hours, s.retention_hours) == (1000, 60.5, 0, 48)


def test_empty_optional_variable_means_default():
    assert settings_from_env({**ENV, "BATCH_INTERVAL_S": ""}).interval_s == DEFAULT_INTERVAL_S


@pytest.mark.parametrize("missing", sorted(ENV))
def test_required_variables(missing):
    with pytest.raises(ValueError, match=missing):
        settings_from_env({k: v for k, v in ENV.items() if k != missing})


@pytest.mark.parametrize("field, value", [("max_offsets_per_trigger", 0), ("interval_s", 0),
                                          ("recompute_hours", -1), ("retention_hours", 24)])
def test_invalid_values(field, value):
    with pytest.raises(ValueError):
        Settings("k", "m", "/c", "/m", **{field: value})


def test_retention_minimum_is_allowed():
    assert Settings("k", "m", "/c", "/m", retention_hours=25).retention == timedelta(hours=25)
