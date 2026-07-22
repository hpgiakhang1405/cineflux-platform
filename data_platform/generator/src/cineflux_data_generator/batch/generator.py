"""Generate deterministic bootstrap and recurring batch source files.

Reads: validated shared and batch scenario configuration.
Writes: bounded record batches with stable MinIO object names and physical schemas.
Runs: the bootstrap and recurring batch runners.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast

import numpy as np
from faker import Faker

from cineflux_data_generator.batch.models import (
    ContentRecordV1,
    ContentRecordV2,
    PlaybackEventRecord,
    SubscriptionRecord,
    UserRecord,
)
from cineflux_data_generator.batch.problems import DuplicateInjector, WeightedCategorySampler
from cineflux_data_generator.config import BatchConfig, DeliveryConfig, SharedConfig
from cineflux_data_generator.ids import content_id, event_id, session_id, user_id
from cineflux_data_generator.playback import session_entities, stable_int


@dataclass(frozen=True)
class GeneratedObject:
    """One source object ready for Parquet serialization."""

    dataset: str
    object_name: str
    records: list[dict[str, Any]]


class BatchDataGenerator:
    """Generate source-aligned batch records without performing external I/O."""

    def __init__(self, shared: SharedConfig, batch: BatchConfig) -> None:
        """Create a deterministic generator for one batch scenario."""
        self._shared = shared
        self._batch = batch
        self._duplicates = DuplicateInjector(batch.duplicate_rate)
        self._city_sampler = WeightedCategorySampler(batch.city_distribution)
        self._genre_sampler = WeightedCategorySampler(batch.genre_distribution)

    def bootstrap_objects(self) -> Iterator[GeneratedObject]:
        """Yield partitioned historical playback objects strictly before cutover."""
        rng = np.random.default_rng(self._shared.seed + 100)
        start = self._shared.cutover_timestamp.astimezone(UTC) - timedelta(
            days=self._batch.history_days
        )
        quotient, remainder = divmod(
            self._batch.base_playback_event_count, self._batch.history_days
        )
        global_event_index = 0
        global_session_index = 0
        file_index = 0
        for day_offset in range(self._batch.history_days):
            day_count = quotient + (1 if day_offset < remainder else 0)
            day_start = start + timedelta(days=day_offset)
            day_event_index = 0
            remaining = day_count
            while remaining > 0:
                chunk_size = min(remaining, self._batch.rows_per_file)
                records = self._playback_records(
                    global_event_index,
                    global_session_index,
                    day_event_index,
                    chunk_size,
                    day_start,
                )
                records = self._duplicates.apply(records, rng)
                event_date = day_start.date().isoformat()
                object_name = (
                    f"{self._batch.bootstrap_prefix}/scenario={self._batch.scenario_name}/"
                    f"event_date={event_date}/part-{file_index:05d}.parquet"
                )
                yield GeneratedObject("initial_historical_playback_events", object_name, records)
                global_event_index += chunk_size
                day_event_index += chunk_size
                file_index += 1
                remaining -= chunk_size
            global_session_index += (day_count + 2) // 3

    def recurring_objects(self, delivery: DeliveryConfig) -> Iterator[GeneratedObject]:
        """Yield full user, subscription, and content snapshots for one delivery."""
        yield from self._user_objects(delivery)
        yield from self._subscription_objects(delivery)
        yield from self._content_objects(delivery)

    def _playback_records(
        self,
        start_event_index: int,
        start_session_index: int,
        day_event_index: int,
        count: int,
        day_start: datetime,
    ) -> list[dict[str, Any]]:
        """Generate coherent session events within one event-date partition."""
        event_types: tuple[
            Literal["playback_started"],
            Literal["playback_progressed"],
            Literal["playback_completed"],
        ] = (
            "playback_started",
            "playback_progressed",
            "playback_completed",
        )
        records: list[dict[str, Any]] = []
        for offset in range(count):
            event_index_value = start_event_index + offset
            day_index = day_event_index + offset
            session_index_value = start_session_index + day_index // 3
            phase = day_index % 3
            entities = session_entities(
                self._shared.seed,
                session_index_value,
                self._shared.user_count,
                self._shared.content_count,
            )
            session_start_second = stable_int(
                self._shared.seed, session_index_value, 72_000, salt=23
            )
            progress_offset = 60 + stable_int(
                self._shared.seed, session_index_value, 1_200, salt=29
            )
            completed_offset = progress_offset + 300 + stable_int(
                self._shared.seed, session_index_value, 3_600, salt=31
            )
            runtime_seconds = 4_800 + stable_int(
                self._shared.seed, session_index_value, 3_201, salt=37
            )
            event_type = event_types[phase]
            if phase == 0:
                event_offset = 0
                position_seconds = 0
                watch_seconds = 0
            elif phase == 1:
                event_offset = progress_offset
                position_seconds = min(progress_offset, runtime_seconds - 1)
                watch_seconds = position_seconds
            else:
                event_offset = completed_offset
                position_seconds = runtime_seconds
                watch_seconds = runtime_seconds
            timestamp = day_start + timedelta(
                seconds=session_start_second + event_offset
            )
            produced_delay = stable_int(
                self._shared.seed, event_index_value, 6, salt=41
            )
            record = PlaybackEventRecord(
                event_id=event_id(event_index_value),
                user_id=user_id(entities.user_index),
                content_id=content_id(entities.content_index),
                session_id=session_id(session_index_value),
                event_type=event_type,
                position_seconds=position_seconds,
                watch_seconds=watch_seconds,
                event_timestamp=timestamp,
                produced_timestamp=timestamp + timedelta(seconds=produced_delay),
                payload_schema_version=1,
            )
            records.append(record.model_dump(mode="python"))
        return records

    def _user_objects(self, delivery: DeliveryConfig) -> Iterator[GeneratedObject]:
        """Yield deterministic user snapshot files with configured city skew."""
        rng = np.random.default_rng(self._shared.seed + 200)
        change_rng = np.random.default_rng(self._shared.seed + 201 + delivery.schema_version)
        file_index = 0
        first_timestamp = min(item.delivery_timestamp for item in self._batch.deliveries)
        for start_index in range(0, self._shared.user_count, self._batch.rows_per_file):
            count = min(self._batch.rows_per_file, self._shared.user_count - start_index)
            cities = self._city_sampler.sample(count, rng)
            birth_years = rng.integers(1945, 2007, size=count)
            languages = rng.choice(np.array(["vi", "en", "ko", "ja"]), size=count)
            marketing = rng.random(count) < 0.55
            changed = change_rng.random(count) < self._batch.entity_change_rate
            records: list[dict[str, Any]] = []
            for offset in range(count):
                language = str(languages[offset])
                opt_in = bool(marketing[offset])
                updated = first_timestamp
                if delivery.schema_version > 1 and bool(changed[offset]):
                    language = "en" if language == "vi" else language
                    opt_in = not opt_in
                    updated = delivery.delivery_timestamp
                record = UserRecord(
                    user_id=user_id(start_index + offset),
                    country_code="VN",
                    city=str(cities[offset]),
                    birth_year=int(birth_years[offset]),
                    preferred_language=language,
                    marketing_opt_in=opt_in,
                    source_updated_timestamp=updated,
                )
                records.append(record.model_dump(mode="python"))
            records = self._duplicates.apply(records, change_rng)
            yield GeneratedObject(
                "users", self._recurring_object_name("users", delivery, file_index), records
            )
            file_index += 1

    def _subscription_objects(self, delivery: DeliveryConfig) -> Iterator[GeneratedObject]:
        """Yield deterministic subscription snapshot files."""
        rng = np.random.default_rng(self._shared.seed + 300)
        change_rng = np.random.default_rng(self._shared.seed + 301 + delivery.schema_version)
        file_index = 0
        first_timestamp = min(item.delivery_timestamp for item in self._batch.deliveries)
        cutover_date = self._shared.cutover_timestamp.date()
        tiers = np.array(["basic", "standard", "premium"])
        for start_index in range(0, self._shared.user_count, self._batch.rows_per_file):
            count = min(self._batch.rows_per_file, self._shared.user_count - start_index)
            base_tiers = rng.choice(tiers, size=count, p=np.array([0.45, 0.35, 0.20]))
            started_days = rng.integers(1, 1_096, size=count)
            changed = change_rng.random(count) < self._batch.entity_change_rate
            records: list[dict[str, Any]] = []
            for offset in range(count):
                tier = cast(Literal["basic", "standard", "premium"], str(base_tiers[offset]))
                updated = first_timestamp
                if delivery.schema_version > 1 and bool(changed[offset]):
                    tier = cast(
                        Literal["basic", "standard", "premium"],
                        {
                            "basic": "standard",
                            "standard": "premium",
                            "premium": "premium",
                        }[tier],
                    )
                    updated = delivery.delivery_timestamp
                record = SubscriptionRecord(
                    user_id=user_id(start_index + offset),
                    subscription_tier=tier,
                    subscription_status="active",
                    billing_country_code="VN",
                    started_date=cutover_date - timedelta(days=int(started_days[offset])),
                    ended_date=None,
                    source_updated_timestamp=updated,
                )
                records.append(record.model_dump(mode="python"))
            records = self._duplicates.apply(records, change_rng)
            yield GeneratedObject(
                "subscriptions",
                self._recurring_object_name("subscriptions", delivery, file_index),
                records,
            )
            file_index += 1

    def _content_objects(self, delivery: DeliveryConfig) -> Iterator[GeneratedObject]:
        """Yield content snapshots with genre skew and physical schema evolution."""
        rng = np.random.default_rng(self._shared.seed + 400)
        change_rng = np.random.default_rng(self._shared.seed + 401 + delivery.schema_version)
        fake = Faker("en_US")
        fake.seed_instance(self._shared.seed + 400)
        file_index = 0
        first_timestamp = min(item.delivery_timestamp for item in self._batch.deliveries)
        genre_names = tuple(self._batch.genre_distribution.keys())
        for start_index in range(0, self._shared.content_count, self._batch.rows_per_file):
            count = min(self._batch.rows_per_file, self._shared.content_count - start_index)
            primary_genres = self._genre_sampler.sample(count, rng)
            content_types = rng.choice(
                np.array(["movie", "series", "documentary"]),
                size=count,
                p=np.array([0.70, 0.20, 0.10]),
            )
            release_years = rng.integers(1950, 2026, size=count)
            runtimes = rng.integers(45, 181, size=count)
            changed = change_rng.random(count) < self._batch.entity_change_rate
            has_score = change_rng.random(count) < self._batch.schema_evolution.non_null_rate
            scores = change_rng.uniform(35.0, 98.0, size=count)
            records: list[dict[str, Any]] = []
            for offset in range(count):
                primary_genre = str(primary_genres[offset])
                secondary_genre = genre_names[
                    (genre_names.index(primary_genre) + 1) % len(genre_names)
                ]
                updated = first_timestamp
                availability = "available"
                if delivery.schema_version > 1 and bool(changed[offset]):
                    availability = "unavailable" if offset % 5 == 0 else "available"
                    updated = delivery.delivery_timestamp
                values: dict[str, Any] = {
                    "content_id": content_id(start_index + offset),
                    "content_type": str(content_types[offset]),
                    "title": f"{fake.catch_phrase()} {start_index + offset + 1}",
                    "genres": [primary_genre, secondary_genre],
                    "release_year": int(release_years[offset]),
                    "runtime_minutes": int(runtimes[offset]),
                    "maturity_rating": str(rng.choice(np.array(["G", "PG", "PG-13", "R"]))),
                    "original_language": str(rng.choice(np.array(["vi", "en", "ko", "ja"]))),
                    "availability_status": availability,
                    "source_updated_timestamp": updated,
                }
                if (
                    delivery.schema_version
                    >= self._batch.schema_evolution.introduced_schema_version
                ):
                    values["critic_score"] = (
                        round(float(scores[offset]), 2) if has_score[offset] else None
                    )
                    record_values = ContentRecordV2.model_validate(values).model_dump(mode="python")
                else:
                    record_values = ContentRecordV1.model_validate(values).model_dump(mode="python")
                records.append(record_values)
            records = self._duplicates.apply(records, change_rng)
            yield GeneratedObject(
                "content", self._recurring_object_name("content", delivery, file_index), records
            )
            file_index += 1

    def _recurring_object_name(
        self, dataset: str, delivery: DeliveryConfig, file_index: int
    ) -> str:
        """Build a stable object name for one recurring snapshot file."""
        delivery_date = delivery.delivery_timestamp.date().isoformat()
        return (
            f"{self._batch.recurring_prefix}/{dataset}/scenario={self._batch.scenario_name}/"
            f"delivery_date={delivery_date}/schema_version={delivery.schema_version}/"
            f"part-{file_index:05d}.parquet"
        )
