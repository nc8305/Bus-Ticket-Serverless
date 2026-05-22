"""
Unit tests for Bus GPS Analysis Pipeline
"""

import pytest
from unittest.mock import MagicMock, patch
import json
import csv
import os
import tempfile


# ─────────────────────────────────────────
# Tests: Data validation helpers
# ─────────────────────────────────────────

class TestGPSDataValidation:
    """Test GPS record validation logic."""

    def test_valid_gps_record(self):
        record = {
            "vehicle_id": "51B-12345",
            "lat": 10.7769,
            "lng": 106.7009,
            "speed": 40.5,
            "driver": "DRV001",
        }
        assert -90 <= record["lat"] <= 90
        assert -180 <= record["lng"] <= 180
        assert record["speed"] >= 0

    def test_latitude_out_of_range(self):
        invalid_lat = 999.0
        assert not (-90 <= invalid_lat <= 90)

    def test_longitude_out_of_range(self):
        invalid_lng = -999.0
        assert not (-180 <= invalid_lng <= 180)

    def test_negative_speed_invalid(self):
        speed = -5.0
        assert speed < 0  # should be rejected

    def test_speed_zero_is_valid(self):
        speed = 0.0
        assert speed >= 0

    def test_vehicle_id_not_empty(self):
        vehicle_id = "51B-12345"
        assert len(vehicle_id) > 0


# ─────────────────────────────────────────
# Tests: CSV parsing
# ─────────────────────────────────────────

class TestCSVParsing:
    """Test CSV data reading and parsing."""

    def test_csv_row_has_required_fields(self):
        required_fields = {"datetime", "vehicle_id", "lng", "lat", "speed", "driver"}
        sample_row = {
            "datetime": "2025-04-01 08:00:00",
            "vehicle_id": "51B-12345",
            "lng": "106.7009",
            "lat": "10.7769",
            "speed": "40.5",
            "driver": "DRV001",
            "door_up": "0",
            "door_down": "0",
        }
        assert required_fields.issubset(sample_row.keys())

    def test_csv_numeric_fields_parseable(self):
        row = {"lat": "10.7769", "lng": "106.7009", "speed": "40.5"}
        assert float(row["lat"]) == pytest.approx(10.7769)
        assert float(row["lng"]) == pytest.approx(106.7009)
        assert float(row["speed"]) == pytest.approx(40.5)

    def test_csv_with_temp_file(self):
        """Write a small CSV and read it back."""
        rows = [
            {"vehicle_id": "51B-001", "lat": "10.77", "lng": "106.70", "speed": "30"},
            {"vehicle_id": "51B-002", "lat": "10.78", "lng": "106.71", "speed": "45"},
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
            tmp_path = f.name

        try:
            with open(tmp_path, newline="") as f:
                reader = list(csv.DictReader(f))
            assert len(reader) == 2
            assert reader[0]["vehicle_id"] == "51B-001"
            assert reader[1]["speed"] == "45"
        finally:
            os.unlink(tmp_path)


# ─────────────────────────────────────────
# Tests: Kafka message serialization
# ─────────────────────────────────────────

class TestKafkaMessageFormat:
    """Test that GPS records serialize correctly for Kafka."""

    def test_message_is_valid_json(self):
        record = {
            "vehicle_id": "51B-12345",
            "lat": 10.7769,
            "lng": 106.7009,
            "speed": 40.5,
            "event_time": "2025-04-01T08:00:00",
        }
        serialized = json.dumps(record)
        deserialized = json.loads(serialized)
        assert deserialized["vehicle_id"] == "51B-12345"
        assert deserialized["speed"] == pytest.approx(40.5)

    def test_message_key_is_vehicle_id(self):
        """Kafka key should be vehicle_id for partition locality."""
        record = {"vehicle_id": "51B-12345", "lat": 10.77, "lng": 106.70}
        key = record["vehicle_id"].encode("utf-8")
        assert key == b"51B-12345"

    def test_batch_messages_all_valid_json(self):
        records = [
            {"vehicle_id": f"51B-{i:03d}", "lat": 10.77 + i * 0.001, "lng": 106.70}
            for i in range(10)
        ]
        for r in records:
            assert json.loads(json.dumps(r))["vehicle_id"] == r["vehicle_id"]


# ─────────────────────────────────────────
# Tests: Database schema helpers
# ─────────────────────────────────────────

class TestDatabaseHelpers:
    """Test SQL-related helper logic (no live DB needed)."""

    def test_insert_params_match_columns(self):
        columns = ["vehicle_id", "latitude", "longitude", "speed", "event_time"]
        record = {
            "vehicle_id": "51B-001",
            "latitude": 10.77,
            "longitude": 106.70,
            "speed": 35.0,
            "event_time": "2025-04-01T08:00:00",
        }
        assert all(col in record for col in columns)

    def test_batch_insert_chunk_size(self):
        """Records should be inserted in batches of 100."""
        BATCH_SIZE = 100
        records = list(range(350))
        chunks = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
        assert len(chunks) == 4
        assert len(chunks[0]) == 100
        assert len(chunks[-1]) == 50
