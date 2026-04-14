"""AstroTrips Blueprint definitions.

Reusable building blocks for the AstroTrips interplanetary travel data platform.
Platform teams define these; analysts and data scientists compose them via YAML.
"""

import os

from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.sdk import TaskGroup, task

from blueprint import BaseModel, Blueprint, BlueprintDagArgs, ConfigDict, Field, TaskOrGroup, field_validator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")


# ---------------------------------------------------------------------------
# Dag args: set template_searchpath so SQL files in include/ are found
# ---------------------------------------------------------------------------


class AstroTripsDagArgsConfig(BaseModel):
    schedule: str | None = None
    description: str | None = None


class AstroTripsDagArgs(BlueprintDagArgs[AstroTripsDagArgsConfig]):
    """Dag-level arguments for AstroTrips pipelines. Sets template_searchpath to include/sql."""

    def render(self, config: AstroTripsDagArgsConfig) -> dict:
        kwargs: dict = {
            "template_searchpath": [f"{AIRFLOW_HOME}/include/sql"],
        }
        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        if config.description is not None:
            kwargs["description"] = config.description
        return kwargs


# ---------------------------------------------------------------------------
# Setup: initialize or reset the AstroTrips database
# ---------------------------------------------------------------------------


class SetupDatabaseConfig(BaseModel):
    """Configuration for database setup."""

    conn_id: str = Field(description="Airflow connection ID for DuckDB")
    seed_data: bool = Field(default=True, description="Load sample fixture data after creating schema")


class SetupDatabase(Blueprint[SetupDatabaseConfig]):
    """Create tables, sequences, and optionally seed the AstroTrips database."""

    def render(self, config: SetupDatabaseConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            cleanup = SQLExecuteQueryOperator(
                task_id="cleanup",
                conn_id=config.conn_id,
                sql="cleanup.sql",
            )
            schema = SQLExecuteQueryOperator(
                task_id="schema",
                conn_id=config.conn_id,
                sql="schema.sql",
            )
            cleanup >> schema

            if config.seed_data:
                fixtures = SQLExecuteQueryOperator(
                    task_id="fixtures",
                    conn_id=config.conn_id,
                    sql="fixtures.sql",
                )
                schema >> fixtures

        return group


# ---------------------------------------------------------------------------
# Ingest: generate random booking data
# ---------------------------------------------------------------------------


class IngestBookingsConfig(BaseModel):
    """Configuration for booking data ingestion."""

    conn_id: str = Field(description="Airflow connection ID for DuckDB")
    n_bookings: int = Field(default=5, ge=1, le=100, description="Number of random bookings to generate per run")


class IngestBookings(Blueprint[IngestBookingsConfig]):
    """Generate random booking and payment records for a given execution date."""

    def render(self, config: IngestBookingsConfig) -> TaskOrGroup:
        return SQLExecuteQueryOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql="generate.sql",
            params={"n_bookings": config.n_bookings},
        )


# ---------------------------------------------------------------------------
# Report: aggregate bookings into daily planet report
# ---------------------------------------------------------------------------


class PlanetReportConfig(BaseModel):
    """Configuration for the daily planet report generator."""

    conn_id: str = Field(description="Airflow connection ID for DuckDB")


class PlanetReport(Blueprint[PlanetReportConfig]):
    """Aggregate bookings into the daily_planet_report table using an upsert pattern."""

    def render(self, config: PlanetReportConfig) -> TaskOrGroup:
        return SQLExecuteQueryOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            sql="report.sql",
            parameters={"reportDate": "{{ ds }}"},
        )


# ---------------------------------------------------------------------------
# Data quality: validate table columns
# ---------------------------------------------------------------------------


class DataQualityCheckConfig(BaseModel):
    """Configuration for data quality validation."""

    model_config = ConfigDict(extra="forbid")

    conn_id: str = Field(description="Airflow connection ID for DuckDB")
    table: str = Field(description="Table name to validate")
    not_null_columns: list[str] = Field(default=[], description="Columns that must contain no null values")
    min_distinct_columns: list[str] = Field(default=[], description="Columns to check for minimum distinct value count (parallel to min_distinct_counts)")
    min_distinct_counts: list[int] = Field(default=[], description="Required minimum distinct count per column (parallel to min_distinct_columns)")
    min_value_columns: list[str] = Field(default=[], description="Columns to check for a minimum numeric value (parallel to min_values)")
    min_values: list[int] = Field(default=[], description="Minimum allowed value per column (parallel to min_value_columns)")

    @field_validator("min_distinct_counts")
    @classmethod
    def distinct_lengths_match(cls, v: list[int], info) -> list[int]:
        cols = info.data.get("min_distinct_columns", [])
        if len(v) != len(cols):
            raise ValueError("min_distinct_counts must have the same length as min_distinct_columns")
        return v

    @field_validator("min_values")
    @classmethod
    def min_value_lengths_match(cls, v: list[int], info) -> list[int]:
        cols = info.data.get("min_value_columns", [])
        if len(v) != len(cols):
            raise ValueError("min_values must have the same length as min_value_columns")
        return v


class DataQualityCheck(Blueprint[DataQualityCheckConfig]):
    """Run column-level data quality checks on a table."""

    def render(self, config: DataQualityCheckConfig) -> TaskOrGroup:
        column_mapping: dict = {}

        for column in config.not_null_columns:
            column_mapping.setdefault(column, {})["null_check"] = {"equal_to": 0}

        for column, count in zip(config.min_distinct_columns, config.min_distinct_counts):
            column_mapping.setdefault(column, {})["distinct_check"] = {"geq_to": count}

        for column, value in zip(config.min_value_columns, config.min_values):
            column_mapping.setdefault(column, {})["min"] = {"geq_to": value}

        return SQLColumnCheckOperator(
            task_id=self.step_id,
            conn_id=config.conn_id,
            table=config.table,
            column_mapping=column_mapping,
        )


# ---------------------------------------------------------------------------
# Weather: fetch and load planet weather data (v1 and v2)
# ---------------------------------------------------------------------------


class WeatherIngestConfig(BaseModel):
    """Configuration for weather data ingestion."""

    conn_id: str = Field(description="Airflow connection ID for DuckDB")


class WeatherIngest(Blueprint[WeatherIngestConfig]):
    """Fetch weather data for all planets from the API and load into planet_weather."""

    def render(self, config: WeatherIngestConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:

            get_planets = SQLExecuteQueryOperator(
                task_id="get_planets",
                conn_id=config.conn_id,
                sql="SELECT DISTINCT planet_id FROM planets",
            )

            @task(task_id="extract_planet_ids")
            def extract_planet_ids(query_result):
                return [row[0] for row in query_result]

            @task(task_id="fetch_weather")
            def fetch_weather(planet_id: int, logical_date=None):
                from include.weather_api import get_planet_weather

                return get_planet_weather(planet_id, logical_date.date())

            @task(task_id="load_weather")
            def load_weather(weather_data: list[dict], logical_date=None):
                from airflow.hooks.base import BaseHook

                conn = BaseHook.get_connection(config.conn_id)
                import duckdb

                db = duckdb.connect(conn.host)
                ds = logical_date.date().isoformat()
                db.execute(f"DELETE FROM planet_weather WHERE reading_date = '{ds}'")
                for w in weather_data:
                    db.execute(
                        "INSERT INTO planet_weather VALUES (?, ?, ?, ?, ?)",
                        [w["planet_id"], w["reading_date"], w["temperature_c"], w["storm_risk"], w["visibility"]],
                    )
                db.close()

            planet_ids = extract_planet_ids(get_planets.output)
            weather_data = fetch_weather.expand(planet_id=planet_ids)
            load_weather(weather_data)

        return group


# ---------------------------------------------------------------------------
# Weather v2: adds storm risk filtering
# ---------------------------------------------------------------------------


class WeatherIngestV2Config(BaseModel):
    """Configuration for weather data ingestion with storm risk filtering."""

    conn_id: str = Field(description="Airflow connection ID for DuckDB")
    max_storm_risk: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Only load weather readings with storm_risk at or below this threshold",
    )


class WeatherIngestV2(Blueprint[WeatherIngestV2Config]):
    """Fetch weather for all planets with optional storm risk filtering. V2 adds threshold control."""

    def render(self, config: WeatherIngestV2Config) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:

            get_planets = SQLExecuteQueryOperator(
                task_id="get_planets",
                conn_id=config.conn_id,
                sql="SELECT DISTINCT planet_id FROM planets",
            )

            @task(task_id="extract_planet_ids")
            def extract_planet_ids(query_result):
                return [row[0] for row in query_result]

            @task(task_id="fetch_weather")
            def fetch_weather(planet_id: int, logical_date=None):
                from include.weather_api import get_planet_weather

                return get_planet_weather(planet_id, logical_date.date())

            @task(task_id="load_weather")
            def load_weather(weather_data: list[dict], logical_date=None):
                from airflow.hooks.base import BaseHook

                threshold = config.max_storm_risk
                filtered = [w for w in weather_data if w["storm_risk"] <= threshold]

                conn = BaseHook.get_connection(config.conn_id)
                import duckdb

                db = duckdb.connect(conn.host)
                ds = logical_date.date().isoformat()
                db.execute(f"DELETE FROM planet_weather WHERE reading_date = '{ds}'")
                for w in filtered:
                    db.execute(
                        "INSERT INTO planet_weather VALUES (?, ?, ?, ?, ?)",
                        [w["planet_id"], w["reading_date"], w["temperature_c"], w["storm_risk"], w["visibility"]],
                    )
                db.close()

            planet_ids = extract_planet_ids(get_planets.output)
            weather_data = fetch_weather.expand(planet_id=planet_ids)
            load_weather(weather_data)

        return group
