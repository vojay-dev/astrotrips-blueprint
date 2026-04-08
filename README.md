# AstroTrips Blueprint Example

_todo: add blog post link once it is published._

## The scenario

AstroTrips is a fictional interplanetary travel company. Customers book trips to the Moon, Mars, and Europa. Each booking generates a payment record calculated from a base fare, a planet-specific cost multiplier, and an optional promo code discount. Every day, a pipeline ingests new bookings, aggregates revenue into a daily planet report, and validates data quality. A separate pipeline fetches weather conditions per planet from an external API.

The same scenario powers [Astronomer's public Airflow workshops](https://github.com/astronomer/devrel-public-workshops), where the full pipeline is written as traditional Python Dags. This project rebuilds it with [Blueprint](https://github.com/astronomer/blueprint), showing how platform teams can create reusable building blocks that others compose via YAML or the [Astro IDE](https://www.astronomer.io/product/ide/) visual builder.

## Blueprints

All blueprints are defined in `dags/blueprints.py`.

| Blueprint | Version(s) | Returns | What it does |
|-----------|-----------|---------|-------------|
| **SetupDatabase** | v1 | TaskGroup | Drops and recreates all AstroTrips tables. Optionally seeds sample data (planets, routes, customers, promo codes, bookings, payments). |
| **IngestBookings** | v1 | single task | Generates N random bookings with matching payments via a Jinja2 SQL template. Configurable `n_bookings` (1 to 100). |
| **PlanetReport** | v1 | single task | Aggregates bookings into a `daily_planet_report` table using an upsert pattern. Calculates passengers, active/completed trips, gross and net revenue. |
| **DataQualityCheck** | v1 | single task | Runs column-level checks (null, distinct, min, max) on any table. Uses `extra="forbid"` to catch YAML typos, `Literal` types for check types, and a custom `@field_validator` to require at least one check. |
| **WeatherIngest** | v1 | TaskGroup | Fetches weather from a fake API for all planets using dynamic task mapping (`.expand()`), then loads results into `planet_weather`. |
| **WeatherIngestV2** | v2 | TaskGroup | Same as v1, but adds a `max_storm_risk` threshold (0.0 to 1.0) to filter out high-risk readings before loading. |

A project-level `AstroTripsDagArgs` class sets `template_searchpath` so all Dags can find the SQL files in `include/sql/`.

## Dag compositions

All Dag YAML files are in `dags/`.

| File | Dag ID | Schedule | Steps |
|------|--------|----------|-------|
| `setup.dag.yaml` | `astrotrips_setup` | `@once` | `init_database` (SetupDatabase with seed data) |
| `daily_pipeline.dag.yaml` | `astrotrips_daily_pipeline` | `@daily` | `ingest` > `report` > `validate` |
| `weather.dag.yaml` | `astrotrips_weather` | `@daily` | `weather` (WeatherIngestV2, max_storm_risk=0.8) |

## Project structure

```
dags/
  blueprints.py              # Blueprint definitions (platform team writes this)
  loader.py                  # Two-line build_all() entry point
  setup.dag.yaml             # One-time database initialization
  daily_pipeline.dag.yaml    # Daily ingest > report > validate
  weather.dag.yaml           # Daily weather ingestion (v2 with filtering)
include/
  sql/
    schema.sql               # Table and sequence definitions
    fixtures.sql             # Sample data (planets, routes, customers, etc.)
    generate.sql             # Jinja2 template for random booking generation
    report.sql               # Aggregation query with upsert pattern
    cleanup.sql              # Drop all tables and sequences
  weather_api.py             # Deterministic fake weather API (hash-seeded RNG)
blueprint/
  generated-schemas/         # JSON schemas for Astro IDE (generated via CLI)
Makefile                     # Convenience commands (run inside scheduler container)
```

## Getting started

Prerequisites: [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) and Docker.

```bash
# Start Airflow
astro dev start

# Open the Airflow UI
open http://localhost:8080

# Trigger the setup Dag first, then the daily pipeline or weather Dag
```

## Using the Blueprint CLI

All `blueprint` commands run inside the scheduler container. The Makefile handles this for you:

```bash
make list                              # List all blueprints
make describe NAME=data_quality_check  # Show a blueprint's schema
make lint                              # Validate all Dag YAMLs
make schemas                           # Generate JSON schemas for Astro IDE
```

Or run directly:

```bash
docker exec -it <scheduler-container> blueprint list
docker exec -it <scheduler-container> blueprint describe weather_ingest
docker exec -it <scheduler-container> blueprint lint dags/daily_pipeline.dag.yaml
```

## Publishing blueprints to the Astro IDE

Blueprints do not appear in the Astro IDE visual builder automatically. You publish them intentionally by generating JSON schemas:

```bash
make schemas
```

This creates one `.schema.json` file per blueprint in `blueprint/generated-schemas/`. Commit these to Git, and the Astro IDE discovers them. This is a governance feature: you control which building blocks are visible to IDE users.

## Related

- [Blueprint OSS](https://github.com/astronomer/blueprint) (GitHub)
- [Part 1: The Rise of Abstraction in Data Orchestration](https://www.astronomer.io/blog/the-rise-of-abstraction-in-data-orchestration/)
- [Part 2: Abstraction with DAG Factory: From Excel to Minecraft](https://www.astronomer.io/blog/abstraction-with-dag-factory-from-excel-to-minecraft/)
- _todo: add blog post link once it is published._
- [Astronomer's public Airflow workshops](https://github.com/astronomer/devrel-public-workshops) (same scenario, Python Dags)
