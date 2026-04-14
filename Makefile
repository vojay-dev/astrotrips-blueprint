# AstroTrips Blueprint Example
# All blueprint commands run inside the Airflow scheduler container
# so no local Python/Blueprint installation is needed.

SCHEDULER := $(shell docker ps --filter "name=scheduler" --format "{{.Names}}" | head -1)
BLUEPRINT := docker exec $(SCHEDULER) blueprint
DAGS_DIR := dags
SCHEMA_DIR := blueprint/generated-schemas

.PHONY: help list describe lint schemas clean-schemas start stop restart logs

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --- Airflow lifecycle ---

start: ## Start the Astro project
	astro dev start

stop: ## Stop the Astro project
	astro dev stop

restart: ## Restart the Astro project
	astro dev restart

logs: ## Tail scheduler logs
	astro dev logs -f scheduler

# --- Blueprint discovery ---

list: ## List all available blueprints
	$(BLUEPRINT) list

describe: ## Describe a blueprint (usage: make describe NAME=ingest_bookings)
	$(BLUEPRINT) describe $(NAME)

# --- Validation ---

lint: ## Lint all Dag YAML files
	@for f in $(DAGS_DIR)/*.dag.yaml; do \
		$(BLUEPRINT) lint $$f; \
	done

# --- Schema generation for Astro IDE ---

schemas: ## Generate all JSON schemas for Astro IDE
	@mkdir -p $(SCHEMA_DIR)
	@for bp in setup_database ingest_bookings planet_report data_quality_check weather_ingest; do \
		$(BLUEPRINT) schema $$bp -o /tmp/$$bp.schema.json; \
		docker cp $(SCHEDULER):/tmp/$$bp.schema.json $(SCHEMA_DIR)/; \
	done
	@echo "Schemas written to $(SCHEMA_DIR)/"

clean-schemas: ## Remove generated schemas
	rm -rf $(SCHEMA_DIR)
