SHELL := /bin/sh

COMPOSE := docker compose
TERRAFORM_DIR := terraform
DAGSTER_SERVICE := dagster
CLICKHOUSE_SERVICE := clickhouse
DBT_PROJECT_DIR := /opt/dbt_project
DBT_PROFILES_DIR := /opt/dbt_project
ASSETS := raw_airports,raw_flights,raw_delay_causes,dbt_build

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available make targets.
	@awk 'BEGIN {FS = ":.*##"; printf "\nAeroPulse commands:\n\n"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: compose-config
compose-config: ## Validate and render docker-compose.yml.
	$(COMPOSE) config

.PHONY: up
up: ## Build and start ClickHouse, Dagster, and Metabase.
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop and remove the local stack.
	$(COMPOSE) down

.PHONY: clean
clean: clean-python clean-dbt ## Remove local generated Python and dbt artifacts.

.PHONY: clean-python
clean-python: ## Remove Python bytecode caches from application code.
	find dagster/app -type d -name __pycache__ -prune -exec rm -rf {} +

.PHONY: clean-dbt
clean-dbt: ## Remove local dbt build artifacts and logs.
	rm -rf dbt_project/target dbt_project/dbt_packages dbt_project/logs

.PHONY: restart
restart: down up ## Restart the local stack.

.PHONY: ps
ps: ## Show running service status.
	$(COMPOSE) ps

.PHONY: logs
logs: ## Follow logs for all services.
	$(COMPOSE) logs -f

.PHONY: dagster-logs
dagster-logs: ## Follow Dagster logs.
	$(COMPOSE) logs -f $(DAGSTER_SERVICE)

.PHONY: metabase-logs
metabase-logs: ## Follow Metabase logs.
	$(COMPOSE) logs -f metabase

.PHONY: clickhouse-logs
clickhouse-logs: ## Follow ClickHouse logs.
	$(COMPOSE) logs -f $(CLICKHOUSE_SERVICE)

.PHONY: terraform-init
terraform-init: ## Initialize Terraform providers.
	terraform -chdir=$(TERRAFORM_DIR) init

.PHONY: terraform-fmt
terraform-fmt: ## Format Terraform files.
	terraform -chdir=$(TERRAFORM_DIR) fmt

.PHONY: terraform-validate
terraform-validate: ## Validate Terraform configuration.
	terraform -chdir=$(TERRAFORM_DIR) validate

.PHONY: terraform-plan
terraform-plan: ## Plan Terraform changes.
	terraform -chdir=$(TERRAFORM_DIR) plan -input=false

.PHONY: terraform-apply
terraform-apply: ## Apply Terraform changes.
	terraform -chdir=$(TERRAFORM_DIR) apply -input=false

.PHONY: terraform-deploy
terraform-deploy: ## Plan, then apply Terraform changes.
	terraform -chdir=$(TERRAFORM_DIR) plan -input=false
	terraform -chdir=$(TERRAFORM_DIR) apply -input=false

.PHONY: terraform-output
terraform-output: ## Show Terraform outputs.
	terraform -chdir=$(TERRAFORM_DIR) output

.PHONY: dbt-debug
dbt-debug: ## Validate dbt profile and ClickHouse connection.
	$(COMPOSE) exec -T $(DAGSTER_SERVICE) uv run dbt debug --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

.PHONY: dbt-build
dbt-build: ## Run dbt build inside the Dagster container.
	$(COMPOSE) exec -T $(DAGSTER_SERVICE) uv run dbt build --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

.PHONY: materialize
materialize: ## Materialize the AeroPulse Dagster assets.
	$(COMPOSE) exec -T $(DAGSTER_SERVICE) uv run dagster asset materialize --select $(ASSETS) -m app

.PHONY: clickhouse-query
clickhouse-query: ## List AeroPulse schemas in ClickHouse.
	$(COMPOSE) exec -T $(CLICKHOUSE_SERVICE) clickhouse-client --user aeropulse --password aeropulse --query "select name from system.databases where name in ('aeropulse_raw','aeropulse_staging','aeropulse_analytics') order by name"

.PHONY: clickhouse-clean-legacy
clickhouse-clean-legacy: ## Drop legacy non-prefixed AeroPulse databases.
	$(COMPOSE) exec -T $(CLICKHOUSE_SERVICE) clickhouse-client --user aeropulse --password aeropulse --multiquery --query "drop database if exists raw; drop database if exists staging; drop database if exists analytics; drop database if exists analytics_staging; drop database if exists analytics_marts; drop database if exists aeropulse;"

.PHONY: verify
verify: compose-config terraform-validate terraform-plan dbt-debug dbt-build clickhouse-query ## Run the main local verification checks.
