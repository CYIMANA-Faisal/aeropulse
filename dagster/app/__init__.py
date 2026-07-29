from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource

from app.assets.dbt_assets import dbt_build
from app.assets.raw_ingestion import raw_airports, raw_delay_causes, raw_flights

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_project"


defs = Definitions(
    assets=[raw_airports, raw_flights, raw_delay_causes, dbt_build],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        )
    },
)
