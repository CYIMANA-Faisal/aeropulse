from dagster import AssetKey, asset
from dagster_dbt import DbtCliResource


@asset(deps=[AssetKey("raw_airports"), AssetKey("raw_flights")], group_name="dbt")
def dbt_build(dbt: DbtCliResource) -> None:
    dbt.cli(["build"]).wait()
