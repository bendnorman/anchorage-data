from dagster import asset
import pandas as pd
from ancdata.helpers import read_csv_coerce_ints


@asset(io_manager_key="sqlite_io_manager", required_resource_keys={"input_dir"})
def raw_residential_cama(context):
    return read_csv_coerce_ints(
        context.resources.input_dir / "CAMAResidential_2022.csv"
    )


@asset(io_manager_key="sqlite_io_manager")
def clean_data(raw_residential_cama):
    return raw_residential_cama
