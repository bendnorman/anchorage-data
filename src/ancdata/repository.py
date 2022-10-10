from dagster import (
    repository,
    load_assets_from_package_module,
    define_asset_job,
    with_resources,
    in_process_executor,
    fs_io_manager,
)
import ancdata
from ancdata.io_managers import ancdata_sqlite_io_manager
from ancdata.resources import input_dir


@repository
def ancdata_repository():
    return [
        with_resources(
            load_assets_from_package_module(ancdata),
            resource_defs={
                "sqlite_io_manager": fs_io_manager,
                "input_dir": input_dir,
            },
        ),
        define_asset_job(name="ancdata_job", executor_def=in_process_executor),
    ]
