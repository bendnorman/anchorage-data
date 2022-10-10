from dagster import resource, Field
from pathlib import Path


@resource(
    config_schema={
        "input_dir": Field(
            str, default_value=str(Path(__file__).parents[2] / "data" / "inputs")
        )
    }
)
def input_dir(init_context):
    return Path(init_context.resource_config["input_dir"])
