from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# The path where the volume is mounted inside the container
DBT_PROJECT_PATH = Path("/opt/airflow/novapay_data_lab")

dag = DbtDag(
    dag_id="dbt_novapay",
    start_date=datetime(2026, 5, 1),
    schedule="0 6 * * *",  # Daily at 6 AM
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default", # Ensure this ID exists in Airflow Connections
            profile_args={"database": "NOVAPAY_STAGING", "schema": "PUBLIC"},
        ),
    ),
    operator_args={
        "install_deps": True, # Runs 'dbt deps' for you
    },
    catchup=False,
)