from datetime import datetime
from airflow.models import Variable, DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pandas as pd
from utils import data_processing

def read_csv_files():
    """Read CSV files and return DataFrames."""
    df_ligas = pd.read_csv("dags/files/df_ligas.csv")
    df_team = pd.read_csv("dags/files/team_table.csv")
    return df_ligas, df_team

def extract_info(df, df_team, **kwargs):
    """Process and merge data, then save to CSV."""
    try:
        df_data = data_processing(df)
        df_final = pd.merge(df_data, df_team, how="inner", on="EQUIPO")
        df_final = df_final[
            ["ID_TEAM", "EQUIPO", "J", "G", "E", "P", "GF", "GC", "DIF", "PTS", "LIGA", "CREATED_AT"]
        ]
        df_final.to_csv("dags/files/premier_positions.csv", index=False)
    except Exception as e:
        print(f"Error processing data: {e}")
        raise

# Define the DAG and its settings
with DAG(
    "FOOTBAL_LEAGUES",
    description="Extracting Data Footbal League",
    start_date=datetime(2024, 7, 6),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Retrieve parameters from Airflow Variables
    params_info = Variable.get("feature_info", deserialize_json=True)

    # Read input CSV files
    df, df_team = read_csv_files()

    # Define the PythonOperator to execute the extract_info function
    extract_data = PythonOperator(
        task_id="EXTRACT_FOTBALL_DATA",
        python_callable=extract_info,
        op_kwargs={"df": df, "df_team": df_team},
    )

    # Define the SnowflakeOperator to upload data to stage
    upload_stage = SnowflakeOperator(
        task_id="upload_data_stage",
        sql="queries/upload_stage.sql",
        snowflake_conn_id="connect_snowflake_leagues",
        warehouse=params_info["DWH"],
        database=params_info["DB"],
        role=params_info["ROLE"],
        params=params_info,
    )

    # Define the SnowflakeOperator to ingest data into the final table
    ingest_table = SnowflakeOperator(
        task_id="ingest_table",
        sql="queries/upload_table.sql",
        snowflake_conn_id="connect_snowflake_leagues",
        warehouse=params_info["DWH"],
        database=params_info["DB"],
        role=params_info["ROLE"],
        params=params_info,
    )

    # Set the task dependencies
    extract_data >> upload_stage >> ingest_table
