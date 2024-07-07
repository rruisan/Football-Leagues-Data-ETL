# Football-Leagues-Data-ETL

## Table of Contents

1. [Project Description](#project-description)
2. [How It Works](#how-it-works)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Automated Execution](#automated-execution)
6. [Data Extraction from ESPN](#data-extraction-from-espn)
7. [Contributions](#contributions)
8. [License](#license)

## Project Description

This project is designed to extract, process, and upload football league data to a Snowflake database. The project uses Apache Airflow for orchestration and automation of tasks, ensuring efficient and regular data updates from multiple football leagues.

## How It Works

The project consists of multiple components organized into directories:

- **dags**: Contains the Airflow DAG definition.
- **files**: Includes CSV files used for input data.
- **queries**: SQL scripts for uploading and ingesting data in Snowflake.
- **utils.py**: Utility functions for data fetching and processing.

### Key Workflow

1. **Read CSV Files**: The DAG reads the input CSV files.
2. **Process Data**: It processes and merges data from various leagues.
3. **Upload to Snowflake**: Finally, it uploads the processed data to a Snowflake stage and ingests it into a final table.

## Configuration

Ensure the following configurations are set up in Airflow:

- **Airflow Variables**:
  - `feature_info`: Contains Snowflake connection parameters like `DWH`, `DB`, `ROLE`, `stage`, `table`, and `path_file`.

## Usage
### Trigger the DAG:

1. Open the Airflow web interface.
2. Locate the DAG named `FOOTBAL_LEAGUES` and trigger it manually.

## Automated Execution

The DAG is configured to be triggered manually, but it can be scheduled to run at regular intervals by setting the `schedule_interval` parameter in the DAG definition.

### Sample DAG Definition
```python
with DAG(
    "FOOTBAL_LEAGUES",
    description="Extracting Data Football League",
    start_date=datetime(2024, 7, 6),
    schedule_interval='@daily',  # Example of setting a daily schedule
    catchup=False,
) as dag:
```
## Data Extraction from ESPN
The script utils.py contains functions to fetch and process football league data from ESPN.

## Contributions
Contributions are welcome! Please fork the repository and submit a pull request with your changes. Ensure your code follows the project's coding standards and includes relevant tests.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.
