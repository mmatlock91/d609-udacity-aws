# STEDI Human Balance Analytics

**WGU D609 - Data Analytics at Scale**  
**Udacity Spark and Data Lakes Project**

## Overview

This project builds an AWS data lakehouse that prepares STEDI Step Trainer and mobile accelerometer data for machine learning. The pipeline filters customer data by research consent, identifies eligible customers with accelerometer readings, and combines eligible sensor readings at matching timestamps.

## Tools and environment

- Amazon S3 stores the landing JSON and trusted/curated Parquet datasets
- AWS Glue Studio generates and runs the Python/PySpark ETL jobs
- AWS Glue Data Catalog stores the table definitions
- Amazon Athena validates the data using SQL
- The implementation uses database `stedi` in `us-east-1` and project bucket `s3://stedi-d609-mike-20261113/`
- Each dataset has its own prefix matching its table name
- Athena query results are stored separately under `athena-results/`
- All Glue jobs use Glue 6.0, Python 3, two G.1X workers, disabled job bookmarks, zero retries, and a 15-minute timeout
- Trusted and curated outputs use Snappy-compressed Parquet
- The scripts enable Data Catalog updates with `enableUpdateCatalog=True` and `updateBehavior="UPDATE_IN_DATABASE"`

## Project files

```text
sql/          Landing table DDL scripts
scripts/      Five Python scripts exported from Glue Studio
screenshots/  Athena query results and validation evidence
```

## Pipeline and execution order

1. Create the `stedi` database
2. Upload the source JSON files to their respective landing folders
3. Run the three DDL scripts in [sql/](sql/)

The Glue jobs read these S3-backed tables through Data Catalog source nodes.

Run the jobs in this order, validating each output before continuing:


| Order | Script                                                                             | Transformation                                                                                                           | Output table               |
| ----- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | -------------------------- |
| 1     | [customer_landing_to_trusted.py](scripts/customer_landing_to_trusted.py)           | Keep customers with a non-null research consent date.                                                                    | `customer_trusted`         |
| 2     | [accelerometer_landing_to_trusted.py](scripts/accelerometer_landing_to_trusted.py) | Inner join accelerometer `user` to trusted customer `email`; retain only accelerometer columns.                          | `accelerometer_trusted`    |
| 3     | [customer_trusted_to_curated.py](scripts/customer_trusted_to_curated.py)           | Inner join trusted customers and accelerometer readings by email; retain distinct customer records.                      | `customers_curated`        |
| 4     | [step_trainer_trusted.py](scripts/step_trainer_trusted.py)                         | Inner join Step Trainer landing readings to distinct eligible customer serial numbers; retain only Step Trainer columns. | `step_trainer_trusted`     |
| 5     | [machine_learning_curated.py](scripts/machine_learning_curated.py)                 | Inner join trusted Step Trainer and accelerometer readings where `sensorreadingtime` equals `timestamp`.                 | `machine_learning_curated` |


## Validation results


| Zone    | Table                      | Validated rows | Evidence                                                                    |
| ------- | -------------------------- | --------------: | --------------------------------------------------------------------------- |
| Landing | `customer_landing`         | 956            | [Count](screenshots/customer_landing_count.png)                             |
| Landing | `accelerometer_landing`    | 81,273         | [Count](screenshots/accelerometer_landing_count.png)                        |
| Landing | `step_trainer_landing`     | 28,680         | [Count](screenshots/step_trainer_landing_count.png)                         |
| Trusted | `customer_trusted`         | 482            | [Count and consent validation](screenshots/customer_trusted_validation.png) |
| Trusted | `accelerometer_trusted`    | 40,981         | [Count](screenshots/accelerometer_trusted_count.png)                        |
| Curated | `customers_curated`        | 482            | [Count](screenshots/customers_curated_count.png)                            |
| Trusted | `step_trainer_trusted`     | 14,460         | [Count](screenshots/step_trainer_trusted_count.png)                         |
| Curated | `machine_learning_curated` | 43,681         | [Count](screenshots/machine_learning_curated_count.png)                     |


- Customer landing data includes records with missing research consent: [landing consent evidence](screenshots/customer_landing_missing_consent.png).
- Trusted customers include zero records with missing research consent: [trusted consent validation](screenshots/customer_trusted_validation.png).
- The final table has zero mismatched or null timestamp pairs: [timestamp validation](screenshots/machine_learning_curated_timestamp_validation.png).

Required data screenshots:

- [Customer landing](screenshots/customer_landing.png)
- [Accelerometer landing](screenshots/accelerometer_landing.png)
- [Step Trainer landing](screenshots/step_trainer_landing.png)
- [Customer trusted](screenshots/customer_trusted.png)

## Implementation scope and reruns

This implementation uses standard consent filtering. It does not exclude pre-consent readings or remove identifying fields from the final table, which keeps the accelerometer `user` email and Step Trainer serial number. Before rerunning a Glue job, delete its previous output files from the destination prefix to avoid duplicate counts. Preserve the original landing JSON, and revalidate downstream tables when their inputs change.

## Source data

The project uses Udacity's [STEDI starter datasets](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter).  
See the upstream [Udacity README](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/blob/main/project/README.md) for the source project structure.