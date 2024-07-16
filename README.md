# Automatic ETL Project for Realty Data From Yandex
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Ubuntu](https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=ubuntu&logoColor=white)
![Cronjob](https://img.shields.io/badge/cronjob-blueviolet?style=for-the-badge&)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Pandas](https://img.shields.io/badge/Pandas-%23EE4C2C.svg?style=for-the-badge&logo=Pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/sqlalchemy-red?style=for-the-badge&)
![Requests](https://img.shields.io/badge/requests-green?style=for-the-badge&)
![BeautifulSoup](https://img.shields.io/badge/beautifulsoup-green?style=for-the-badge&)
![Geopy](https://img.shields.io/badge/geopy-yellow?style=for-the-badge&logoColor=white)

This repository contains a custom Extract, Transform, Load (ETL) pipeline that utilizes Docker, PostgreSQL, Python and Cron to model an automatic ETL pipeline.

## Repository Structure

1. **[docker-compose.yml](./docker-compose.yml)**: Three services are defined:
   - `source_postgres`: The source PostgreSQL database.
   - `destination_postgres`: The destination PostgreSQL database.
   - `etl`: The service that runs all steps of the ETL pipeline.

2. **[source_db_init/init.sql](./source_db_init/init.sql)**: This SQL script initializes the source database with empty tables (in case there is no data stored yet)

3. **[destination_db_init/init.sql](./destination_db_init/init.sql)**: This SQL script initializes the destination database with empty tables (in case there is no data stored yet)

4. **[etl](./etl)**: This directory contains scripts and configurations files for the ETL pipeline

   4.1. **[Dockerfile](./etl/Dockerfile)**: This Dockerfile sets up Ubuntu with Python and all necessary packages [requirements.txt](./etl/requirements.txt). Also installs cron and the PostgreSQL client. Lastly, the corresponding service starts with the specified entrypoint

   4.2. **[.env](./etl/env/.env)**: This file contains necessary enviromental variables, which are required for the ETL process

   4.3. **[src/etl](./etl/src/etl)**: This directory contains Python source code and configuration files requried for the ETL process. 

      - [init.py](./etl/src/etl/__init__.py): Initialises custom logger and defines necessary path variables
      - [pipeline.py](./etl/src/etl/etl_pipeline.py): Implementation of the function which runs the ETL pipeline
      - [parser.py](./etl/src/etl/parser.py): Implementation of the parser with retrieves realty data from [RealtyYa](https://realty.ya.ru/sankt-peterburg/snyat/kvartira/) and saves raw data to the source database
      - [transformer.py](./etl/src/etl/transformer.py): Implementation of the transformer which transforms raw data from the source database to the form appropriate for the data analysis. Transformed data is then saved to the destination database.
      - [utils.py](./etl/src/etl/utils.py): Implementation of the utilities required for the ETL pipeline
      - [config.yaml](./etl/src/etl/config.yaml): Configuration file of the ETL pipeline

   4.4. **[logs](./etl/logs)**: This directory contains log files of the ETL pipeline

   4.5. **[scripts](./etl/scripts)**: This directory contains bash and python scripts

      - [entrypoint.sh](./etl/scripts/entrypoint.sh): Entrypoint for the ETL service which runs the cron job for the ETL pipeline and also initialises jupyter notebook
      - [reset_cron.sh](./etl/scripts/reset_cron.sh): Resets the cron job for the ETL pipeline
      - [scheduler.py](./etl/scripts/scheduler.py): Reschedules the ETL job in cron
      - [run.py](./etl/scripts/run.py): Runs the ETL pipeline

   4.6. **[research](./etl/research)**: This directory contains jupyter notebooks for the research and debugging purposes

   4.7. **[tests](./etl/tests)**: TODO


## Getting Started

1. Build the corresponding image: 
   ```shell
   docker compose build
   ```
2. Start all services using this image - as a result the ETL pipeline will periodically run: 
   ```shell
   docker compose up
   ```
3. Access the jupyter notebook via http://localhost:8888/tree (find token within logs of the etl service)

In case you want to reshedule the ETL pipeline:
1. Change `cronjob.schedule` field in [config.yaml](./etl/src/etl/config.yaml) in order to change the timing at which the ETL pipeline runs
2. Reset the cron job for the ETL pipeline by running [reset_cron.sh](./etl/scripts/reset_cron.sh) or by restarting the whole container of the ETL service
