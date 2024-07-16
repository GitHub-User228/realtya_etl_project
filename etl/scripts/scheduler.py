#!/usr/local/bin/python3

from crontab import CronTab

from etl.utils import read_yaml
from etl import logger, CONFIG_PATH, PROJECT_PATH


# reading configuration file for cron job scheduling
configs = read_yaml(path=CONFIG_PATH)["cronjob"]

# creating cron object to manage cron jobs
cron = CronTab(user=configs["username"])

# removing existing cron jobs if any exists
for job in cron:
    cron.remove(job)
logger.info("Existing cron jobs has been removed")

# adding new cron job for the ETL pipeline
command = (
    f'bash -l -c "{PROJECT_PATH}/scripts/run.py'
    + f'-p True -t True -ows False -owd False"'
)
job = cron.new(command=command)
job.setall(configs["schedule"])
cron.write()
logger.info("ETL job has been added")
