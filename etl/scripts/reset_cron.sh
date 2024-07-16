#!/bin/bash

service cron stop
python3 ${PROJECT_PATH}/scripts/scheduler.py
service cron start