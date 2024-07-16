#!/bin/bash

chmod +x ${PROJECT_PATH}/scripts/*.sh

${PROJECT_PATH}/scripts/reset_cron.sh

jupyter notebook --allow-root --ip 0.0.0.0