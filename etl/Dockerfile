FROM python:3.11

RUN apt-get update
RUN apt-get install -y nano
RUN apt-get install -y cron

WORKDIR /home/etl_project

COPY ./requirements.txt ./requirements.txt

RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt

COPY env/.env env/.env

RUN sed -e 's/^/export /' env/.env >> ~/.bashrc

ENTRYPOINT ["scripts/entrypoint.sh"]