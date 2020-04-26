FROM python:3.7-alpine

WORKDIR /usr/src/app

RUN apk update \
    && apk add ttf-dejavu \
    && apk add openjdk8-jre \
    && apk add libpq postgresql-dev mariadb-connector-c-dev git \
    && apk add build-base openssl-dev libffi-dev unixodbc-dev

ADD . /usr/src/app
RUN pip install -r requirements.txt

CMD ["python", "-m", "vk_service"]
