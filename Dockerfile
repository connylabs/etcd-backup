FROM python:3-alpine

RUN mkdir -p /app
WORKDIR /app
COPY backup-etcd.py /app
COPY docker-run.sh /app

CMD /app/docker-run.sh
