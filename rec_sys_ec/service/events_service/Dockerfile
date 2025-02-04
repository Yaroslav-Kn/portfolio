FROM python:3.10
LABEL author=${AUTHOR}

COPY ./service/events_service ./events_service
WORKDIR /events_service

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE ${EVENTS_PORT}

CMD uvicorn events_service:app --host 0.0.0.0 --port ${EVENTS_PORT}