FROM python:3.10
LABEL author=${AUTHOR}

COPY ./service/recommendation_service ./recommendation_service
WORKDIR /recommendation_service

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE ${RECOMMENDATIONS_PORT}

CMD uvicorn recommendation_service:app --host 0.0.0.0 --port ${RECOMMENDATIONS_PORT}