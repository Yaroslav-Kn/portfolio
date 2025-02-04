#!/bin/sh
source ./.env
export RECOMMENDATIONS_PORT=$RECOMMENDATIONS_PORT
export EVENTS_PORT=$EVENTS_PORT

cd service/events_service
uvicorn events_service:app --port $EVENTS_PORT --reload &
cd ../recommendation_service
uvicorn recommendation_service:app --port $RECOMMENDATIONS_PORT --reload
