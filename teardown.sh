#!/bin/sh

ENV_ID=$(jq -r .environment cluster-details.json)

echo "Deleting the datagen connector, cluster, topics, and environment now"
confluent environment delete "$ENV_ID"
