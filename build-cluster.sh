#!/bin/sh

CLUSTER_NAME="dotnet-streaming-cluster"
CLOUD="${CLOUD:-gcp}"
REGION="${REGION:-us-east1}"
ENVIRONMENT="dotnet-streaming"

echo "Creating an environment - $ENVIRONMENT"
ENV_ID=$(confluent environment create $ENVIRONMENT -o json | jq -r .id)
echo "Creating the cluster - $CLUSTER_NAME"
CLUSTER_ID=$(confluent kafka cluster create $CLUSTER_NAME -o json \
  --cloud $CLOUD \
  --region $REGION \
  --environment "$ENV_ID" | jq -r .id);

STATUS="PROVISIONING"
echo "Waiting for the cluster to be available, will check every 5 seconds"
while [ $STATUS != "UP" ]; do
  STATUS=$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r .status)
  sleep 5
  echo "Cluster is $STATUS"
done

echo "Creating cluster credentials"
CLUSTER_CREDS=$(confluent api-key create --resource "$CLUSTER_ID" --environment "$ENV_ID" -o json);
API_KEY=$(echo "${CLUSTER_CREDS}" | jq -r .key);
API_SECRET=$(echo "${CLUSTER_CREDS}" | jq -r .secret);

echo "Creating the input and output topics"
confluent kafka topic create --cluster "$CLUSTER_ID" --environment "$ENV_ID" tpl_input
confluent kafka topic create --cluster "$CLUSTER_ID" --environment "$ENV_ID" tpl_output 

echo "Creating the client config file 'kafka.properties' at the root of the project"
confluent kafka client-config create csharp --cluster "$CLUSTER_ID" \
   --environment "$ENV_ID" \
   --api-key "$API_KEY" \
   --api-secret "$API_SECRET" 1> kafka.properties 2>&1
  
echo "Creating the datagen connector now"
CONNECT_CONFIG=$(cat purchase-datagen.json)
CONNECT_CONFIG=$(echo $CONNECT_CONFIG | jq '. + { "kafka.api.key": "'${API_KEY}'"}' )
CONNECT_CONFIG=$(echo $CONNECT_CONFIG | jq '. + { "kafka.api.secret": "'${API_SECRET}'"}' )

echo $CONNECT_CONFIG > connector.json

CONNECTOR_ID=$(confluent connect create --config connector.json \
  --cluster "$CLUSTER_ID"\
  --environment "$ENV_ID" -o json | jq -r .id)

echo "Storing cluster information locally in cluster-details.json which can be used to automate cleanup"
confluent kafka cluster describe "$CLUSTER_ID"\
 --environment "$ENV_ID" -o json \
  | jq '. + { "environment": "'${ENV_ID}'"}' \
  | jq '. + { "connector": "'${CONNECTOR_ID}'"}' > cluster-details.json
  
rm connector.json