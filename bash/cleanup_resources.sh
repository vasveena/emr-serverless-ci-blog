#!/bin/bash

# Update your region. Example: us-west-2

region=$1
prometheus_workspace=$2

if [ -z "$1" ]; then
     printf "\n\nYou need to provide an AWS region\n\n"
     printf "Usage: sh cleanup_resources.sh [region: required] [prometheus workspace ID: optional]\n\n"
     printf "Usage Examples: \n\n"
     printf "sh cleanup_resources.sh us-east-1\n"
     printf "sh cleanup_resources.sh us-east-1 ws-1a23456-xyz2-12ab-cd12-1234456789\n\n"
     exit;
fi

echo "Deleting Amazon EMR Serverless Applications"
listOfApps=$(aws emr-serverless list-applications --region $region | jq '.applications[] | select((.name=="monitor-spark-with-ci" or .name=="custom-jre-with-ci" or .name=="data-science-with-ci") and (.state=="STARTING" or .state=="STARTED" or .state=="STOPPED" or .state=="STOPPING" or .state=="CREATED" or .state=="CREATING"))' | jq -r '.id')
if [[ -n "$listOfApps" ]]; then
  while IFS= read -r appID; do
     echo "Stopping application: $appID" 
     aws emr-serverless stop-application --application-id $appID --region $region 
     echo "Waiting until application $appID stops"
     while true; do 
        appState=$(aws emr-serverless get-application --application-id $appID --region $region | jq '.application | select(.name=="monitor-spark-with-ci" or .name=="custom-jre-with-ci" or .name=="data-science-with-ci")' | jq -r '.state' )
        if [ "$appState" = "STOPPED" ] || [ "$appState" = "CREATED" ]; then
            echo "Application stopped"
            break; 
        fi 
     done
     echo "Deleting application: $appID"
     aws emr-serverless delete-application --application-id $appID --region $region
  done <<< "$listOfApps" 
fi 

echo "Deleting Amazon ECR Images"
aws ecr batch-delete-image --region $region \
    --repository-name emr-serverless-ci-examples \
    --image-ids "$(aws ecr list-images --region $region --repository-name emr-serverless-ci-examples --query 'imageIds[*]' --output json
)" || true
    
echo "Delete ECR Repository" 
aws ecr delete-repository --repository-name emr-serverless-ci-examples --region $region

if [[ -n "$prometheus_workspace" ]]; then
      echo "Deleting Prometheus workspace"
      aws amp delete-workspace --workspace-id $prometheus_workspace --region $region
fi
