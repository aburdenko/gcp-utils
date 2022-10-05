#!/bin/bash

#bq rm  --connection --location us-east1  --project_id=$(gcloud config get-value project)  653488387759.us.gcf-conn

bq mk --connection --display_name='my_gcf_conn' \
      --connection_type=CLOUD_RESOURCE \
      --project_id=$(gcloud config get-value project) \
      --location=us  gcf-conn


bq show --location=us  --connection gcf-conn

gcloud config set functions/region us-central1

gcloud beta functions delete add-fake-user2 -q

 gcloud beta functions deploy add-fake-user2 \
--runtime python37 \
--trigger-http \
--entry-point add_fake_user \
--source ./deploy \
--allow-unauthenticated 

gcloud alpha functions add-iam-policy-binding add-fake-user2 \
--region us-central1 \
--project aburdenko-project \
--member serviceAccount:connection-653488387759-h87q@gcp-sa-bigquery-condel.iam.gserviceaccount.com \
--role roles/cloudfunctions.invoker \
--account aburdenko@google.com 




gcloud functions call add-fake-user2 --data '{ 
        "calls": [ 
            [4557, "acme"], 
            [8756, "hilltop"] 
        ] 
}'


echo "refreshing functions.sql..."    
f=$( cat ./deploy/sql/functions.sql )    

bq query --use_legacy_sql=false \
      "$f"        

echo "done."	
