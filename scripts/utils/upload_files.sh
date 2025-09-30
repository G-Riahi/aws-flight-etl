#!/bin/sh

#Defining folder paths 
log_file=~/aws-flight-etl/logs.log

data_folder=~/aws-flight-etl/data/raw

bucket_name=aws-flight-etl-project

#Function to write logs
log(){
    echo "$(date '+%Y-%m-%d %H:%M:%S'): $1" >> $log_file
}

log "Downloading dataset locally from Kaggle"
python3 scripts/utils/download_kaggle.py >> $log_file 2>&1

log "Raw datasets downloaded"

if ! aws s3 ls "s3://$bucket_name" 2>/dev/null; then
    log "Bucket $bucket_name doesn't exist. Creating..."
    aws s3 mb "s3://$bucket_name"
fi

for file in "$data_folder"/*; do 
    log "Copying $file to S3://$bucket_name"
    aws s3 cp "$file" "s3://$bucket_name/raw/"
    log "Copying $file to S3://$bucket_name completed"
done

log "Datasets successfully stored in S3://$bucket_name"
log "-----------------------------------------------------------------------------------------------------------------"