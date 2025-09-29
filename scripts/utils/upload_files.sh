#!/bin/sh

timestamp=$(date '+%Y-%m-%d %H:%M:%S')

echo "$timestamp: downloading dataset locally from Kaggle"
python3 download_kaggle.sh

timestamp=$(date '+%Y-%m-%d %H:%M:%S')
echo "$timestamp: raw datasets downloaded"

data_folder=~/aws-flight-etl/data/raw

bucket_name="aws-flight-etl-project"

if ! aws s3 ls "s3://$bucket_name" 2>/dev/null; then
    echo "Bucket $bucket_name doesn't exist. Creating..."
    aws s3 mb "s3://$bucket_name"
fi

for file in "$data_folder"/*
do 
    aws s3 mv "$file" "s3://$bucket_name/raw/"
done