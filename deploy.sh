#!/bin/bash
docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/google-bigquery-extractor quay.io/keboola/google-bigquery-extractor:$TRAVIS_TAG
docker tag keboola/google-bigquery-extractor quay.io/keboola/google-bigquery-extractor:latest
docker push quay.io/keboola/google-bigquery-extractor:$TRAVIS_TAG
docker push quay.io/keboola/google-bigquery-extractor:latest