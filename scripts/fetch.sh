##!/usr/bin/env bash

# Read env
source .ssh-config

if [ $# -eq 0 ]; then
    echo "Please provide a file name."; exit 1
fi

# Extract from Hadoop inside cluster
ssh -i ~/.ssh/ada_rsa -l $SSH_USERNAME $SSH_URL "hadoop fs -get $1 $REMOTE_DIR/data/$1"

# Copy to local over ssh
scp -i ~/.ssh/ada_rsa -r $SSH_USERNAME@$SSH_URL:$REMOTE_DIR/data/$1 ./data/$1
