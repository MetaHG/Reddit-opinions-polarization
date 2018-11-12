##!/usr/bin/env bash

SSH_USERNAME="jperreno"
SSH_URL="iccluster028.iccluster.epfl.ch"
REMOTE_DIR="~/ADAProject"
SSH_PW_FILE=".ssh-pw"

# Create zip with dependencies
zip -r src.zip src
mv src.zip jobs/src.zip

# Copy content of "jobs" folder
scp -i "~/.ssh/ada_rsa" -r "jobs" $SSH_USERNAME@$SSH_URL:$REMOTE_DIR

# Remove zip
rm jobs/src.zip

# scp -i "~/.ssh/ada_rsa" -r "src" $SSH_USERNAME@$SSH_URL:$REMOTE_DIR
# scp -i "~/.ssh/ada_rsa" -r "./{scripts,jobs}" $SSH_USERNAME@$SSH_URL:$REMOTE_DIR
#
