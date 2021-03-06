##!/usr/bin/env bash

# Fetch variables
source .ssh-config

# Copy content of "jobs" folder
scp -i "~/.ssh/ada_rsa" -r "jobs" $SSH_USERNAME@$SSH_URL:$REMOTE_DIR
scp -i "~/.ssh/ada_rsa" -r "src" $SSH_USERNAME@$SSH_URL:$REMOTE_DIR
scp -i "~/.ssh/ada_rsa" -r "scripts" $SSH_USERNAME@$SSH_URL:$REMOTE_DIR
scp -i "~/.ssh/ada_rsa" -r "eggs" $SSH_USERNAME@$SSH_URL:$REMOTE_DIR
scp -i "~/.ssh/ada_rsa" -r "archives" $SSH_USERNAME@$SSH_URL:$REMOTE_DIR
