##!/usr/bin/env bash

source .ssh-config

ssh -i ~/.ssh/ada_rsa $SSH_USERNAME@$SSH_URL
