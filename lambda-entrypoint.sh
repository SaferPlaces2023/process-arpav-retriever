#!/bin/bash
# This script is the entrypoint for the Lambda function
# Used to emulate the awslinux2 entrypoint
exec python -m awslambdaric "$@"