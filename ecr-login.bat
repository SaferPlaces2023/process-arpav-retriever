@echo off
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 901702069075.dkr.ecr.us-east-1.amazonaws.com
