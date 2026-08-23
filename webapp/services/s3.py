"""AWS S3 client factory and shared constants."""

import logging

import boto3
from config import Config

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client("s3", region_name=Config.AWS_REGION)


# Cost-allocation tag applied to every S3 object created by this app.
# Formatted as a URL query-string for the S3 ``Tagging`` header.
S3_COST_TAGGING = "Project=avoided-emissions"
