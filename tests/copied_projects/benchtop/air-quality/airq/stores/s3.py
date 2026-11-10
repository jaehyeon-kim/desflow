"""The SeaweedFS buckets the other stores keep their files in, through its S3 API."""

import os

import boto3


def resource():
    """
    Returns an S3 resource for SeaweedFS, with the settings `airq.core.config` gives PyIceberg.

    Returns:
        boto3.resources.base.ServiceResource: The S3 resource.
    """
    return boto3.resource(
        "s3",
        endpoint_url=os.environ["PYICEBERG_CATALOG__ODCTL__S3__ENDPOINT"],
        aws_access_key_id=os.environ["PYICEBERG_CATALOG__ODCTL__S3__ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ[
            "PYICEBERG_CATALOG__ODCTL__S3__SECRET_ACCESS_KEY"
        ],
        region_name=os.environ["PYICEBERG_CATALOG__ODCTL__S3__REGION"],
    )


def delete_prefix(s3, bucket: str, prefix: str) -> int:
    """
    Deletes every object under a prefix.

    boto3 lists the objects a page at a time and deletes each page in one request.

    Args:
        s3 (boto3.resources.base.ServiceResource): The S3 resource.
        bucket (str): The bucket.
        prefix (str): The key prefix.

    Returns:
        int: How many objects were deleted.
    """
    responses = s3.Bucket(bucket).objects.filter(Prefix=prefix).delete()
    return sum(len(r.get("Deleted", [])) for r in responses)
