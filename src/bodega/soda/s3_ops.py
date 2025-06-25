"""
Core S3 operations for doc_store library.

This module provides low-level S3 operations with proper error handling,
retry logic, and performance monitoring.
"""

import time
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from loguru import logger

from .config import get_config
from .exceptions import (
    S3OperationError,
    S3ConnectionError,
    S3PermissionError,
    S3ObjectNotFoundError,
    S3BucketNotFoundError,
    RetryExhaustedError,
)
from .utils import retry_with_backoff, timing_context, format_file_size


# Global S3 client instance for reuse
_s3_client = None


def get_s3_client() -> boto3.client:
    """
    Get configured S3 client with proper credentials and region.
    
    Returns:
        boto3.client: Configured S3 client
        
    Raises:
        S3ConnectionError: If unable to create S3 client
    """
    global _s3_client
    
    if _s3_client is None:
        try:
            config = get_config()
            credentials = config.get_aws_credentials()
            
            # Filter out None values
            client_kwargs = {k: v for k, v in credentials.items() if v is not None}
            
            _s3_client = boto3.client('s3', **client_kwargs)
            
            logger.info(f"S3 client initialized successfully for region {config.aws_region}")
            
        except Exception as e:
            logger.error(f"Failed to create S3 client: {str(e)}")
            raise S3ConnectionError(
                f"Failed to create S3 client: {str(e)}",
                operation="get_s3_client"
            )
    
    return _s3_client


@retry_with_backoff(max_attempts=3, exceptions=(ClientError, BotoCoreError))
def list_objects_with_prefix(bucket: str, prefix: str = "", max_keys: int = 1000) -> List[Dict[str, Any]]:
    """
    List objects in S3 bucket with optional prefix.
    
    Args:
        bucket: S3 bucket name
        prefix: Object key prefix to filter by
        max_keys: Maximum number of objects to return
        
    Returns:
        List[Dict]: List of object metadata dictionaries
        
    Raises:
        S3OperationError: If operation fails
    """
    try:
        with timing_context(f"list_objects_with_prefix(bucket={bucket}, prefix={prefix})"):
            s3_client = get_s3_client()
            
            objects = []
            continuation_token = None
            
            while len(objects) < max_keys:
                # Calculate remaining keys needed
                remaining_keys = max_keys - len(objects)
                page_size = min(remaining_keys, 1000)  # S3 max page size
                
                kwargs = {
                    'Bucket': bucket,
                    'Prefix': prefix,
                    'MaxKeys': page_size
                }
                
                if continuation_token:
                    kwargs['ContinuationToken'] = continuation_token
                
                response = s3_client.list_objects_v2(**kwargs)
                
                # Add objects from this page
                if 'Contents' in response:
                    objects.extend(response['Contents'])
                
                # Check if there are more pages
                if not response.get('IsTruncated', False):
                    break
                    
                continuation_token = response.get('NextContinuationToken')
                if not continuation_token:
                    break
            
            logger.info(f"Listed {len(objects)} objects from s3://{bucket}/{prefix}")
            return objects[:max_keys]  # Ensure we don't exceed max_keys
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchBucket':
            raise S3BucketNotFoundError(
                f"Bucket not found: {bucket}",
                operation="list_objects_with_prefix",
                bucket=bucket
            )
        elif error_code in ['AccessDenied', 'Forbidden']:
            raise S3PermissionError(
                f"Access denied to bucket: {bucket}",
                operation="list_objects_with_prefix",
                bucket=bucket
            )
        else:
            raise S3OperationError(
                f"Failed to list objects: {str(e)}",
                operation="list_objects_with_prefix",
                bucket=bucket
            )
    except Exception as e:
        raise S3OperationError(
            f"Unexpected error listing objects: {str(e)}",
            operation="list_objects_with_prefix",
            bucket=bucket
        )


@retry_with_backoff(max_attempts=3, exceptions=(ClientError, BotoCoreError))
def get_object_content(bucket: str, key: str) -> bytes:
    """
    Download object content from S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key
        
    Returns:
        bytes: Object content
        
    Raises:
        S3OperationError: If operation fails
        S3ObjectNotFoundError: If object doesn't exist
    """
    try:
        with timing_context(f"get_object_content(bucket={bucket}, key={key})"):
            s3_client = get_s3_client()
            
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            
            content_length = len(content)
            logger.info(f"Downloaded object s3://{bucket}/{key} ({format_file_size(content_length)})")
            
            return content
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchKey':
            raise S3ObjectNotFoundError(
                f"Object not found: s3://{bucket}/{key}",
                operation="get_object_content",
                bucket=bucket,
                key=key
            )
        elif error_code == 'NoSuchBucket':
            raise S3BucketNotFoundError(
                f"Bucket not found: {bucket}",
                operation="get_object_content",
                bucket=bucket
            )
        elif error_code in ['AccessDenied', 'Forbidden']:
            raise S3PermissionError(
                f"Access denied to object: s3://{bucket}/{key}",
                operation="get_object_content",
                bucket=bucket,
                key=key
            )
        else:
            raise S3OperationError(
                f"Failed to get object: {str(e)}",
                operation="get_object_content",
                bucket=bucket,
                key=key
            )
    except Exception as e:
        raise S3OperationError(
            f"Unexpected error getting object: {str(e)}",
            operation="get_object_content",
            bucket=bucket,
            key=key
        )


@retry_with_backoff(max_attempts=3, exceptions=(ClientError, BotoCoreError))
def put_object_content(
    bucket: str,
    key: str,
    content: Union[str, bytes],
    content_type: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    metadata: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Upload object content to S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key
        content: Content to upload (string or bytes)
        content_type: MIME content type
        tags: Object tags as key-value pairs
        metadata: Object metadata as key-value pairs
        
    Returns:
        Dict: Upload response metadata
        
    Raises:
        S3OperationError: If operation fails
    """
    try:
        with timing_context(f"put_object_content(bucket={bucket}, key={key})"):
            s3_client = get_s3_client()
            
            # Convert string to bytes if needed
            if isinstance(content, str):
                content_bytes = content.encode('utf-8')
                if content_type is None:
                    content_type = 'text/plain; charset=utf-8'
            else:
                content_bytes = content
            
            # Build upload arguments
            put_args = {
                'Bucket': bucket,
                'Key': key,
                'Body': content_bytes
            }
            
            if content_type:
                put_args['ContentType'] = content_type
            
            if metadata:
                put_args['Metadata'] = metadata
            
            if tags:
                # Convert tags dict to URL-encoded string
                tag_string = '&'.join(f"{k}={v}" for k, v in tags.items())
                put_args['Tagging'] = tag_string
            
            response = s3_client.put_object(**put_args)
            
            content_length = len(content_bytes)
            logger.info(f"Uploaded object s3://{bucket}/{key} ({format_file_size(content_length)})")
            
            if tags:
                logger.debug(f"Object tags: {tags}")
            
            return {
                'ETag': response.get('ETag'),
                'VersionId': response.get('VersionId'),
                'Size': content_length
            }
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchBucket':
            raise S3BucketNotFoundError(
                f"Bucket not found: {bucket}",
                operation="put_object_content",
                bucket=bucket
            )
        elif error_code in ['AccessDenied', 'Forbidden']:
            raise S3PermissionError(
                f"Access denied to bucket: {bucket}",
                operation="put_object_content",
                bucket=bucket,
                key=key
            )
        else:
            raise S3OperationError(
                f"Failed to put object: {str(e)}",
                operation="put_object_content",
                bucket=bucket,
                key=key
            )
    except Exception as e:
        raise S3OperationError(
            f"Unexpected error putting object: {str(e)}",
            operation="put_object_content",
            bucket=bucket,
            key=key
        )


@retry_with_backoff(max_attempts=3, exceptions=(ClientError, BotoCoreError))
def get_object_tags(bucket: str, key: str) -> Dict[str, str]:
    """
    Get object tags from S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key
        
    Returns:
        Dict[str, str]: Object tags as key-value pairs
        
    Raises:
        S3OperationError: If operation fails
        S3ObjectNotFoundError: If object doesn't exist
    """
    try:
        with timing_context(f"get_object_tags(bucket={bucket}, key={key})"):
            s3_client = get_s3_client()
            
            response = s3_client.get_object_tagging(Bucket=bucket, Key=key)
            
            # Convert TagSet list to dictionary
            tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
            
            logger.debug(f"Retrieved {len(tags)} tags for s3://{bucket}/{key}")
            return tags
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchKey':
            raise S3ObjectNotFoundError(
                f"Object not found: s3://{bucket}/{key}",
                operation="get_object_tags",
                bucket=bucket,
                key=key
            )
        elif error_code == 'NoSuchBucket':
            raise S3BucketNotFoundError(
                f"Bucket not found: {bucket}",
                operation="get_object_tags",
                bucket=bucket
            )
        elif error_code in ['AccessDenied', 'Forbidden']:
            raise S3PermissionError(
                f"Access denied to object: s3://{bucket}/{key}",
                operation="get_object_tags",
                bucket=bucket,
                key=key
            )
        else:
            raise S3OperationError(
                f"Failed to get object tags: {str(e)}",
                operation="get_object_tags",
                bucket=bucket,
                key=key
            )
    except Exception as e:
        raise S3OperationError(
            f"Unexpected error getting object tags: {str(e)}",
            operation="get_object_tags",
            bucket=bucket,
            key=key
        )


@retry_with_backoff(max_attempts=3, exceptions=(ClientError, BotoCoreError))
def put_object_tags(bucket: str, key: str, tags: Dict[str, str]) -> None:
    """
    Set object tags in S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key
        tags: Tags to set as key-value pairs
        
    Raises:
        S3OperationError: If operation fails
        S3ObjectNotFoundError: If object doesn't exist
    """
    try:
        with timing_context(f"put_object_tags(bucket={bucket}, key={key})"):
            s3_client = get_s3_client()
            
            # Convert tags dict to TagSet format
            tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
            
            s3_client.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={'TagSet': tag_set}
            )
            
            logger.info(f"Set {len(tags)} tags on s3://{bucket}/{key}")
            logger.debug(f"Tags: {tags}")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchKey':
            raise S3ObjectNotFoundError(
                f"Object not found: s3://{bucket}/{key}",
                operation="put_object_tags",
                bucket=bucket,
                key=key
            )
        elif error_code == 'NoSuchBucket':
            raise S3BucketNotFoundError(
                f"Bucket not found: {bucket}",
                operation="put_object_tags",
                bucket=bucket
            )
        elif error_code in ['AccessDenied', 'Forbidden']:
            raise S3PermissionError(
                f"Access denied to object: s3://{bucket}/{key}",
                operation="put_object_tags",
                bucket=bucket,
                key=key
            )
        else:
            raise S3OperationError(
                f"Failed to put object tags: {str(e)}",
                operation="put_object_tags",
                bucket=bucket,
                key=key
            )
    except Exception as e:
        raise S3OperationError(
            f"Unexpected error putting object tags: {str(e)}",
            operation="put_object_tags",
            bucket=bucket,
            key=key
        )


@retry_with_backoff(max_attempts=3, exceptions=(ClientError, BotoCoreError))
def object_exists(bucket: str, key: str) -> bool:
    """
    Check if object exists in S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key
        
    Returns:
        bool: True if object exists, False otherwise
        
    Raises:
        S3OperationError: If operation fails (not including object not found)
    """
    try:
        with timing_context(f"object_exists(bucket={bucket}, key={key})"):
            s3_client = get_s3_client()
            
            s3_client.head_object(Bucket=bucket, Key=key)
            logger.debug(f"Object exists: s3://{bucket}/{key}")
            return True
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchKey' or error_code == '404':
            logger.debug(f"Object does not exist: s3://{bucket}/{key}")
            return False
        elif error_code == 'NoSuchBucket':
            raise S3BucketNotFoundError(
                f"Bucket not found: {bucket}",
                operation="object_exists",
                bucket=bucket
            )
        elif error_code in ['AccessDenied', 'Forbidden']:
            raise S3PermissionError(
                f"Access denied to object: s3://{bucket}/{key}",
                operation="object_exists",
                bucket=bucket,
                key=key
            )
        else:
            raise S3OperationError(
                f"Failed to check object existence: {str(e)}",
                operation="object_exists",
                bucket=bucket,
                key=key
            )
    except Exception as e:
        raise S3OperationError(
            f"Unexpected error checking object existence: {str(e)}",
            operation="object_exists",
            bucket=bucket,
            key=key
        )


def delete_object(bucket: str, key: str) -> None:
    """
    Delete object from S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key
        
    Raises:
        S3OperationError: If operation fails
    """
    try:
        with timing_context(f"delete_object(bucket={bucket}, key={key})"):
            s3_client = get_s3_client()
            
            s3_client.delete_object(Bucket=bucket, Key=key)
            logger.info(f"Deleted object s3://{bucket}/{key}")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchBucket':
            raise S3BucketNotFoundError(
                f"Bucket not found: {bucket}",
                operation="delete_object",
                bucket=bucket
            )
        elif error_code in ['AccessDenied', 'Forbidden']:
            raise S3PermissionError(
                f"Access denied to object: s3://{bucket}/{key}",
                operation="delete_object",
                bucket=bucket,
                key=key
            )
        else:
            raise S3OperationError(
                f"Failed to delete object: {str(e)}",
                operation="delete_object",
                bucket=bucket,
                key=key
            )
    except Exception as e:
        raise S3OperationError(
            f"Unexpected error deleting object: {str(e)}",
            operation="delete_object",
            bucket=bucket,
            key=key
        )


def list_objects_with_tags(bucket: str, prefix: str = "", tag_filters: Optional[Dict[str, str]] = None) -> List[Tuple[str, Dict[str, str]]]:
    """
    List objects with their tags, optionally filtered by tag values.
    
    Args:
        bucket: S3 bucket name
        prefix: Object key prefix to filter by
        tag_filters: Optional tag key-value pairs to filter by
        
    Returns:
        List[Tuple[str, Dict[str, str]]]: List of (object_key, tags) tuples
        
    Raises:
        S3OperationError: If operation fails
    """
    try:
        with timing_context(f"list_objects_with_tags(bucket={bucket}, prefix={prefix})"):
            objects = list_objects_with_prefix(bucket, prefix)
            results = []
            
            for obj in objects:
                key = obj['Key']
                try:
                    tags = get_object_tags(bucket, key)
                    
                    # Apply tag filters if specified
                    if tag_filters:
                        match = all(
                            tags.get(filter_key) == filter_value 
                            for filter_key, filter_value in tag_filters.items()
                        )
                        if not match:
                            continue
                    
                    results.append((key, tags))
                    
                except S3ObjectNotFoundError:
                    # Object was deleted between listing and tag retrieval
                    logger.warning(f"Object disappeared during tag retrieval: s3://{bucket}/{key}")
                    continue
            
            logger.info(f"Found {len(results)} objects with tags matching criteria")
            return results
            
    except Exception as e:
        raise S3OperationError(
            f"Failed to list objects with tags: {str(e)}",
            operation="list_objects_with_tags",
            bucket=bucket
        ) 