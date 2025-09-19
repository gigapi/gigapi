from typing import BinaryIO, List
import boto3
from botocore.client import Config
import io
import os
import fnmatch
from .fs_operator import FsOperator

class FSOperatorS3(FsOperator):
    def __init__(self, hostname: str, port: int, username: str, password: str,
                 bucket_name: str, prefix: str, use_ssl: bool = True):
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip('/') + '/'

        # Configure the S3 client
        self.s3 = boto3.client(
            's3',
            endpoint_url=f'{"https" if use_ssl else "http"}://{hostname}:{port}',
            aws_access_key_id=username,
            aws_secret_access_key=password,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'  # This can be any valid region for S3-compatible servers
        )
        self.sep = "/"

    def _full_path(self, path: str) -> str:
        if path.startswith("/"):
            return path
        return f"{self.prefix.rstrip("/")}/{path.lstrip("/")}"

    def rmrf(self, path: str):
        full_path = self._full_path(path).lstrip("/")
        objects_to_delete = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=full_path)
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete.get('Contents', [])]}
        if delete_keys['Objects']:
            self.s3.delete_objects(Bucket=self.bucket_name, Delete=delete_keys)

    def mkdir(self, path: str):
        # S3 doesn't have directories, so we create an empty file to represent the directory
        full_path = self._full_path(path.rstrip('/') + '/')
        self.s3.put_object(Bucket=self.bucket_name, Key=full_path, Body='')

    def copy_internal(self, src_path: str, dst_path: str) -> None:
        src_full_path = self._full_path(src_path)
        dst_full_path = self._full_path(dst_path)
        copy_source = {'Bucket': self.bucket_name, 'Key': src_full_path}
        self.s3.copy(copy_source, self.bucket_name, dst_full_path)

    def copy_external(self, src_path: str, dst_url: str) -> None:
        pass

    def create_file(self, path: str, content: BinaryIO) -> None:
        full_path = self._full_path(path)
        self.s3.upload_fileobj(content, self.bucket_name, full_path)

    def ls(self, path: str) -> List[str]:
        full_path = self._full_path(path)
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=full_path, Delimiter='/')
        files = [obj['Key'][len(full_path):] for obj in response.get('Contents', []) if obj['Key'] != full_path]
        directories = [prefix['Prefix'][len(full_path):].rstrip('/') for prefix in response.get('CommonPrefixes', [])]
        return files + directories

    def find(self, path: str, pattern: str) -> List[str]:
        full_path = self._full_path(path)
        result = []
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=full_path):
            for obj in page.get('Contents', []):
                key = obj['Key']
                relative_path = key[len(self.prefix):]
                if fnmatch.fnmatch(os.path.basename(relative_path), pattern):
                    result.append(relative_path)
        return result

    def open_file(self, path: str) -> BinaryIO:
        full_path = self._full_path(path)
        try:
            s3_object = self.s3.get_object(Bucket=self.bucket_name, Key=full_path)
            return io.BytesIO(s3_object['Body'].read())
        except self.s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"File not found: {path}")


    def get_size(self, path: str) -> int:
        full_path = self._full_path(path)
        try:
            response = self.s3.head_object(Bucket=self.bucket_name, Key=full_path)
            return response['ContentLength']
        except self.s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"File not found: {path}")
        except Exception as e:
            raise RuntimeError(f"Error getting size of file {path}: {str(e)}")

    def is_file(self, path: str) -> bool:
        full_path = self._full_path(path)
        try:
            # Use head_object to check if the object exists and is a file
            self.s3.head_object(Bucket=self.bucket_name, Key=full_path)
            return True
        except self.s3.exceptions.NoSuchKey:
            # The object doesn't exist
            return False
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object doesn't exist
                return False
            else:
                # Some other error occurred
                raise RuntimeError(f"Error checking if path is a file {path}: {str(e)}")