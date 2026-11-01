
import argparse
import os
from pathlib import Path

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload a CSV source file to Bronze object storage."
    )
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--bucket", default="bronze")
    parser.add_argument(
        "--key",
        default="chicago_taxi_kaggle/source=2023/Taxi_Trips_2023.csv",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.file.is_file():
        raise FileNotFoundError(args.file)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv(
            "RUSTFS_ENDPOINT_URL", "http://localhost:9000"
        ),
        aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    try:
        s3.head_object(Bucket=args.bucket, Key=args.key)
        print(f"Object already exists: s3://{args.bucket}/{args.key}")
        return
    except s3.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise

    transfer_config = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=64 * 1024 * 1024,
        max_concurrency=4,
    )

    size_mb = args.file.stat().st_size / (1024 * 1024)
    print(f"Uploading {args.file.name} ({size_mb:.2f} MB)...")

    s3.upload_file(
        str(args.file),
        args.bucket,
        args.key,
        Config=transfer_config,
    )

    print(f"Uploaded: s3://{args.bucket}/{args.key}")


if __name__ == "__main__":
    main()
