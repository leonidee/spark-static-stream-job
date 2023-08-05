from os import getenv

import dotenv
import pandas as pd
from s3fs import S3FileSystem

dotenv.load_dotenv()


def main():
    s3 = S3FileSystem(
        key=getenv("AWS_ACCESS_KEY_ID"),
        secret=getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=getenv("AWS_ENDPOINT_URL"),
    )

    a = s3.ls(
        "s3://data-ice-lake-05/master/data/source/spark-static-stream/clients-locations"
    )
    with s3.open(*a, "rb") as f:
        df = pd.read_parquet(f)

    print(df.head(10))


if __name__ == "__main__":
    main()
