import s3fs

class S3Config:
    def __init__(self, **context):
        self.__access_key  = context['ACCESS_KEY']
        self.__secret_key  = context['SECRET_KEY']
        self.__s3_endpoint  = context['S3_ENDPOINT']

    def connection_s3(self):
        return s3fs.S3FileSystem(
            client_kwargs={'endpoint_url': self.__s3_endpoint},
            key=self.__access_key,
            secret=self.__secret_key
        )

    def upload_file(self, local_path: str, s3_path: str):
        fs = self.connection_s3()
        fs.put(local_path, s3_path)
        print(f"✅ File {local_path} Successfully Upload s3://{s3_path}")