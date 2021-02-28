# from airflow.hooks.S3_hook import S3Hook

# def get_s3_files(s3_path, filename):
#     """
#     E.G.
#     s3_path = HES-AE/NIC243790_HES_AE_201599.zip
#     filename = NIC243790_HES_AE_201599.zip
#     """
#     # Initialize the s3 hook
#     s3_hook = S3Hook()
#     # Read the keys from s3 bucket
#     print(s3_path)
#     print(filename)
#     s3_hook.get_key(key=s3_path, bucket_name="dars-raw").download_file(f"./input_data/{filename}")


# def get_files(files, table):
#     for file in files:
#         get_s3_files(file, f"{table}/{file}")