import os
from faker import Faker
from minio import Minio
from minio.error import S3Error

# Configuration
MINIO_ENDPOINT_1 = "127.0.0.1:9000"  # Change to your first MinIO server endpoint
MINIO_ENDPOINT_2 = "127.0.0.2:9000"  # Change to your second MinIO server endpoint
ACCESS_KEY = "minio_user"                  # Replace with your MinIO access key
SECRET_KEY = "minio_password"                   # Replace with your MinIO secret key
BUCKET_NAME = "py-test"              # Replace with your MinIO bucket name
NUM_FILES = 1000                     # Number of fake files to generate

# Initialize Faker and MinIO clients
fake = Faker()
minio_client_1 = Minio(MINIO_ENDPOINT_1,
                       access_key=ACCESS_KEY,
                       secret_key=SECRET_KEY,
                       secure=False)  # Set to True if using HTTPS

minio_client_2 = Minio(MINIO_ENDPOINT_2,
                       access_key=ACCESS_KEY,
                       secret_key=SECRET_KEY,
                       secure=False)  # Set to True if using HTTPS

# Create bucket on both MinIO instances if it doesn't exist
try:
    if not minio_client_1.bucket_exists(BUCKET_NAME):
        minio_client_1.make_bucket(BUCKET_NAME)
    if not minio_client_2.bucket_exists(BUCKET_NAME):
        minio_client_2.make_bucket(BUCKET_NAME)
except S3Error as e:
    print(f"Error creating bucket: {e}")

# Generate fake files and upload to the first MinIO instance
for i in range(NUM_FILES):
    # Generate fake content
    file_name = f"fake_file_{i + 1}.txt"
    content = fake.text(max_nb_chars=200)  # Generate random text content

    # Save the content to a local file
    with open(file_name, 'w') as f:
        f.write(content)

    # Upload the file to the first MinIO instance
    try:
        minio_client_1.fput_object(BUCKET_NAME, file_name, file_name)
        print(f"Uploaded {file_name} to bucket {BUCKET_NAME} on MinIO 1")
    except S3Error as e:
        print(f"Error uploading {file_name} to MinIO 1: {e}")

    # Optionally, remove the local file after uploading
    os.remove(file_name)

print("All files uploaded to MinIO 1 successfully.")

# Check synchronization between the two MinIO instances
for i in range(NUM_FILES):
    file_name = f"fake_file_{i + 1}.txt"
    try:
        # Check if the file exists in the second MinIO instance
        minio_client_2.stat_object(BUCKET_NAME, file_name)
        print(f"{file_name} exists in bucket {BUCKET_NAME} on MinIO 2")

        # Download the file from both MinIO instances
        minio_client_1.fget_object(BUCKET_NAME, file_name, f"minio1_{file_name}")
        minio_client_2.fget_object(BUCKET_NAME, file_name, f"minio2_{file_name}")

        # Compare file contents
        with open(f"minio1_{file_name}", 'r') as f1, open(f"minio2_{file_name}", 'r') as f2:
            content_1 = f1.read()
            content_2 = f2.read()
            if content_1 == content_2:
                print(f"{file_name} is synchronized between MinIO 1 and MinIO 2")
            else:
                print(f"{file_name} is NOT synchronized between MinIO 1 and MinIO 2")

        # Remove the downloaded files
        os.remove(f"minio1_{file_name}")
        os.remove(f"minio2_{file_name}")

    except S3Error as e:
        print(f"Error checking {file_name} on MinIO 2: {e}")

print("Synchronization check completed.")

