import os
from faker import Faker
from minio import Minio
from minio.error import S3Error

# Configuration
MINIO_ENDPOINT = "min.local.com"  # Change to your MinIO server endpoint
ACCESS_KEY = "minio_user"          # Replace with your MinIO access key
SECRET_KEY = "minio_password"          # Replace with your MinIO secret key
BUCKET_NAME = "demo_bucket"        # Replace with your MinIO bucket name
NUM_FILES = 100                           # Number of fake files to generate

# Initialize Faker and MinIO client
fake = Faker()
minio_client = Minio(MINIO_ENDPOINT,
                      access_key=ACCESS_KEY,
                      secret_key=SECRET_KEY,
                      secure=False)  # Set to True if using HTTPS

# Create bucket if it doesn't exist
try:
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
except S3Error as e:
    print(f"Error creating bucket: {e}")

# Generate fake files and upload to MinIO
for i in range(NUM_FILES):
    # Generate fake content
    file_name = f"fake_file_{i + 1}.txt"
    content = fake.text(max_nb_chars=200)  # Generate random text content

    # Save the content to a local file
    with open(file_name, 'w') as f:
        f.write(content)

    # Upload the file to MinIO
    try:
        minio_client.fput_object(BUCKET_NAME, file_name, file_name)
        print(f"Uploaded {file_name} to bucket {BUCKET_NAME}")
    except S3Error as e:
        print(f"Error uploading {file_name}: {e}")

    # Optionally, remove the local file after uploading
    os.remove(file_name)

print("All files uploaded successfully.")

