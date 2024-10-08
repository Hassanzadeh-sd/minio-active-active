from minio import Minio
from minio.error import S3Error

# Configuration
MINIO_ENDPOINT = "s3.sos.com"  # Change to your MinIO server endpoint
ACCESS_KEY = "sos_minio_user"          # Replace with your MinIO access key
SECRET_KEY = "vyF04hwL7PMazOVdW3BpY"          # Replace with your MinIO secret key
BUCKET_NAME = "py-large-file"        # Replace with your MinIO bucket name
NUM_FILES = 100                           # Number of fake files to generate

minio_client = Minio(MINIO_ENDPOINT,
                      access_key=ACCESS_KEY,
                      secret_key=SECRET_KEY,
                      secure=False)  # Set to True if using HTTPS

OBJECT_NAME = "largefile.txt" 
DOWNLOAD_PATH = "/tmp/largefile.txt" 

# Download the file
try:
    minio_client.fget_object(BUCKET_NAME, OBJECT_NAME, DOWNLOAD_PATH)
    print(f"Downloaded '{OBJECT_NAME}' to '{DOWNLOAD_PATH}'")
except S3Error as e:
    print(f"Error occurred: {e}")
