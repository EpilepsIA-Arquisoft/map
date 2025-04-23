import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

GCS_BUCKET_NAME = "examenes-eeg"
GCS_BASE_FOLDER = "uploads/"