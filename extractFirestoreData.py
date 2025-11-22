from pymongo import MongoClient
from urllib.parse import quote_plus
import pandas as pd
from google.cloud import storage
from datetime import datetime
import os

def export_inventory_to_gcs():
    # === MongoDB-compatible Firestore connection details ===
    username = "abhranil"
    password = "ljcVOu0GqPUpc7PBnrsVCzr_Q9kFMy59NBFuiTrKf01_Lf0z"
    host = "89848341-4b54-406a-b047-a2e911f2ee64.asia-south1.firestore.goog"
    database_name = "quick-commerce-inventory"
    collection_name = "items"

    # Encode password safely
    encoded_password = quote_plus(password)

    # Build MongoDB connection URI
    uri = (
        f"mongodb://{username}:{encoded_password}@{host}:443/{database_name}"
        "?loadBalanced=true&tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false"
    )

    # === GCS bucket configuration ===
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    CSV_FILE = f"inventory_data_{timestamp}.csv"
    BUCKET_NAME = "dataproc-staging-asia-south1-925894589695-qxkvzrhv"
    DESTINATION_BLOB_NAME = f"Inventorydata/{CSV_FILE}"

    # === Connect to MongoDB ===
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)

    try:
        client.admin.command("ping")
        print("✅ Connected to MongoDB-compatible Firestore successfully!")

        db = client[database_name]
        print("📁 Available collections:", db.list_collection_names())

        collection = db[collection_name]

        # Fetch all documents
        docs = list(collection.find())
        print(f"📦 Retrieved {len(docs)} documents from '{collection_name}'")

        if not docs:
            print("⚠️ No data found in the collection. Check DB or permissions.")
        else:
            for d in docs[:3]:
                print("🧾 Sample doc:", d)

        # === Convert to DataFrame ===
        df = pd.DataFrame(docs)
        print("🧮 DataFrame preview (before cleanup):")
        print(df.head())

        # ✅ Drop the MongoDB internal _id field if it exists
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])
            print("🧹 Removed '_id' column successfully.")

        # Save cleaned data to CSV
        df.to_csv(CSV_FILE, index=False)
        print(f"💾 Data saved locally to: {CSV_FILE}")

        # === Upload CSV to GCS ===
        print("☁️ Uploading CSV to GCS bucket...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(DESTINATION_BLOB_NAME)
        blob.upload_from_filename(CSV_FILE)
        print(f"✅ File uploaded to gs://{BUCKET_NAME}/{DESTINATION_BLOB_NAME}")

    except Exception as e:
        print("❌ Error during connection or data extraction:", e)

    finally:
        client.close()
        print("🔒 MongoDB connection closed.")
