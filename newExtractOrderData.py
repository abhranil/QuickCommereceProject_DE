# newExtractOrderData.py

import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import pandas as pd
from datetime import datetime
from google.cloud import storage


def export_orders_to_gcs(
    host="192.168.224.3",
    user="abhranil",
    password="Abhranil@89",
    database="QuickCommerce",
    bucket_name="dataproc-staging-asia-south1-925894589695-qxkvzrhv",
    gcs_folder="OrdersData"
):
    """Exports Orders table from MySQL to GCS as a CSV file."""
    conn = None
    try:
        # 1️⃣ Connect to MySQL
        conn = MySQLdb.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=3306
        )
        print("✅ Connected to MySQL database!")

        # 2️⃣ Fetch data
        cur = conn.cursor()
        cur.execute("SELECT * FROM Orders")
        rows = cur.fetchall()

        if not rows:
            print("⚠️ No records found in 'Orders' table.")
            return None

        print(f"✅ Fetched {len(rows)} records from 'Orders' table.")

        # 3️⃣ Convert to DataFrame
        column_names = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=column_names)
        print("✅ Data successfully loaded into DataFrame!")

        # 4️⃣ Save CSV locally
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_csv_path = f"/home/airflow/gcs/data/orders_data_{timestamp}.csv"
        df.to_csv(local_csv_path, index=False)
        print(f"✅ Data saved locally as {local_csv_path}")

        # 5️⃣ Upload to Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob = f"{gcs_folder}/orders_data_{timestamp}.csv"
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_csv_path)

        print(f"✅ File uploaded to GCS bucket '{bucket_name}' at '{destination_blob}'")

        return {
            "status": "success",
            "local_file": local_csv_path,
            "gcs_path": f"gs://{bucket_name}/{destination_blob}",
            "record_count": len(df)
        }

    except MySQLdb.Error as err:
        print(f"❌ MySQL Error: {err}")
        return {"status": "error", "message": str(err)}

    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return {"status": "error", "message": str(e)}

    finally:
        if conn:
            conn.close()
            print("🔒 Database connection closed.")
