import streamlit as st
import boto3
from botocore.config import Config
from datetime import timezone, timedelta, datetime
import pandas as pd

st.set_page_config(page_title="S3 Glacier Restore Checker", layout="wide")
st.title("📦 S3 Glacier Restored File Checker")

# --- Step 1: User Inputs ---
with st.form("credentials_form"):
    access_key = st.text_input("🔑 AWS Access Key ID")
    secret_key = st.text_input("🔐 AWS Secret Access Key", type="password")
    region = st.text_input("🌍 AWS Region", value="us-east-1")
    bucket = st.text_input("🪣 S3 Bucket Name")
    prefix = st.text_input("📁 Folder Prefix (e.g. logs/2025/ or blank for full bucket)", value="")
    submitted = st.form_submit_button("Check Restored Files")

if submitted:
    if not all([access_key, secret_key, region, bucket]):
        st.error("❌ Please fill in all required fields.")
    else:
        try:
            # Create session
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
            s3 = session.client('s3', config=Config(retries={'max_attempts': 10, 'mode': 'standard'}))

            st.info("🔍 Scanning for restored Glacier/Deep Archive files...")

            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

            restored_data = []

            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']

                    try:
                        head = s3.head_object(Bucket=bucket, Key=key)
                    except:
                        continue

                    storage_class = head.get('StorageClass', 'STANDARD')
                    restore_status = head.get('Restore')

                    if not storage_class.startswith('GLACIER') and storage_class != 'DEEP_ARCHIVE':
                        continue

                    if restore_status and 'ongoing-request="false"' in restore_status:
                        expiry = None
                        if 'expiry-date=' in restore_status:
                            expiry_str = restore_status.split('expiry-date="')[-1].split('"')[0]
                            try:
                                expiry_utc = datetime.strptime(expiry_str, "%a, %d %b %Y %H:%M:%S %Z")
                                gmt8 = expiry_utc.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                                expiry = gmt8.strftime("%Y-%m-%d %H:%M:%S GMT+8")
                            except:
                                expiry = expiry_str or "Unknown"

                        try:
                            url = s3.generate_presigned_url(
                                'get_object',
                                Params={'Bucket': bucket, 'Key': key},
                                ExpiresIn=3600
                            )
                        except:
                            url = "Error generating URL"

                        restored_data.append({
                            "File Key": key,
                            "Storage Class": storage_class,
                            "Expires (GMT+8)": expiry,
                            "Download URL": f"<a href='{url}' target='_blank'>{url}</a>"
                        })

            if restored_data:
                df = pd.DataFrame(restored_data)
                st.success(f"✅ Found {len(restored_data)} restored file(s).")

                # Show table with full clickable URL
                st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)
            else:
                st.warning("⚠️ No restored files found.")
        except Exception as e:
            st.error(f"❌ Error: {e}")
