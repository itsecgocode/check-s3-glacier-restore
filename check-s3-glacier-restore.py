
import streamlit as st
import boto3
from botocore.config import Config
from datetime import timezone, timedelta, datetime
import pandas as pd
import time

st.set_page_config(page_title="S3 Glacier Toolkit", layout="wide")
st.title("📦 S3 Glacier Toolkit")

# --- Mode Selection ---
mode = st.radio("Select Mode", ["Check Restored Files", "Restore Latest Files"])

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"

def check_restore_status(s3, bucket, keys):
    current_status = []
    download_links = []
    for key in keys:
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            restore_status = head.get('Restore')
            storage_class = head.get('StorageClass', 'STANDARD')

            if restore_status and 'ongoing-request="false"' in restore_status:
                status_text = "✅ Restored"
                try:
                    url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=3600)
                except:
                    url = "Error generating URL"
            elif restore_status and 'ongoing-request="true"' in restore_status:
                status_text = "⏳ Restoring..."
                url = ""
            else:
                status_text = "⌛ Not restoring or not Glacier"
                url = ""
        except Exception as e:
            status_text = f"❌ Error: {str(e)}"
            url = ""

        current_status.append((key, storage_class, status_text))
        download_links.append(url)

    df_status = pd.DataFrame(current_status, columns=["File Key", "Storage Class", "Restore Status"])
    df_status["Download Link"] = download_links
    return df_status

# --- AWS Credentials Form ---
with st.form("credentials_form"):
    access_key = st.text_input("🔑 AWS Access Key ID")
    secret_key = st.text_input("🔐 AWS Secret Access Key")
    region = st.text_input("🌍 AWS Region", value="ap-southeast-1")
    bucket = st.text_input("🪣 S3 Bucket Name")
    prefix = st.text_input("📁 Folder Prefix (e.g. logs/2025/ or blank for full bucket)", value="")
    submitted = st.form_submit_button("Continue")

if submitted:
    st.session_state.aws_access_key = access_key
    st.session_state.aws_secret_key = secret_key
    st.session_state.aws_region = region
    st.session_state.aws_bucket = bucket
    st.session_state.aws_prefix = prefix
    st.session_state.restore_triggered = False
    st.session_state.manual_refresh_clicked = False
    st.session_state.restore_results = None
    st.session_state.sorted_files = []

if 'aws_access_key' in st.session_state:
    access_key = st.session_state.aws_access_key
    secret_key = st.session_state.aws_secret_key
    region = st.session_state.aws_region
    bucket = st.session_state.aws_bucket
    prefix = st.session_state.aws_prefix

    try:
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        s3 = session.client("s3", config=Config(retries={"max_attempts": 10, "mode": "standard"}))

        if mode == "Check Restored Files":
            st.info("🔍 Counting total files...")
            paginator = s3.get_paginator("list_objects_v2")
            total_files = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                total_files += len(page.get("Contents", []))

            if total_files == 0:
                st.warning("⚠️ No files found with the specified prefix.")
            else:
                st.info(f"📂 Found {total_files} file(s). Scanning for restored Glacier objects...")
                pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
                progress = st.progress(0)
                status = st.empty()
                scanned = 0
                restored_data = []

                for page in pages:
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        scanned += 1
                        progress.progress(min(scanned / total_files, 1.0))
                        status.text(f"🔍 Scanning: {key[-60:]}")
                        try:
                            head = s3.head_object(Bucket=bucket, Key=key)
                        except:
                            continue
                        storage_class = head.get("StorageClass", "STANDARD")
                        restore_status = head.get("Restore")

                        if not storage_class.startswith("GLACIER") and storage_class != "DEEP_ARCHIVE":
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
                                url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=3600)
                            except:
                                url = "Error generating URL"

                            restored_data.append({
                                "File Key": key,
                                "Storage Class": storage_class,
                                "Expires (GMT+8)": expiry,
                                "Download URL": f"<a href='{url}' target='_blank'>{url}</a>"
                            })

                progress.empty()
                status.empty()

                if restored_data:
                    df = pd.DataFrame(restored_data)
                    st.success(f"✅ Found {len(restored_data)} restored file(s).")
                    st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)
                else:
                    st.warning("⚠️ No restored Glacier/Deep Archive files found.")

        elif mode == "Restore Latest Files":
            if not st.session_state.sorted_files:
                paginator = s3.get_paginator('list_objects_v2')
                all_objects = []
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    contents = page.get('Contents', [])
                    for obj in contents:
                        key = obj['Key']
                        size = obj['Size']
                        if not key.endswith('/') and size > 0:
                            all_objects.append(obj)

                sorted_files = sorted(all_objects, key=lambda x: x['LastModified'])[-10:]
                st.session_state.sorted_files = sorted_files

            sorted_files = st.session_state.sorted_files
            if sorted_files:
                with st.form("restore_form"):
                    st.subheader("📝 Select files to restore from Glacier")
                    selected_files = []
                    for f in sorted_files:
                        lm_gmt8 = f['LastModified'].astimezone(timezone(timedelta(hours=8)))
                        size_str = format_size(f['Size'])
                        label = f"{f['Key']} ({size_str}, Uploaded: {lm_gmt8.strftime('%Y-%m-%d %H:%M:%S GMT+8')})"
                        if st.checkbox(label, key=f['Key']):
                            selected_files.append(f['Key'])

                    restore_days = st.number_input("📅 Restore Duration (days)", min_value=1, max_value=30, value=1)
                    retrieval_tier = st.selectbox("⚡ Retrieval Tier", options=["Expedited", "Standard", "Bulk"], index=0)
                    submitted_restore = st.form_submit_button("🔁 Trigger Restore")

                    if submitted_restore:
                        st.session_state.selected_keys = selected_files
                        st.session_state.restore_triggered = True
                        st.session_state.restore_days = restore_days
                        st.session_state.retrieval_tier = retrieval_tier
                        st.session_state.manual_refresh_clicked = False
                        st.session_state.restore_results = None
                        st.rerun()

            if st.session_state.get("restore_triggered") or st.session_state.get("restore_results"):
                if st.session_state.get("restore_triggered") and st.session_state.get("selected_keys"):
                    if not st.session_state.get("restore_results") and not st.session_state.get("manual_refresh_clicked"):
                        results = []
                        for key in st.session_state.selected_keys:
                            try:
                                s3.restore_object(
                                    Bucket=bucket,
                                    Key=key,
                                    RestoreRequest={
                                        'Days': int(st.session_state.restore_days),
                                        'GlacierJobParameters': {'Tier': st.session_state.retrieval_tier}
                                    }
                                )
                                results.append((key, "✅ Restore request submitted"))
                            except Exception as e:
                                results.append((key, f"❌ Failed: {str(e)}"))
                        st.session_state.restore_results = results

                if st.session_state.get("restore_results"):
                    st.subheader("🚀 Submitting restore requests...")
                    df = pd.DataFrame(st.session_state.restore_results, columns=["File Key", "Restore Status"])
                    st.success("Restore requests completed.")
                    st.table(df)
                    manual_refresh = st.button("🔄 Manually Refresh Now")
                    if manual_refresh:
                        st.session_state.manual_refresh_clicked = True
                        st.rerun()

                st.subheader("🔁 Auto-refreshing restore status...")
                status_placeholder = st.empty()
                countdown_placeholder = st.empty()
                table_placeholder = st.empty()
                df_status = check_restore_status(s3, bucket, st.session_state.selected_keys)
                table_placeholder.table(df_status)

                if st.session_state.get("manual_refresh_clicked", False):
                    status_placeholder.info("🔄 Manual refresh complete.")
                    st.session_state.manual_refresh_clicked = False
                    st.stop()

                refresh_interval = 60
                max_checks = 10
                for attempt in range(max_checks):
                    df_status = check_restore_status(s3, bucket, st.session_state.selected_keys)
                    table_placeholder.table(df_status)
                    if all(status == "✅ Restored" for status in df_status["Restore Status"]):
                        status_placeholder.success("🎉 All files are restored.")
                        break
                    for remaining in range(refresh_interval, 0, -1):
                        countdown_placeholder.info(f"🔄 Refreshing in {remaining} seconds...")
                        time.sleep(1)
                countdown_placeholder.empty()
                st.session_state.restore_triggered = False

    except Exception as e:
        st.error(f"❌ Error: {e}")
