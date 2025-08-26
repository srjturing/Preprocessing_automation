import pandas as pd
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Define the service account credentials and scope
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = 'turing-genai-ws-58339643dd3f.json'  # Update with your JSON path

# Authenticate and build the Drive service
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

# Folder ID to scan recursively
FOLDER_ID = '1gBMdlAC6CfDfOCe275QDrOaLoGwjlPVj'  # Replace with your root folder ID

# Supported image extensions
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.jfif', '.heic', '.svg']

def has_image_extension(file_name):
    return any(file_name.strip().lower().endswith(ext) for ext in IMAGE_EXTENSIONS)

def get_folder_details(folder_id, cache):
    """Get folder details including name and parent, with comprehensive error handling"""
    if folder_id in cache:
        return cache[folder_id]
    
    try:
        res = service.files().get(
            fileId=folder_id, 
            fields="id, name, parents",
            supportsAllDrives=True
        ).execute()
        
        name = res.get("name", "Unknown")
        parent_ids = res.get("parents", [])
        parent_id = parent_ids[0] if parent_ids else None
        
        cache[folder_id] = {"name": name, "parent": parent_id}
        return cache[folder_id]
        
    except Exception as e:
        print(f"Warning: Could not get details for folder {folder_id}: {e}")
        cache[folder_id] = {"name": "Unknown", "parent": None}
        return cache[folder_id]

def build_complete_filepath(folder_id, filename, cache):
    """Build complete filepath from top-most parent to filename"""
    path_parts = []
    current_folder = folder_id
    visited_folders = set()  # Prevent infinite loops
    
    # Build path from current folder up to the very top parent
    while current_folder and current_folder not in visited_folders:
        visited_folders.add(current_folder)
        details = get_folder_details(current_folder, cache)
        
        if details["name"] != "Unknown":
            path_parts.insert(0, details["name"])
        
        # Move to parent folder
        current_folder = details["parent"]
        
        # If no parent, we've reached the top
        if not current_folder:
            break
    
    # Add the filename at the end
    if filename:
        path_parts.append(filename)
    
    return "/".join(path_parts)

def get_folder_name(service, folder_id, cache):
    if folder_id in cache:
        return cache[folder_id]
    try:
        folder = service.files().get(fileId=folder_id, fields="name").execute()
        name = folder.get('name', 'Unknown')
        cache[folder_id] = name
        return name
    except Exception as e:
        print(f"Error getting folder name for {folder_id}: {e}")
        return 'Unknown'

def list_images_recursively(service, folder_id):
    image_records = []
    folders_to_check = [folder_id]
    folder_name_cache = {}
    folder_cache = {}
    
    while folders_to_check:
        current_folder = folders_to_check.pop()
        folder_name = get_folder_name(service, current_folder, folder_name_cache)
        print(f":mag: Scanning folder: {folder_name} ({current_folder})")
        
        page_token = None
        while True:
            try:
                response = service.files().list(
                    q=f"'{current_folder}' in parents",
                    fields="nextPageToken, files(id, name, mimeType, parents)",
                    pageToken=page_token,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True
                ).execute()
                
                for item in response.get('files', []):
                    file_id = item.get('id')
                    file_name = item.get('name', '').strip()
                    mime_type = item.get('mimeType', '')
                    parent_ids = item.get('parents', [])
                    
                    if mime_type == 'application/vnd.google-apps.folder':
                        folders_to_check.append(file_id)
                    elif mime_type.startswith('image/') or has_image_extension(file_name):
                        parent_id = parent_ids[0] if parent_ids else current_folder
                        
                        # Build complete filepath including filename
                        complete_filepath = build_complete_filepath(parent_id, file_name, folder_cache)
                        
                        # Also build folder path only (without filename)
                        folder_path = build_complete_filepath(parent_id, None, folder_cache)
                        
                        image_link = f"https://drive.google.com/file/d/{file_id}/view"
                        
                        metadata_dict = {
                            "image_name": file_name,
                            "image_link": image_link,
                            "domain": folder_name.split("/")[-1],
                        }
                        
                        image_records.append({
                            "metadata": json.dumps(metadata_dict, ensure_ascii=False),
                            "image": image_link,
                            "prompt": ".",
                            "model_a": ".",
                            "image_name": file_name
                        })
                        
                        print(f"  Found image: {complete_filepath}")
                
                page_token = response.get('nextPageToken', None)
                if not page_token:
                    break
                    
            except Exception as e:
                print(f"Error scanning folder {current_folder}: {e}")
                break
    
    return image_records

# Run the scan
print("Starting Google Drive image scan...")
print(f"Scanning from folder ID: {FOLDER_ID}")
print("Building complete paths from top-most parent to filename...")
records = list_images_recursively(service, FOLDER_ID)

# Save to CSV
if records:
    df = pd.DataFrame(records)
    df.to_csv("extracted_images_with_metadata.csv", index=False)
    print(f"\n:white_check_mark: {len(records)} image(s) saved to 'extracted_images_with_metadata.csv'")
    print("\nColumns in the CSV:")
    for col in df.columns:
        print(f"  - {col}")
else:
    print("\n:warning: No images found in the folder or nested folders.")