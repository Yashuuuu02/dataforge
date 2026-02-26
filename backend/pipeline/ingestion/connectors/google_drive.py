"""Google Drive connector — OAuth2 flow and file download."""

import logging
import os
import tempfile
from io import BytesIO
from typing import Optional

from app.core.minio_client import upload_file as minio_upload

logger = logging.getLogger(__name__)

# OAuth2 scopes for Drive read-only
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
REDIRECT_URI = os.environ.get("GOOGLE_REDIRECT_URI", "urn:ietf:wg:oauth:2.0:oob")


class GoogleDriveConnector:
    """OAuth2-based Google Drive file download."""

    @staticmethod
    def get_auth_url() -> str:
        """Generate the OAuth2 authorization URL for user consent."""
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
        except ImportError:
            raise RuntimeError("google-auth-oauthlib not installed")

        if not CLIENT_ID or not CLIENT_SECRET:
            raise ValueError("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set")

        flow = InstalledAppFlow.from_client_config(
            {
                "installed": {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [REDIRECT_URI],
                }
            },
            scopes=SCOPES,
        )
        auth_url, _ = flow.authorization_url(prompt="consent")
        return auth_url

    @staticmethod
    def download_file(
        auth_code: str,
        file_id: str,
        minio_bucket: str = "dataforge-raw",
    ) -> dict:
        """Exchange auth code for credentials and download a file.

        Returns: {minio_key, filename, size_bytes}
        """
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaIoBaseDownload
        except ImportError:
            raise RuntimeError("google-api-python-client not installed")

        flow = InstalledAppFlow.from_client_config(
            {
                "installed": {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [REDIRECT_URI],
                }
            },
            scopes=SCOPES,
        )
        flow.fetch_token(code=auth_code)
        credentials = flow.credentials

        service = build("drive", "v3", credentials=credentials)

        # Get file metadata
        file_meta = service.files().get(fileId=file_id, fields="name, size, mimeType").execute()
        filename = file_meta.get("name", f"gdrive_{file_id}")
        mime_type = file_meta.get("mimeType", "application/octet-stream")

        # Download file
        request = service.files().get_media(fileId=file_id)
        buffer = BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        buffer.seek(0)
        size = buffer.getbuffer().nbytes
        minio_key = f"gdrive-import/{filename}"

        minio_upload(minio_bucket, minio_key, buffer, length=size, content_type=mime_type)

        logger.info("Downloaded from Google Drive: %s (%d bytes)", filename, size)
        return {
            "minio_key": minio_key,
            "filename": filename,
            "size_bytes": size,
        }

    @staticmethod
    def list_folder(auth_code: str, folder_id: str) -> list[dict]:
        """List files in a Google Drive folder."""
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
            from googleapiclient.discovery import build
        except ImportError:
            raise RuntimeError("google-api-python-client not installed")

        flow = InstalledAppFlow.from_client_config(
            {
                "installed": {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [REDIRECT_URI],
                }
            },
            scopes=SCOPES,
        )
        flow.fetch_token(code=auth_code)
        credentials = flow.credentials

        service = build("drive", "v3", credentials=credentials)
        results = service.files().list(
            q=f"'{folder_id}' in parents",
            fields="files(id, name, size, mimeType)",
        ).execute()

        return [
            {
                "id": f["id"],
                "name": f["name"],
                "size": int(f.get("size", 0)),
                "mime_type": f.get("mimeType", ""),
            }
            for f in results.get("files", [])
        ]
