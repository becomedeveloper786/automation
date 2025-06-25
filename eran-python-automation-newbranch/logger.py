import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from typing import Set, List, Dict, Any
import time

import gspread
from google.oauth2.service_account import Credentials  
import os
from typing import Set, List, Dict, Any
import time

import os
from google.oauth2.service_account import Credentials
import gspread

class GoogleSheetManager:
    def __init__(self, sheet_id: str):
        try:
            # Load credentials from ENV variables (not file)
            creds_dict = {
                "type": os.getenv("GOOGLE_TYPE"),
                "project_id": os.getenv("GOOGLE_PROJECT_ID"),
                "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
                "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),  # Fix newlines
                "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
                "client_id": os.getenv("GOOGLE_CLIENT_ID"),
                "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
                "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
                "auth_provider_x509_cert_url": os.getenv("GOOGLE_CERT_URL"),
                "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL")
            }
            
            creds = Credentials.from_service_account_info(creds_dict)  # Use dict, not file
            client = gspread.authorize(creds)
            
            spreadsheet = client.open_by_key(sheet_id)
            self.log_sheet = spreadsheet.worksheet("Log")
            # ... rest of your code ...
            
        except Exception as e:
            raise Exception(f"❌ Failed to initialize GoogleSheetManager: {e}")

    def get_processed_emails(self) -> Set[str]:
        """Retrieves all emails from the 'Processed Leads' sheet for deduplication."""
        try:
            print("Fetching previously processed emails for deduplication...")
            emails = self.processed_sheet.col_values(1)[1:]  # Get all values from the first column, skipping header
            print(f"Found {len(emails)} previously processed emails.")
            return set(emails)
        except Exception as e:
            print(f"Warning: Could not fetch processed emails. Deduplication may not work. Error: {e}")
            return set()

    def log_run_summary(self, status: str, counts: Dict[str, Any], error_msg: str = ""):
        """Logs a summary of the automation run to the 'Log' sheet."""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        row = [
            timestamp,
            status,
            counts.get('fetched', 0),
            counts.get('new', 0),
            counts.get('verified', 0),
            counts.get('uploaded', 0),
            counts.get('deleted', 0),
            error_msg
        ]
        self.log_sheet.append_row(row)

    def log_processed_leads(self, leads: List[Dict[str, Any]]):
        """Logs successfully processed leads to the 'Processed Leads' sheet."""
        if not leads:
            return
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        rows = []
        for lead in leads:
            rows.append([
                lead['email'],
                lead.get('first_name', ''),
                lead.get('last_name', ''),
                lead.get('company', ''),
                lead.get('linkedin', ''),
                timestamp
            ])
        self.processed_sheet.append_rows(rows)
        print(f"Logged {len(rows)} new leads to the 'Processed Leads' sheet.")

    def log_invalid_leads(self, leads: List[Dict[str, Any]]):
        """Logs invalid/undeliverable leads to the 'Invalid Leads' sheet."""
        if not leads:
            return
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        rows = []
        for lead in leads:
            rows.append([lead['email'], lead.get('reason', 'failed verification'), timestamp])
        self.invalid_sheet.append_rows(rows)
        print(f"Logged {len(rows)} invalid leads to the 'Invalid Leads' sheet.")
