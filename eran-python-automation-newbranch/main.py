import os
import time
import schedule
import traceback # <-- Add this import
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from apollo import fetch_all_contacts_from_list
from reoon import verify_emails
from instantly import push_to_instantly, delete_finished_leads
from logger import GoogleSheetManager
from notifier import send_error_email
from utils import split_contacts

def run_automation_flow():
    """
    The main end-to-end automation workflow.
    """
    print(f"\n{'='*50}\n▶️  Starting Automation Run at {time.strftime('%Y-%m-%d %H:%M:%S')}\n{'='*50}")
    
    sheet_manager = None
    counts = {
        'fetched': 0, 'new': 0, 'verified': 0, 'uploaded': 0, 'deleted': 0
    }

    try:
        # --- 0. Initialize Google Sheet Manager ---
        sheet_manager = GoogleSheetManager(os.getenv("SHEET_ID"))

        # --- 1. Fetch Previously Processed Contacts for Deduplication ---
        processed_emails = sheet_manager.get_processed_emails()

        # --- 2. Pull Fresh Contacts from Apollo.io ---
        all_apollo_contacts = fetch_all_contacts_from_list()
        counts['fetched'] = len(all_apollo_contacts)
        
        # --- 3. Deduplicate Contacts ---
        new_contacts = [
            c for c in all_apollo_contacts if c['email'] and c['email'].lower() not in processed_emails
        ]
        counts['new'] = len(new_contacts)
        print(f"Deduplication complete. Found {counts['new']} new contacts to process.")

        if not new_contacts:
            print("No new contacts to process. Ending run.")
            sheet_manager.log_run_summary(status="Completed (No New Leads)", counts=counts)
            return

        # --- 4. Verify Emails using Reoon.com ---
        valid_contacts, invalid_contacts = verify_emails(new_contacts)
        counts['verified'] = len(valid_contacts)
        
        # Log invalid contacts to their sheet
        if invalid_contacts:
            sheet_manager.log_invalid_leads(invalid_contacts)

        if not valid_contacts:
            print("No valid contacts after verification. Ending run.")
            sheet_manager.log_run_summary(status="Completed (No Valid Leads)", counts=counts)
            return

        # --- 5. Split and Push Validated Contacts to Instantly.ai ---
        split_percent = int(os.getenv("CAMPAIGN_SPLIT_PERCENT", 60))
        split_data = split_contacts(valid_contacts, split_percent)
        
        uploaded_count = push_to_instantly(split_data)
        counts['uploaded'] = uploaded_count

        # --- 6. Log a record of the successfully pushed contacts ---
        sheet_manager.log_processed_leads(valid_contacts)

        # --- 7. Delete Completed/Replied Contacts from Instantly ---
        deleted_count = delete_finished_leads()
        counts['deleted'] = deleted_count

        # --- 8. Log Final Summary ---
        sheet_manager.log_run_summary(status="Success", counts=counts)
        print(f"✅ Automation run completed successfully!")

    except Exception as e:
        # --- UPDATED ERROR HANDLING ---
        # Capture the full, detailed error traceback
        error_traceback = traceback.format_exc()
        error_message = f"Automation failed with an unrecoverable error.\n\n{error_traceback}"
        
        print(f"❌ CRITICAL ERROR:\n{error_message}")
        
        # Log failure to Google Sheet if possible
        if sheet_manager:
            # Log the original, shorter error message to the sheet
            sheet_manager.log_run_summary(status="Failed", counts=counts, error_msg=str(e)[:500])
        
        # Send email notification with the full details
        send_error_email(error_message)

    finally:
        print(f"{'='*50}\n⏹️  Automation Run Finished at {time.strftime('%Y-%m-%d %H:%M:%S')}\n{'='*50}")


if __name__ == "__main__":
    print("Automation script initialized.")
    
    # Run once immediately for testing purposes
    run_automation_flow()
    
    # Schedule the job to run every 24 hours
    schedule.every().day.at("09:00").do(run_automation_flow)
    
    print(f"Script scheduled to run daily at 09:00. Current time: {time.strftime('%H:%M:%S')}")
    print("Waiting for the next scheduled run... (Press Ctrl+C to exit)")

    while True:
        schedule.run_pending()
        time.sleep(60)