import requests
import os
import time
from typing import List, Dict, Any

# V2 API Base URL
API_BASE_URL = "https://api.instantly.ai/api/v2"

def push_to_instantly(contacts_split: Dict[str, List[Dict[str, Any]]], batch_size: int = 100) -> int:
    """
    Pushes contacts to Instantly.ai campaigns using the V2 API.
    
    Uses the official API endpoints from the documentation:
    - POST /api/v2/leads - For adding new leads
    - POST /api/v2/leads/list - For bulk lead operations

    Args:
        contacts_split: A dictionary containing lists of contacts for each campaign.
                        Example: {'campaign_a': [...], 'campaign_b': [...]}
        batch_size: The number of leads to upload in a single API call.

    Returns:
        The total number of contacts successfully uploaded.
    """
    print("\n=== Instantly.ai V2 API Upload ===")
    
    api_key = os.getenv("INSTANTLY_API_KEY")
    campaign_ids = {
        "campaign_a": os.getenv("CAMPAIGN_PRIMARY_ID"),
        "campaign_b": os.getenv("CAMPAIGN_SECONDARY_ID")
    }

    if not api_key or not all(campaign_ids.values()):
        raise ValueError("❌ Missing Instantly credentials (API Key or Campaign IDs) in environment variables")

    total_uploaded = 0
    
    # Process each campaign specified in the input dictionary
    for campaign_key, contact_list in contacts_split.items():
        campaign_id = campaign_ids.get(campaign_key)
        if not campaign_id:
            print(f"⚠️ Skipping '{campaign_key}' as its campaign ID is not set.")
            continue
            
        campaign_name = "Primary" if campaign_key == 'campaign_a' else "Secondary"
        print(f"\n📤 Uploading to {campaign_name} Campaign (ID: {campaign_id})")
        print(f"ℹ️ Found {len(contact_list)} contacts to upload.")
        
        if not contact_list:
            print("✅ No contacts to upload. Skipping.")
            continue

        # Split contacts into batches
        for i in range(0, len(contact_list), batch_size):
            batch = contact_list[i:i + batch_size]
            
            # Prepare leads payload according to official API specs
            leads_payload = []
            for contact in batch:
                lead_data = {
                    "email": contact['email'],
                    "first_name": contact.get('first_name', ''),
                    "last_name": contact.get('last_name', ''),
                    "company": contact.get('company', ''),
                    "custom_variables": {
                        "linkedin": contact.get('linkedin', '')
                    },
                    "campaign_id": campaign_id
                }
                leads_payload.append(lead_data)

            # Use the official /api/v2/leads endpoint for bulk upload
            endpoint = f"{API_BASE_URL}/leads"
            
            try:
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                
                # For single lead, use the leads endpoint directly
                if len(leads_payload) == 1:
                    response = requests.post(
                        endpoint,
                        json=leads_payload[0],
                        headers=headers
                    )
                else:
                    # For multiple leads, use the leads/list endpoint
                    response = requests.post(
                        f"{API_BASE_URL}/leads/list",
                        json={"leads": leads_payload},
                        headers=headers
                    )
                
                if response.status_code in (200, 201):
                    batch_uploaded_count = len(batch)
                    total_uploaded += batch_uploaded_count
                    print(f"✓ Successfully uploaded batch {i//batch_size + 1} ({batch_uploaded_count} leads). Total uploaded: {total_uploaded}")
                else:
                    print(f"⚠️ Failed to upload batch {i//batch_size + 1} (Status: {response.status_code})")
                    print(f"Response: {response.text[:300]}...")
                
                # Sleep to respect API rate limits between batches
                time.sleep(1) 
                
            except Exception as e:
                print(f"💥 An exception occurred during batch {i//batch_size + 1} upload: {str(e)}")

        print(f"✅ {campaign_name} campaign upload complete.")

    print("\n=== Upload Summary ===")
    total_contacts_to_upload = sum(len(v) for v in contacts_split.values())
    if total_contacts_to_upload > 0:
        success_rate = (total_uploaded / total_contacts_to_upload * 100) if total_contacts_to_upload > 0 else 0
        print(f"📤 Total contacts uploaded: {total_uploaded} / {total_contacts_to_upload}")
        print(f"📊 Success rate: {success_rate:.1f}%")
    else:
        print("No contacts were provided for upload.")
    
    return total_uploaded


def delete_finished_leads() -> int:
    """
    Deletes leads that have 'Finished' status from specified campaigns using the V2 API.
    Uses:
      - POST /api/v2/leads/list (with filters and pagination)
      - POST /api/v2/leads/remove (with emails)
    Returns:
        The total count of deleted leads.
    """
    api_key = os.getenv("INSTANTLY_API_KEY")
    campaign_ids = [os.getenv("CAMPAIGN_PRIMARY_ID"), os.getenv("CAMPAIGN_SECONDARY_ID")]

    if not api_key:
        raise ValueError("❌ Missing INSTANTLY_API_KEY in environment variables")

    deleted_count = 0
    filter_values = ["FILTER_VAL_COMPLETED"]  # Add other filters if needed

    print("\n=== Instantly.ai V2 API Cleanup ===")
    print(f"🗑️ Starting cleanup of leads with filters: {filter_values}")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    for campaign_id in campaign_ids:
        if not campaign_id:
            continue

        print(f"\n🔍 Checking campaign {campaign_id} for leads to delete...")
        emails_to_delete = []
        for filter_val in filter_values:
            starting_after = None
            while True:
                payload = {
                    "campaign": campaign_id,
                    "filter": filter_val,
                    "limit": 100
                }
                if starting_after:
                    payload["starting_after"] = starting_after

                response = requests.post(
                    f"{API_BASE_URL}/leads/list",
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()
                data = response.json()
                leads = data.get('data', [])
                emails_to_delete.extend([lead['email'] for lead in leads])

                # Pagination
                starting_after = data.get("next_starting_after")
                if not starting_after or not leads:
                    break

        if not emails_to_delete:
            print(f"✅ No leads with filters {filter_values} found in campaign {campaign_id}.")
            continue

        print(f"ℹ️ Found {len(emails_to_delete)} leads to delete.")

        # Delete in batches of 100
        for i in range(0, len(emails_to_delete), 100):
            batch = emails_to_delete[i:i+100]
            delete_payload = {
                "campaign_id": campaign_id,
                "emails": batch
            }
            delete_response = requests.post(
                f"{API_BASE_URL}/leads/remove",
                json=delete_payload,
                headers=headers
            )
            if delete_response.status_code == 200:
                deleted_count += len(batch)
                print(f"✅ Successfully deleted {len(batch)} leads from campaign {campaign_id}.")
            else:
                print(f"❌ Error during deletion for campaign {campaign_id}. Status: {delete_response.status_code}, Response: {delete_response.text}")
            time.sleep(1)

    return deleted_count


# Example Usage
if __name__ == "__main__":
    # To run this example:
    # 1. Make sure you have the 'requests' library: pip install requests
    # 2. Set your environment variables before running the script:
    #    export INSTANTLY_API_KEY="your_api_key"
    #    export CAMPAIGN_PRIMARY_ID="your_primary_campaign_id"
    #    export CAMPAIGN_SECONDARY_ID="your_secondary_campaign_id"

    # --- Example 1: Pushing new contacts ---
    print("--- Running Example: Pushing Contacts ---")
    # Create some mock contact data
    mock_contacts = {
        "campaign_a": [
            {"email": "elon.musk@tesla.com", "first_name": "Elon", "company": "Tesla", "linkedin": "elonmusk"},
            {"email": "satya.nadella@microsoft.com", "first_name": "Satya", "company": "Microsoft", "linkedin": "satyanadella"},
        ],
        "campaign_b": [
            {"email": "tim.cook@apple.com", "first_name": "Tim", "company": "Apple", "linkedin": "timcook"},
        ]
    }
    
    try:
        # NOTE: This will make a REAL API call if your environment variables are set.
        # Uncomment the line below to run the push function.
        # uploaded = push_to_instantly(mock_contacts)
        # print(f"\nTotal contacts pushed in this run: {uploaded}")
        print("Example push function is commented out to prevent accidental API calls.")
        print("Uncomment the line in the `if __name__ == '__main__':` block to test.")

    except ValueError as e:
        print(e)
    
    # --- Example 2: Deleting finished leads ---
    print("\n--- Running Example: Deleting Finished Leads ---")
    try:
        # NOTE: This will make a REAL API call if your environment variables are set.
        # Uncomment the line below to run the delete function.
        # deleted = delete_finished_leads()
        # print(f"\nTotal leads deleted in this run: {deleted}")
        print("Example delete function is commented out to prevent accidental API calls.")
        print("Uncomment the line in the `if __name__ == '__main__':` block to test.")
        
    except ValueError as e:
        print(e)