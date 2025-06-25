import requests
import os
import time
import sys
from typing import List, Tuple
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

def validate_reoon_api_key():
    """
    Validates the Reoon API key before attempting bulk email verification.
    Raises an exception if the API key is invalid.
    """
    api_key = os.getenv("REOON_API_KEY")
    if not api_key:
        raise ValueError("REOON_API_KEY must be set in .env file.")
    
    url = "https://emailverifier.reoon.com/api/v1/create-bulk-verification-task/"  # Single email verification endpoint
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'User-Agent': 'Vikram-Automation/1.0'
    }
    
    # Update the payload to match their expected format
    payload = {
        "api_key": api_key,
        "email": "test@example.com"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code != 200:
            raise Exception(f"API key test failed (Status {response.status_code}): {response.text}")
        
        print("✅ Reoon API key validated successfully.")
    except Exception as e:
        error_msg = f"""
VERIFICATION FAILED
-------------------
Error: {str(e)}

Recommended Action:
1. Verify REOON_API_KEY in .env matches exactly what Reoon provided
2. Check your Reoon account to ensure the API key is active
3. Confirm your subscription hasn't expired
"""
        print(error_msg, file=sys.stderr)
        raise Exception(f"Reoon API key validation failed: {e}")



def verify_emails(contacts: List[dict]) -> Tuple[List[dict], List[dict]]:
    """
    Verifies emails with detailed logging and error handling
    """
    print("\n=== Email Verification ===")
    print(f"Starting verification for {len(contacts)} contacts")
    
    api_key = os.getenv("REOON_API_KEY")
    if not api_key:
        raise ValueError("❌ Missing REOON_API_KEY in environment variables")

    url = "https://emailverifier.reoon.com/api/v1/create-bulk-verification-task/"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'User-Agent': 'Apollo-Instantly-Automation/1.0'
    }

    # Prepare data
    email_list = [contact['email'] for contact in contacts]
    contact_map = {contact['email']: contact for contact in contacts}
    
    print(f"ℹ️ First 5 emails for verification: {email_list[:5]}")

    valid_contacts = []
    invalid_contacts = []
    batch_size = 100  # Reoon's max batch size
    
    try:
        for i in range(0, len(email_list), batch_size):
            batch = email_list[i:i + batch_size]
            print(f"\n🔍 Verifying batch {i//batch_size + 1} (emails {i+1}-{min(i+batch_size, len(email_list))})")
            
            # Create verification task
            task_payload = {
                "task": "apollo_automation",
                "emails": batch,
                "key": api_key
            }
            
            start_time = time.time()
            create_response = requests.post(url, headers=headers, json=task_payload, timeout=45)
            create_response.raise_for_status()
            
            task_data = create_response.json()
            task_id = task_data.get('task_id')
            if not task_id:
                raise Exception("No task ID returned from Reoon API")
                
            print(f"ℹ️ Verification task created (ID: {task_id})")
            print("⏳ Waiting for verification to complete...")

            results_url = f'https://emailverifier.reoon.com/api/v1/get-result-bulk-verification-task/'
            results_params = {
                'key': api_key,
                'task-id': task_id
            }

            # Poll for completion (max 18 tries, 10s each = 3 minutes)
            max_tries = 18
            for attempt in range(max_tries):
                time.sleep(10)
                results_response = requests.get(results_url, params=results_params)
                results_response.raise_for_status()
                results_data = results_response.json()
                if results_data.get('status') == 'completed':
                    break
                print(f"⏳ Still running... ({attempt+1}/{max_tries})")
            else:
                raise Exception(f"Verification not completed after {max_tries*10} seconds. Status: {results_data.get('status')}")

            # Process results
            batch_valid = 0
            batch_invalid = 0
            results = results_data.get('results', {})

            for email, verification in results.items():
                contact = contact_map[email]
                if verification.get('is_safe_to_send') and verification.get('is_deliverable'):
                    valid_contacts.append(contact)
                    batch_valid += 1
                else:
                    reason = verification.get('status', 'unknown')
                    invalid_contacts.append({
                        'email': email, 
                        'reason': reason,
                        'details': f"Score: {verification.get('overall_score')}, Status: {reason}"
                    })
                    batch_invalid += 1

            print(f"✔️ Batch results: {batch_valid} valid, {batch_invalid} invalid")
            print(f"📊 Cumulative totals: {len(valid_contacts)} valid, {len(invalid_contacts)} invalid")
            time.sleep(2)
            
    except requests.exceptions.RequestException as e:
        error_msg = f"""
❌ Reoon API request failed:
Error: {str(e)}
URL: {e.request.url if hasattr(e, 'request') else 'Unknown'}
Status: {e.response.status_code if hasattr(e, 'response') else 'N/A'}
Response: {e.response.text if hasattr(e, 'response') else 'None'}
"""
        raise Exception(error_msg)
    except Exception as e:
        raise Exception(f"❌ Verification failed: {str(e)}")

    print("\n=== Verification Complete ===")
    print(f"✅ Total valid emails: {len(valid_contacts)}")
    print(f"❌ Total invalid emails: {len(invalid_contacts)}")
    print(f"📈 Success rate: {(len(valid_contacts)/len(contacts))*100:.1f}%")
    
    return valid_contacts, invalid_contacts