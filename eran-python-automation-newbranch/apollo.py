import requests
import os
import time
from typing import List, Dict, Any

def fetch_all_contacts_from_list() -> List[Dict[str, Any]]:
    """
    Fetches contacts from an Apollo list with enhanced logging and error handling.
    """
    api_key = os.getenv("APOLLO_API_KEY")
    list_id = os.getenv("APOLLO_LIST_ID")
    
    # Test mode configuration
    test_mode_pages = os.getenv("TEST_MODE_PAGES")
    if test_mode_pages:
        print(f"🔧 TEST MODE ACTIVE: Limiting to {test_mode_pages} pages")
    else:
        print("⚙️ PRODUCTION MODE: Fetching all available pages")

    if not api_key or not list_id:
        raise ValueError("❌ Missing Apollo credentials: APOLLO_API_KEY and APOLLO_LIST_ID must be set")

    if "REPLACE_WITH" in list_id:
        raise ValueError("❌ FATAL: Please replace APOLLO_LIST_ID placeholder with your actual list ID")

    url = "https://api.apollo.io/v1/contacts/search"
    headers = {
        'Content-Type': 'application/json', 
        'Cache-Control': 'no-cache',
        'X-Api-Key': api_key
    }
    
    all_contacts = []
    page = 1
    total_pages = None
    
    print("\n=== Apollo Contact Fetch ===")
    print("Starting contact fetch from Apollo...")
    
    while True:
        if test_mode_pages and page > int(test_mode_pages):
            print(f"\nℹ️ Test mode limit reached ({test_mode_pages} pages). Stopping fetch.")
            break
            
        print(f"\n📄 Processing page {page}...")
        payload = {"q_list_ids": [list_id], "per_page": 100, "page": page}
        
        try:
            start_time = time.time()
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            request_time = time.time() - start_time
            
            data = response.json()
            
            # First time through, get total pages if available
            if total_pages is None:
                total_pages = data.get('pagination', {}).get('total_pages')
                if total_pages:
                    print(f"ℹ️ Total pages available: {total_pages}")
            
            contacts_on_page = data.get('contacts', [])
            print(f"⏱️ Request time: {request_time:.2f}s | Found {len(contacts_on_page)} contacts")
            
            if not contacts_on_page:
                print("\n✅ Reached end of contact list")
                break

            valid_contacts = 0
            for contact in contacts_on_page:
                if contact.get('email'):
                    all_contacts.append({
                        'id': contact.get('id'),
                        'first_name': contact.get('first_name', ''),
                        'last_name': contact.get('last_name', ''),
                        'email': contact['email'],
                        'company': contact.get('organization', {}).get('name', ''),
                        'linkedin': contact.get('linkedin_url', ''),
                        'annual_revenue': contact.get('organization', {}).get('annual_revenue', ''),
                        'company_headcount': contact.get('organization', {}).get('estimated_num_employees', '')
                    })
                    valid_contacts += 1
            
            print(f"✔️ Added {valid_contacts} valid contacts ({(valid_contacts/len(contacts_on_page))*100:.1f}% of page)")
            print(f"📊 Total contacts so far: {len(all_contacts)}")
            
            page += 1
            time.sleep(1)  # Be nice to the API
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = 900  # 15 minutes
                print(f"⚠️ Rate limit hit! Waiting {wait_time/60} minutes before retrying...")
                time.sleep(wait_time)
                continue  # Retry the same page
            
            error_msg = f"""
❌ HTTP Error {e.response.status_code} on page {page}
URL: {url}
Response: {e.response.text}
"""
            if e.response.status_code == 404:
                error_msg += "\nPossible causes:\n- Invalid list ID\n- API endpoint changed"
            raise Exception(error_msg)
            
        except Exception as e:
            raise Exception(f"❌ Unexpected error on page {page}: {str(e)}")

    print(f"\n=== Fetch Complete ===")
    print(f"✅ Successfully fetched {len(all_contacts)} contacts from {page-1} pages")
    if test_mode_pages:
        print(f"NOTE: Results limited by test mode ({test_mode_pages} pages)")
    
    return all_contacts