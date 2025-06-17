import os
import requests
import pandas as pd
import re
import time
from datetime import datetime
from supabase import create_client, Client

def collect_and_upload_data():
    """Collect Parliament data and upload to Supabase"""
    
    # Initialize Supabase client
    supabase_url = os.environ['SUPABASE_URL']
    supabase_key = os.environ['SUPABASE_KEY']
    supabase: Client = create_client(supabase_url, supabase_key)
    
    print("🏛️ Starting Parliament data collection...")
    
    # Your existing collection code (simplified for automation)
    all_records = []
    url = "https://interests-api.parliament.uk/api/v1/Interests"
    
    # Collect first 1000 records per day (we can increase this later)
    batch_size = 50
    max_records = 1000
    
    for batch in range(20):  # 20 batches of 50 = 1000 records
        skip = batch * batch_size
        params = {"take": batch_size, "skip": skip}
        
        print(f"Collecting batch {batch+1}/20: records {skip+1}-{skip+batch_size}")
        
        try:
            response = requests.get(url, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                
                if not data['items']:
                    print("✅ No more records available")
                    break
                
                for item in data['items']:
                    summary = item['summary']
                    amounts = re.findall(r'£([\d,]+\.?\d*)', summary)
                    companies = re.findall(r'([^*]+?)\s*-\s*£', summary)
                    clean_companies = [c.strip().lstrip('*').strip() for c in companies if c.strip()]
                    
                    record = {
                        'interest_id': item['id'],
                        'mp_name': item['member']['nameDisplayAs'],
                        'mp_id': item['member']['id'],
                        'constituency': item['member'].get('memberFrom', ''),
                        'party': item['member'].get('party', ''),
                        'category': item['category']['name'],
                        'date_registered': item['registrationDate'],
                        'summary': summary,
                        'companies': ' | '.join(clean_companies),
                        'amounts': ' | '.join(amounts),
                        'company_count': len(clean_companies),
                        'collected_at': datetime.now().isoformat()
                    }
                    all_records.append(record)
                
                print(f"✅ Got {len(data['items'])} records")
                
            else:
                print(f"❌ Error {response.status_code}")
                break
                
        except Exception as e:
            print(f"❌ Error: {e}")
            continue
        
        time.sleep(3)  # Be polite to the API
    
    # Upload to Supabase
    if all_records:
        print(f"📤 Uploading {len(all_records)} records to Supabase...")
        
        try:
            # Insert data (upsert to handle duplicates)
            result = supabase.table('parliament_interests').upsert(all_records).execute()
            print(f"✅ Successfully uploaded {len(all_records)} records!")
            
        except Exception as e:
            print(f"❌ Upload error: {e}")
    
    else:
        print("❌ No records to upload")

if __name__ == "__main__":
    collect_and_upload_data()
