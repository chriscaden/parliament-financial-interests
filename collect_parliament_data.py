import requests
import pandas as pd
import re
import time
from datetime import datetime
import os

def collect_parliament_data():
    """Collect all UK Parliament financial interests data"""
    all_records = []
    url = "https://interests-api.parliament.uk/api/v1/Interests"
    
    # Get total count first
    response = requests.get(url, params={"take": 1})
    if response.status_code == 200:
        total_available = response.json().get('totalResults', 0)
        print(f"📊 Total records available: {total_available}")
    else:
        print("❌ Can't determine total records")
        return None
    
    print(f"🚀 Collecting ALL {total_available} records...")
    
    batch_size = 100
    current_skip = 0
    
    while current_skip < total_available:
        params = {"take": batch_size, "skip": current_skip}
        
        print(f"Getting records {current_skip+1} to {min(current_skip+batch_size, total_available)}")
        
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                batch_records = data['items']
                
                if not batch_records:
                    print("✅ No more records available")
                    break
                
                for item in batch_records:
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
                        'category_id': item['category']['id'],
                        'date_registered': item['registrationDate'],
                        'date_published': item['publishedDate'],
                        'summary': summary,
                        'companies': ' | '.join(clean_companies),
                        'amounts': ' | '.join(amounts),
                        'company_count': len(clean_companies),
                        'amount_count': len(amounts),
                        'collected_at': datetime.now().isoformat()
                    }
                    all_records.append(record)
                
                print(f"✅ Collected {len(batch_records)} records (Total: {len(all_records)})")
                current_skip += len(batch_records)
                
            elif response.status_code == 429:
                print("⚠️  Rate limited! Waiting 30 seconds...")
                time.sleep(30)
                continue
            else:
                print(f"❌ Error {response.status_code}")
                break
                
        except Exception as e:
            print(f"❌ Error: {e}")
            break
        
        time.sleep(3)  # Be polite
        
        if len(all_records) > total_available:
            break
    
    return all_records

if __name__ == "__main__":
    print("🏛️ UK Parliament Financial Interests Data Collector")
    print("=" * 50)
    
    records = collect_parliament_data()
    
    if records:
        # Save locally for testing
        df = pd.DataFrame(records)
        df_unique = df.drop_duplicates(subset=['interest_id'])
        
        filename = f"parliament_data_{datetime.now().strftime('%Y%m%d')}.csv"
        df_unique.to_csv(filename, index=False)
        
        print(f"\n🎉 Collection complete!")
        print(f"📊 Total unique records: {len(df_unique)}")
        print(f"💾 Saved to: {filename}")
    else:
        print("❌ No data collected")
