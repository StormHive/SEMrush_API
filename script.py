import requests
import pandas as pd
from io import StringIO
from datetime import datetime


API_KEY = '2aa6752707d345499726c8b10b3c394a'
BASE_URL = 'https://api.semrush.com'

keywords = [
    "Systemic Mastocytosis community support", "Systemic Mastocytosis resources",
    "AYVAKIT patient reviews", "AYVAKIT side effects",
    "Managing Systemic Mastocytosis symptoms", "Living with Systemic Mastocytosis",
    "Systemic Mastocytosis symptom management", "Systemic Mastocytosis patient experience",
    "Effective treatments for Systemic Mastocytosis", "Advanced Systemic Mastocytosis treatments"
]

def current_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def fetch_organic_results(keyword):
    params = {
        'type': 'phrase_organic',
        'key': API_KEY,
        'phrase': keyword,
        'database': 'us',
        'display_limit': 10 
    }
    response = requests.get(f"{BASE_URL}/?", params=params)
    print(f"Requesting: {response.url}")
    print("Raw response text:", response.text) 

    if response.status_code == 200 and response.text:
        data = pd.read_csv(StringIO(response.text), delimiter=';')
        if not data.empty:
            data['Keyword'] = keyword 
            return data
        else:
            print(f"No data found for keyword '{keyword}'")
            return pd.DataFrame({'Keyword': [keyword], 'Message': ['SEMrush Error 50 :No data found']})
    else:
        print(f"Error fetching organic results for keyword '{keyword}': {response.status_code}")
        return pd.DataFrame({'Keyword': [keyword], 'Message': [f"Error {response.status_code}"]}) 
    

def fetch_traffic_sources(domain):
    params = {
        'target' : domain,
        'key': API_KEY,
    }
    response = requests.get("https://api.semrush.com/analytics/ta/api/v3/sources?target=medium.com&device_type=mobile&display_limit=5&display_offset=0&country=us&sort_order=traffic_diff&traffic_channel=referral&traffic_type=organic&display_date=2020-06-01&export_columns=target,from_target,display_date,country,traffic_share,traffic,channel&key=2aa6752707d345499726c8b10b3c394a")
    if response.status_code == 200 and response.text:
        try:
            data = pd.read_csv(StringIO(response.text), delimiter=';')
            if not data.empty:
                data['Domain'] = domain
                return data
            else:
                print(f"No data found for domain '{domain}'")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error parsing CSV response for domain '{domain}': {e}")
            return pd.DataFrame()
    else:
        print(f"Error fetching traffic sources for domain '{domain}': {response.status_code}")
        return pd.DataFrame({'Domain': [domain], 'Message': [f"Error {response.status_code}"]})

organic_results = pd.DataFrame()
traffic_sources = pd.DataFrame()
timestamp = current_timestamp()


for keyword in keywords:
    results = fetch_organic_results(keyword)
    if not results.empty and 'Domain' in results.columns:
        organic_results = pd.concat([organic_results, results], ignore_index=True)
        for domain in results['Domain'].unique():
            sources = fetch_traffic_sources(domain)
            if not sources.empty:
                sources['domain'] = domain  
                traffic_sources = pd.concat([traffic_sources, sources], ignore_index=True)
                
    else:
        no_data_row = pd.DataFrame({
            'Keyword': [keyword],
            'Message': ['No data found']
        })
        organic_results = pd.concat([organic_results, no_data_row], ignore_index=True)

organic_results.to_csv(f'organic_results_{timestamp}.csv', index=False)
traffic_sources.to_csv(f'traffic_sources_{timestamp}.csv', index=False)

print("Data fetching and CSV export completed successfully.")



