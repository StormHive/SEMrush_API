import os
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv('API_KEY')
BASE_URL = os.getenv('BASE_URL')

def current_timestamp():
    """Returns the current timestamp formatted as a string."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def fetch_organic_results(keyword):
    """Fetches organic search results for a given keyword from SEMrush API.

    Args:
        keyword (str): The keyword to fetch data for.

    Returns:
        pd.DataFrame: The DataFrame containing organic results or an error message.
    """
    params = {
        'type': 'phrase_organic',
        'key': API_KEY,
        'phrase': keyword,
        'database': 'us',
        'display_limit': 10
    }
    response = requests.get(f"{BASE_URL}/", params=params)
    if response.ok:
        try:
            data = pd.read_csv(StringIO(response.text), delimiter=';')
            if not data.empty:
                data['Keyword'] = keyword
                return data
            else:
                return pd.DataFrame({'Keyword': [keyword], 'Message': response.text})
        except Exception as e:
            return pd.DataFrame({'Keyword': [keyword], 'Message': [str(e)]})
    else:
        return pd.DataFrame({'Keyword': [keyword], 'Message': [f"Error {response.status_code}"]})


def fetch_paid_search_keywords(domain):
    params = {
        'type': 'domain_adwords',
        'key': API_KEY,
        'domain': domain,
        'display_sort': 'po_asc',
        'database': 'us',
    }
    response = requests.get(f"{BASE_URL}/?", params=params)
    if response.ok:
        try:
            data = pd.read_csv(StringIO(response.text), delimiter=';')
            if not data.empty:
                data['Domain'] = domain
                return data
            else:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame({'Domain': [domain], 'Message': [str(e)]})
    else:
        return pd.DataFrame({'Domain': [domain], 'Message': [f"Error {response.status_code}"]})
    
def fetch_organic_search_keywords(domain):
    params = {
        'type': 'domain_organic',
        'key': API_KEY,
        'domain': domain,
        'display_sort': 'po_asc',
        'database': 'us',
    }
    response = requests.get(f"{BASE_URL}/?", params=params)
    if response.ok:
        try:
            data = pd.read_csv(StringIO(response.text), delimiter=';')
            if not data.empty:
                data['Domain'] = domain
                return data
            else:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame({'Domain': [domain], 'Message': [str(e)]})
    else:
        return pd.DataFrame({'Domain': [domain], 'Message': [f"Error {response.status_code}"]})

def main():
    keywords = [
        "Systemic Mastocytosis community support", "Systemic Mastocytosis resources",
        "AYVAKIT patient reviews", "AYVAKIT side effects",
        "Managing Systemic Mastocytosis symptoms", "Living with Systemic Mastocytosis",
        "Systemic Mastocytosis symptom management", "Systemic Mastocytosis patient experience",
        "Effective treatments for Systemic Mastocytosis", "Advanced Systemic Mastocytosis treatments"
    ]
    organic_results = pd.DataFrame()
    paid_search_keywords = pd.DataFrame()
    organic_search_keywords = pd.DataFrame()
    timestamp = current_timestamp()

    for keyword in keywords:
        results = fetch_organic_results(keyword)
        if not results.empty and 'Domain' in results.columns:
            organic_results = pd.concat([organic_results, results], ignore_index=True)
            for domain in results['Domain'].unique():
                organic_sources = fetch_organic_search_keywords(domain)
                paid_sources = fetch_paid_search_keywords(domain)
                if not paid_sources.empty:
                    paid_sources['domain'] = domain  
                    paid_search_keywords = pd.concat([paid_search_keywords, paid_sources], ignore_index=True)
                if not organic_sources.empty:
                    organic_sources['domain'] = domain  
                    organic_search_keywords = pd.concat([organic_search_keywords, organic_sources], ignore_index=True)
        else:
            no_data_row = pd.DataFrame({
                'Keyword': [keyword],
                'Message': [str(results["Message"]).split('\n')[0]]
            })
            organic_results = pd.concat([organic_results, no_data_row], ignore_index=True)

    organic_results.to_csv(f'organic_results_{timestamp}.csv', index=False)
    paid_search_keywords.to_csv(f'paid_search_keywords_{timestamp}.csv', index=False)
    organic_search_keywords.to_csv(f'organic_search_keywords_{timestamp}.csv', index=False)


    print("Data fetching and CSV export completed successfully.")

if __name__ == "__main__":
    main()
