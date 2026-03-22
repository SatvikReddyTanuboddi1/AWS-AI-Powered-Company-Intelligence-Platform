import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

s3 = boto3.client('s3')

BUCKET = 'company-intel-datalake-satvik'
HEADERS = {'User-Agent': 'CompanyIntelPlatform satvik@email.com'}


def fetch_company_filings(ticker, company_name, max_pages=3):
    """
    Fetch recent SEC filings with pagination.
    
    EDGAR search returns ~10-20 results per page. We paginate
    up to max_pages to catch filings from prolific filers.
    """
    
    today = datetime.utcnow().strftime('%Y-%m-%d')
    start_date = (datetime.utcnow() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    encoded_query = urllib.parse.quote(f'"{ticker}" OR "{company_name}"')
    
    all_hits = []
    total_results = 0
    
    for page_from in range(0, max_pages * 20, 20):
        search_url = (
            f'https://efts.sec.gov/LATEST/search-index'
            f'?q={encoded_query}'
            f'&forms=10-K,10-Q,8-K'
            f'&dateRange=custom'
            f'&startdt={start_date}'
            f'&enddt={today}'
            f'&from={page_from}'
        )
        
        req = urllib.request.Request(search_url, headers=HEADERS)
        
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode('utf-8'))
                
            hits = data.get('hits', {}).get('hits', [])
            total_results = data.get('hits', {}).get('total', {}).get('value', 0)
            
            if not hits:
                break
                
            all_hits.extend(hits)
            print(f"Page {page_from // 20 + 1}: fetched {len(hits)} filings")
            
            if len(all_hits) >= total_results:
                break
                
        except Exception as e:
            print(f"Error fetching filings page {page_from}: {e}")
            break
    
    return {
        'total_available': total_results,
        'total_fetched': len(all_hits),
        'hits': all_hits
    }


def fetch_company_facts(cik):
    """
    Fetch structured financial facts from EDGAR XBRL API.
    
    Extracts key financial metrics (revenue, net income, assets, EPS)
    rather than downloading the full payload and discarding it.
    """
    
    cik_padded = str(cik).zfill(10)
    url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json'
    
    req = urllib.request.Request(url, headers=HEADERS)
    
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"Error fetching company facts: {e}")
        return None
    
    us_gaap = data.get('facts', {}).get('us-gaap', {})
    
    def get_recent_values(metric_name, max_entries=8):
        metric = us_gaap.get(metric_name, {})
        units = metric.get('units', {})
        
        values = units.get('USD', units.get('USD/shares', []))
        if not values:
            return []
        
        annual = [
            {
                'value': v.get('val'),
                'end_date': v.get('end'),
                'filed': v.get('filed'),
                'form': v.get('form')
            }
            for v in values
            if v.get('form') in ('10-K', '10-Q')
        ]
        
        # Deduplicate by end_date — prefer 10-K over 10-Q for same period
        seen = {}
        for entry in annual:
            end = entry.get('end_date', '')
            if end not in seen or entry.get('form') == '10-K':
                seen[end] = entry
        
        deduped = sorted(seen.values(), key=lambda x: x.get('end_date', ''), reverse=True)
        return deduped[:max_entries]
    
    facts_summary = {
        'entity_name': data.get('entityName'),
        'cik': data.get('cik'),
        'facts_categories': list(data.get('facts', {}).keys()),
        'financials': {
            'revenue': get_recent_values('Revenues') or get_recent_values('RevenueFromContractWithCustomerExcludingAssessedTax'),
            'net_income': get_recent_values('NetIncomeLoss'),
            'total_assets': get_recent_values('Assets'),
            'stockholders_equity': get_recent_values('StockholdersEquity'),
            'eps_basic': get_recent_values('EarningsPerShareBasic'),
            'eps_diluted': get_recent_values('EarningsPerShareDiluted'),
            'operating_income': get_recent_values('OperatingIncomeLoss'),
            'cash_and_equivalents': get_recent_values('CashAndCashEquivalentsAtCarryingValue'),
        }
    }
    
    return facts_summary


def fetch_cik_lookup(ticker):
    """Get CIK number for a ticker symbol."""
    
    url = 'https://www.sec.gov/files/company_tickers.json'
    req = urllib.request.Request(url, headers=HEADERS)
    
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            
        for entry in data.values():
            if entry.get('ticker', '').upper() == ticker.upper():
                return entry.get('cik_str')
        return None
    except Exception as e:
        print(f"Error in CIK lookup: {e}")
        return None


def lambda_handler(event, context):
    """
    Main handler. Expects event like:
    {"ticker": "TSLA", "company_name": "Tesla"}
    """
    
    ticker = event.get('ticker', 'TSLA')
    company_name = event.get('company_name', 'Tesla')
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    print(f"Collecting SEC data for {company_name} ({ticker})")
    
    # 1. Look up the CIK number
    cik = fetch_cik_lookup(ticker)
    print(f"CIK for {ticker}: {cik}")
    
    # 2. Fetch recent filings with pagination
    filings_data = fetch_company_filings(ticker, company_name)
    print(f"Fetched {filings_data['total_fetched']} of {filings_data['total_available']} available filings")
    
    # 3. Fetch structured financial facts with actual metric extraction
    facts_summary = None
    if cik:
        facts_summary = fetch_company_facts(cik)
        if facts_summary:
            print(f"Extracted financials for {facts_summary.get('entity_name')}")
        else:
            print(f"No financial facts available for CIK {cik}")
    
    # 4. Build the complete data package
    data_package = {
        'ticker': ticker,
        'company_name': company_name,
        'cik': cik,
        'collected_at': timestamp,
        'source': 'sec_edgar',
        'filings': filings_data,
        'facts_summary': facts_summary
    }
    
    # 5. Store in S3 bronze zone
    s3_key = f'bronze/sec-filings/{ticker}/{timestamp}.json'
    
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=json.dumps(data_package, default=str),
            ContentType='application/json'
        )
        print(f"Stored data at s3://{BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Failed to write to S3: {e}")
        raise
    
    # 6. Return metadata for Step Functions to pass downstream
    return {
        'statusCode': 200,
        'ticker': ticker,
        'company_name': company_name,
        'cik': cik,
        'filings_fetched': filings_data['total_fetched'],
        'filings_available': filings_data['total_available'],
        'has_financials': facts_summary is not None,
        's3_key': s3_key,
        'collected_at': timestamp
    }