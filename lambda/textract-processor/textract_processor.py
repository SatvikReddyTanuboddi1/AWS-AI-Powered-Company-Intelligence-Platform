import json
import re
import time
import boto3
import urllib.request
from datetime import datetime, timezone

s3 = boto3.client('s3')
textract = boto3.client('textract')

BUCKET = 'company-intel-datalake-satvik'
HEADERS = {'User-Agent': 'CompanyIntelPlatform satvik@email.com'}

SECTION_PATTERNS = {
    'risk_factors': re.compile(
        r'item\s+1a[\.\s\-:]+risk\s+factors',
        re.IGNORECASE
    ),
    'mda': re.compile(
        r'item\s+7[\.\s\-:]+management.s?\s+discussion\s+and\s+analysis',
        re.IGNORECASE
    ),
    'business': re.compile(
        r'item\s+1[\.\s\-:]+business(?!\s+risk)',
        re.IGNORECASE
    ),
    'legal_proceedings': re.compile(
        r'item\s+3[\.\s\-:]+legal\s+proceedings',
        re.IGNORECASE
    ),
    'financial_statements': re.compile(
        r'item\s+8[\.\s\-:]+financial\s+statements',
        re.IGNORECASE
    ),
}


def get_filing_url(cik, filing_type='10-K'):
    """Get the URL of the most recent filing document from EDGAR."""
    
    cik_padded = str(cik).zfill(10)
    url = f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
    
    req = urllib.request.Request(url, headers=HEADERS)
    
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"Failed to fetch submissions for CIK {cik}: {e}")
        return None, None
    
    recent = data.get('filings', {}).get('recent', {})
    forms = recent.get('form', [])
    accessions = recent.get('accessionNumber', [])
    primary_docs = recent.get('primaryDocument', [])
    filing_dates = recent.get('filingDate', [])
    
    for i, form in enumerate(forms):
        if form == filing_type and i < len(accessions) and i < len(primary_docs):
            accession = accessions[i].replace('-', '')
            primary_doc = primary_docs[i]
            filing_date = filing_dates[i] if i < len(filing_dates) else 'unknown'
            
            doc_url = (
                f'https://www.sec.gov/Archives/edgar/data/'
                f'{cik}/{accession}/{primary_doc}'
            )
            
            print(f"Found {filing_type} filed {filing_date}: {primary_doc}")
            return doc_url, filing_date
    
    print(f"No {filing_type} filing found for CIK {cik}")
    return None, None


def download_document(url):
    """Download a document from EDGAR with rate limit delay."""
    
    # FIX #2: Rate limit delay before EDGAR requests
    time.sleep(0.2)
    
    req = urllib.request.Request(url, headers=HEADERS)
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            content_type = response.headers.get('Content-Type', '')
            body = response.read()
            return body, content_type
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None, None


def extract_text_from_html(html_bytes):
    """Extract text from an HTML filing document."""
    
    try:
        text = html_bytes.decode('utf-8', errors='ignore')
    except Exception:
        text = html_bytes.decode('latin-1', errors='ignore')
    
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(?:br|p|div|tr|li|h[1-6])[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&#160;', ' ')
    
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = text.strip()
    
    return text


def extract_text_with_textract(pdf_bytes, ticker, timestamp):
    """
    Extract text from a PDF using Textract async API.
    
    FIX #1: Reduced polling to 30 iterations (150s max) to leave
    margin for result collection within Lambda's 300s timeout.
    
    FIX #4: After polling succeeds, fetches page 1 fresh instead
    of reusing the poll response, ensuring clean pagination.
    """
    
    temp_key = f'temp/textract/{ticker}/{timestamp}.pdf'
    
    s3.put_object(
        Bucket=BUCKET,
        Key=temp_key,
        Body=pdf_bytes,
        ContentType='application/pdf'
    )
    print(f"Uploaded PDF to s3://{BUCKET}/{temp_key}")
    
    try:
        response = textract.start_document_text_detection(
            DocumentLocation={
                'S3Object': {
                    'Bucket': BUCKET,
                    'Name': temp_key
                }
            }
        )
        
        job_id = response['JobId']
        print(f"Textract job started: {job_id}")
        
        # FIX #1: Reduced from 48 to 30 polls (150s max)
        # Leaves ~90s margin for download, upload, result collection, S3 write
        max_polls = 30
        for poll in range(max_polls):
            time.sleep(5)
            
            status_response = textract.get_document_text_detection(JobId=job_id)
            status = status_response['JobStatus']
            
            if status == 'SUCCEEDED':
                print(f"Textract job completed after {(poll + 1) * 5}s")
                break
            elif status == 'FAILED':
                msg = status_response.get('StatusMessage', 'Unknown error')
                print(f"Textract job failed: {msg}")
                return None
            
            if poll % 6 == 0:
                print(f"Textract still processing... ({(poll + 1) * 5}s elapsed)")
        else:
            print("Textract job timed out after 150s")
            return None
        
        # FIX #4: Fetch results fresh instead of reusing poll response
        # This ensures clean pagination from page 1
        all_text = []
        next_token = None
        
        while True:
            if next_token:
                result = textract.get_document_text_detection(
                    JobId=job_id,
                    NextToken=next_token
                )
            else:
                # Fresh fetch of page 1
                result = textract.get_document_text_detection(JobId=job_id)
            
            for block in result.get('Blocks', []):
                if block['BlockType'] == 'LINE':
                    all_text.append(block['Text'])
            
            next_token = result.get('NextToken')
            if not next_token:
                break
        
        text = '\n'.join(all_text)
        print(f"Extracted {len(all_text)} lines from PDF")
        return text
        
    finally:
        try:
            s3.delete_object(Bucket=BUCKET, Key=temp_key)
            print(f"Cleaned up temp PDF")
        except Exception as e:
            print(f"Warning: failed to clean up {temp_key}: {e}")


def identify_sections(text):
    """
    Identify and extract key sections from the filing text.
    
    FIX #3: Uses finditer() and takes the LAST match for each section
    pattern to skip Table of Contents entries and find the actual
    section body. Also validates that extracted section has minimum
    content length — TOC entries produce very short matches.
    """
    
    if not text:
        return {}
    
    section_positions = []
    for section_name, pattern in SECTION_PATTERNS.items():
        # Find ALL matches, not just the first
        matches = list(pattern.finditer(text))
        
        if not matches:
            continue
        
        if len(matches) == 1:
            # Only one match — use it
            section_positions.append((matches[0].start(), section_name))
        else:
            # Multiple matches (TOC + actual section) — use the last one
            # The actual section body appears later in the document
            section_positions.append((matches[-1].start(), section_name))
    
    if not section_positions:
        return {'full_text': text[:50000]}
    
    section_positions.sort(key=lambda x: x[0])
    
    sections = {}
    for i, (pos, name) in enumerate(section_positions):
        if i + 1 < len(section_positions):
            end_pos = section_positions[i + 1][0]
        else:
            end_pos = pos + 20000
        
        section_text = text[pos:end_pos].strip()
        
        # Skip sections that are suspiciously short (likely TOC remnants)
        if len(section_text) < 200:
            print(f"Skipping section '{name}' — only {len(section_text)} chars (likely TOC entry)")
            continue
        
        if len(section_text) > 15000:
            section_text = section_text[:15000] + '\n... [truncated]'
        
        sections[name] = section_text
    
    # If all sections were too short (all TOC), fall back to full text
    if not sections:
        return {'full_text': text[:50000]}
    
    return sections


def lambda_handler(event, context):
    """
    Main handler. Downloads SEC filing, extracts text, stores in silver.
    """
    
    ticker = event.get('ticker', 'TSLA')
    company_name = event.get('company_name', 'Tesla')
    cik = event.get('cik')
    sec_s3_key = event.get('sec_s3_key')
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    
    print(f"Processing SEC filings for {company_name} ({ticker})")
    
    # Get CIK from bronze SEC data if not provided directly
    if not cik and sec_s3_key:
        try:
            sec_data = json.loads(
                s3.get_object(Bucket=BUCKET, Key=sec_s3_key)['Body'].read()
            )
            cik = sec_data.get('cik')
        except Exception as e:
            print(f"Failed to read CIK from {sec_s3_key}: {e}")
    
    if not cik:
        raise ValueError(f"No CIK available for {ticker}. Cannot process SEC filings.")
    
    # FIX #2: Rate limit delay before first EDGAR call
    time.sleep(0.2)
    
    doc_url, filing_date = get_filing_url(cik, filing_type='10-K')
    
    if not doc_url:
        doc_url, filing_date = get_filing_url(cik, filing_type='10-Q')
    
    if not doc_url:
        raise RuntimeError(f"No 10-K or 10-Q filing found for {ticker} (CIK: {cik})")
    
    doc_bytes, content_type = download_document(doc_url)
    
    if not doc_bytes:
        raise RuntimeError(f"Failed to download filing from {doc_url}")
    
    print(f"Downloaded {len(doc_bytes)} bytes, content-type: {content_type}")
    
    is_pdf = (
        'pdf' in (content_type or '').lower()
        or doc_url.lower().endswith('.pdf')
    )
    
    if is_pdf:
        print("Document is PDF — using Textract")
        full_text = extract_text_with_textract(doc_bytes, ticker, timestamp)
        extraction_method = 'textract'
    else:
        print("Document is HTML — using direct text extraction")
        full_text = extract_text_from_html(doc_bytes)
        extraction_method = 'html_parse'
    
    if not full_text:
        raise RuntimeError(f"Failed to extract text from {doc_url}")
    
    print(f"Extracted {len(full_text)} characters using {extraction_method}")
    
    sections = identify_sections(full_text)
    
    print(f"Identified {len(sections)} sections: {list(sections.keys())}")
    
    filing_package = {
        'ticker': ticker,
        'company_name': company_name,
        'cik': cik,
        'processed_at': timestamp,
        'filing_date': filing_date,
        'filing_url': doc_url,
        'extraction_method': extraction_method,
        'total_chars': len(full_text),
        'sections_found': list(sections.keys()),
        'sections': sections,
        'full_text_preview': full_text[:5000],
    }
    
    filing_key = f'silver/filings/{ticker}/{timestamp}.json'
    
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=filing_key,
            Body=json.dumps(filing_package, default=str),
            ContentType='application/json'
        )
        print(f"Stored extracted filing at {filing_key}")
    except Exception as e:
        print(f"Failed to write to S3: {e}")
        raise
    
    return {
        'statusCode': 200,
        'ticker': ticker,
        'company_name': company_name,
        'filing_s3_key': filing_key,
        'filing_date': filing_date,
        'extraction_method': extraction_method,
        'total_chars': len(full_text),
        'sections_found': list(sections.keys()),
        'processed_at': timestamp
    }