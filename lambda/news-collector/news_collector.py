import json
import time
import boto3
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta

s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')

BUCKET = 'company-intel-datalake-satvik'


def get_api_key():
    """Retrieve NewsAPI key from Secrets Manager."""
    try:
        response = secrets.get_secret_value(SecretId='company-intel/api-keys')
        secret = json.loads(response['SecretString'])

        if 'newsapi_key' not in secret:
            raise KeyError("'newsapi_key' not found in secret. Check Secrets Manager.")

        return secret['newsapi_key']
    except Exception as e:
        print(f"Failed to retrieve API key from Secrets Manager: {e}")
        raise


def sanitize_input(text):
    """
    Remove characters that could break NewsAPI query syntax.
    Only strips double quotes, which would corrupt the quoted phrase syntax.
    """
    return text.replace('"', '').strip()


def fetch_news(company_name, ticker, api_key, max_pages=2):
    """
    Fetch recent news articles from NewsAPI with pagination.

    API key is passed as a header (X-Api-Key) instead of a URL
    parameter to prevent it from leaking in error logs/tracebacks.

    Paginates up to max_pages (50 articles per page) to capture
    broader coverage for heavily-discussed companies.
    """

    from_date = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')

    safe_company = sanitize_input(company_name)
    safe_ticker = sanitize_input(ticker)
    query = urllib.parse.quote(f'"{safe_company}" OR "{safe_ticker}"')

    all_articles = []
    total_results = 0
    errors = []

    for page in range(1, max_pages + 1):
        if page > 1:
            time.sleep(0.5)

        url = (
            f'https://newsapi.org/v2/everything'
            f'?q={query}'
            f'&from={from_date}'
            f'&sortBy=relevancy'
            f'&pageSize=50'
            f'&page={page}'
            f'&language=en'
        )

        # API key in header, NOT in URL — prevents leaking in tracebacks
        req = urllib.request.Request(url)
        req.add_header('X-Api-Key', api_key)

        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                data = json.loads(response.read().decode('utf-8'))

            if data.get('status') != 'ok':
                error_msg = data.get('message', 'Unknown NewsAPI error')
                errors.append(f"Page {page}: {error_msg}")
                print(f"NewsAPI error on page {page}: {error_msg}")
                break

            total_results = data.get('totalResults', 0)
            articles = data.get('articles', [])

            if not articles:
                break

            all_articles.extend(articles)
            print(f"Page {page}: fetched {len(articles)} articles")

            # Stop if we've got everything
            if len(all_articles) >= total_results:
                break

        except urllib.error.HTTPError as e:
            errors.append(f"Page {page}: HTTP {e.code} - {e.reason}")
            print(f"HTTP error fetching news page {page}: {e.code} {e.reason}")
            break
        except Exception as e:
            errors.append(f"Page {page}: {str(e)}")
            print(f"Error fetching news page {page}: {e}")
            break

    return {
        'articles': all_articles,
        'total_results': total_results,
        'total_fetched': len(all_articles),
        'errors': errors
    }


def lambda_handler(event, context):
    """
    Main handler. Expects event like:
    {"ticker": "TSLA", "company_name": "Tesla"}
    """

    ticker = event.get('ticker', 'TSLA')
    company_name = event.get('company_name', 'Tesla')
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

    print(f"Collecting news for {company_name} ({ticker})")

    # 1. Get API key from Secrets Manager
    api_key = get_api_key()

    # 2. Fetch news articles with pagination
    result = fetch_news(company_name, ticker, api_key)
    articles = result['articles']

    # 3. Determine if the fetch was successful enough to proceed
    fetch_failed = len(articles) == 0 and len(result['errors']) > 0

    if fetch_failed:
        raise RuntimeError(f"News fetch failed completely: {result['errors']}")

    print(f"Fetched {result['total_fetched']} of {result['total_results']} available articles")

    # 4. Build data package
    data_package = {
        'ticker': ticker,
        'company_name': company_name,
        'collected_at': timestamp,
        'source': 'newsapi',
        'total_results': result['total_results'],
        'total_fetched': result['total_fetched'],
        'fetch_errors': result['errors'],
        'articles': [
            {
                'title': a.get('title'),
                'description': a.get('description'),
                'source_name': a.get('source', {}).get('name'),
                'author': a.get('author'),
                'url': a.get('url'),
                'published_at': a.get('publishedAt'),
                'content': a.get('content')
            }
            for a in articles
        ]
    }

    # 5. Store in S3 bronze zone
    s3_key = f'bronze/news/{ticker}/{timestamp}.json'

    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=json.dumps(data_package, default=str),
            ContentType='application/json'
        )
        print(f"Stored {len(articles)} articles at s3://{BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Failed to write to S3: {e}")
        raise

    # 6. Return metadata for Step Functions
    return {
        'statusCode': 200,
        'ticker': ticker,
        'company_name': company_name,
        'articles_count': len(articles),
        'total_available': result['total_results'],
        'fetch_errors': result['errors'],
        's3_key': s3_key,
        'collected_at': timestamp
    }
