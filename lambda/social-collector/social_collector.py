import json
import time
import boto3
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone

s3 = boto3.client('s3')

BUCKET = 'company-intel-datalake-satvik'
HN_BASE_URL = 'https://hn.algolia.com/api/v1'


def search_hn(query, tags='story', page=0, hits_per_page=50, week_ago=None):
    """
    Search Hacker News via the Algolia-powered API.

    No auth required. Indexes all HN stories, comments, and polls.

    Tags filter:
    - 'story' = top-level posts only
    - 'comment' = comments only
    """

    encoded_query = urllib.parse.quote(query)
    numeric_filter = urllib.parse.quote(f'created_at_i>{week_ago}')
    url = (
        f'{HN_BASE_URL}/search'
        f'?query={encoded_query}'
        f'&tags={tags}'
        f'&numericFilters={numeric_filter}'
        f'&hitsPerPage={hits_per_page}'
        f'&page={page}'
    )

    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'CompanyIntelPlatform/1.0')

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        print(f"HTTP error searching HN: {e.code} {e.reason}")
        return None
    except Exception as e:
        print(f"Error searching HN: {e}")
        return None


def extract_story(hit):
    """Extract relevant fields from an HN story."""

    return {
        'object_id': hit.get('objectID'),
        'type': 'story',
        'title': hit.get('title'),
        'url': hit.get('url'),
        'author': hit.get('author'),
        'points': hit.get('points', 0),
        'num_comments': hit.get('num_comments', 0),
        'created_at': hit.get('created_at'),
        'created_at_i': hit.get('created_at_i'),
        'story_text': hit.get('story_text', ''),
        'hn_url': f"https://news.ycombinator.com/item?id={hit.get('objectID', '')}",
    }


def extract_comment(hit):
    """Extract relevant fields from an HN comment."""

    return {
        'object_id': hit.get('objectID'),
        'type': 'comment',
        'comment_text': hit.get('comment_text', '')[:2000],
        'author': hit.get('author'),
        'points': hit.get('points', 0),
        'created_at': hit.get('created_at'),
        'created_at_i': hit.get('created_at_i'),
        'story_id': hit.get('story_id'),
        'story_title': hit.get('story_title'),
        'story_url': hit.get('story_url'),
        'parent_id': hit.get('parent_id'),
        'hn_url': f"https://news.ycombinator.com/item?id={hit.get('objectID', '')}",
    }


def deduplicate(items, key_field='object_id', sort_field='points'):
    """
    Deduplicate items by key_field, keeping the version with
    the highest sort_field value. Skips items with missing key_field.
    """

    seen = {}
    for item in items:
        oid = item.get(key_field)
        if oid is None:
            print(f"Warning: item missing '{key_field}', skipping: {item}")
            continue
        if oid not in seen or item.get(sort_field, 0) > seen[oid].get(sort_field, 0):
            seen[oid] = item

    return sorted(seen.values(), key=lambda x: x.get(sort_field, 0), reverse=True)


def lambda_handler(event, context):
    """
    Main handler. Expects event like:
    {"ticker": "TSLA", "company_name": "Tesla"}
    """

    ticker = event.get('ticker', 'TSLA')
    company_name = event.get('company_name', 'Tesla')
    now = datetime.now(timezone.utc)
    timestamp = now.strftime('%Y%m%d_%H%M%S_%f')
    week_ago = int((now - timedelta(days=7)).timestamp())

    print(f"Collecting HN data for {company_name} ({ticker})")

    all_stories = []
    all_comments = []
    errors = []

    queries = [company_name, ticker]

    for i, query in enumerate(queries):
        # 1. Fetch stories with per-query pagination tracking
        query_story_count = 0

        for page in range(2):
            stories_data = search_hn(query, tags='story', page=page, week_ago=week_ago)

            if not stories_data:
                errors.append(f"Failed to fetch stories for '{query}' page {page}")
                break

            hits = stories_data.get('hits', [])
            if not hits:
                break

            all_stories.extend([extract_story(h) for h in hits])
            query_story_count += len(hits)
            print(f"Stories for '{query}' page {page}: {len(hits)} hits")

            # Per-query check — not cumulative across queries
            if query_story_count >= stories_data.get('nbHits', float('inf')):
                break

            time.sleep(0.3)

        # 2. Fetch comments — always delay before this call
        time.sleep(0.3)
        comments_data = search_hn(query, tags='comment', page=0, hits_per_page=50, week_ago=week_ago)

        if comments_data:
            hits = comments_data.get('hits', [])
            all_comments.extend([extract_comment(h) for h in hits])
            print(f"Comments for '{query}': {len(hits)} hits")
        else:
            errors.append(f"Failed to fetch comments for '{query}'")

        # Polite delay between queries, not after the last one
        if i < len(queries) - 1:
            time.sleep(0.3)

    # 3. Deduplicate — stories by points, comments by recency
    unique_stories = deduplicate(all_stories, sort_field='points')
    unique_comments = deduplicate(all_comments, sort_field='created_at_i')

    duplicates_removed = (len(all_stories) - len(unique_stories)) + (len(all_comments) - len(unique_comments))
    if duplicates_removed > 0:
        print(f"Deduplicated: removed {duplicates_removed} duplicate entries")

    # 4. Determine success/failure
    total_items = len(unique_stories) + len(unique_comments)
    fetch_failed = total_items == 0 and len(errors) > 0

    if fetch_failed:
        raise RuntimeError(f"HN fetch failed completely: {errors}")

    print(f"Total: {len(unique_stories)} stories, {len(unique_comments)} comments")

    # 5. Build data package
    data_package = {
        'ticker': ticker,
        'company_name': company_name,
        'collected_at': timestamp,
        'source': 'hackernews',
        'queries_used': queries,
        'total_stories': len(unique_stories),
        'total_comments': len(unique_comments),
        'fetch_errors': errors,
        'stories': unique_stories,
        'comments': unique_comments,
    }

    # 6. Store in S3 bronze zone
    s3_key = f'bronze/social/{ticker}/{timestamp}.json'

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

    # 7. Return metadata for Step Functions
    return {
        'statusCode': 200,
        'ticker': ticker,
        'company_name': company_name,
        'stories_count': len(unique_stories),
        'comments_count': len(unique_comments),
        'fetch_errors': errors,
        's3_key': s3_key,
        'collected_at': timestamp
    }
