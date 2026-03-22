import json
import re
import boto3
from datetime import datetime, timezone

s3 = boto3.client('s3')

BUCKET = 'company-intel-datalake-satvik'

# Patterns that indicate cookie consent / tracking boilerplate
BOILERPLATE_PATTERNS = [
    r'IAB Transparency.*?Framework',
    r'our partners.*?collect',
    r'consent.*?legitimate interest',
    r'cookies.*?personali[sz]e',
    r'we and our.*?partners',
    r'data.*?for personalised',
    r'manage your privacy',
    r'Your Privacy Choices',
]
BOILERPLATE_RE = re.compile('|'.join(BOILERPLATE_PATTERNS), re.IGNORECASE)

# Phrases that are almost always boilerplate — exact match
BOILERPLATE_EXACT = {'accept all', 'reject all', 'privacy settings'}

# HTML entity cleanup
HTML_ENTITIES = {
    '&amp;': '&',
    '&lt;': '<',
    '&gt;': '>',
    '&quot;': '"',
    '&#x27;': "'",
    '&#x2F;': '/',
    '&#39;': "'",
    '&nbsp;': ' ',
}
HTML_ENTITY_RE = re.compile('|'.join(re.escape(k) for k in HTML_ENTITIES.keys()))

# NewsAPI truncation marker
TRUNCATION_RE = re.compile(r'\[\+\d+ chars\]')

# Inline boilerplate stripping — removes boilerplate embedded within article content
# These patterns match the full boilerplate block so it gets stripped
# while preserving the actual article text around it
BOILERPLATE_STRIP_RE = re.compile(
    r'(?:'
    r'We and our \d+ partners.*?(?:privacy settings|reject all|accept all)'
    r'|IAB Transparency.*?Framework.*?(?:\.|$)'
    r'|We use cookies.*?(?:privacy settings|learn more|accept|reject).*?\.'
    r'|Your Privacy Choices.*?(?:\.|$)'
    r'|By clicking.*?(?:cookie policy|privacy policy).*?\.'
    r')',
    re.IGNORECASE | re.DOTALL
)


def read_s3_json(key):
    """Read and parse a JSON file from S3."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))


def clean_html_entities(text):
    """Replace HTML entities with their actual characters."""
    if not text:
        return ''
    return HTML_ENTITY_RE.sub(lambda m: HTML_ENTITIES[m.group()], text)


def strip_html_tags(text):
    """Remove any remaining HTML tags."""
    if not text:
        return ''
    return re.sub(r'<[^>]+>', '', text)


def is_boilerplate(text):
    """
    Check if text is mostly cookie consent / tracking boilerplate.
    
    Uses two strategies:
    1. Exact match for short phrases that are always boilerplate
    2. Pattern match + ratio check for longer text — the boilerplate
       pattern must cover a significant portion of the text to trigger,
       preventing false positives on articles that merely mention
       "accept all" in a financial context.
    """
    if not text:
        return False
    
    text_lower = text.lower().strip()
    
    if text_lower in BOILERPLATE_EXACT:
        return True
    
    matches = list(BOILERPLATE_RE.finditer(text))
    if not matches:
        return False
    
    matched_chars = sum(m.end() - m.start() for m in matches)
    ratio = matched_chars / len(text)
    
    return ratio > 0.3


def clean_text(text):
    """Full text cleaning pipeline."""
    if not text:
        return ''
    
    text = clean_html_entities(text)
    text = strip_html_tags(text)
    text = TRUNCATION_RE.sub('', text)
    text = BOILERPLATE_STRIP_RE.sub('', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def is_relevant(text, search_terms):
    """
    Check if text is relevant to the company.
    
    Uses word boundary matching to prevent false positives:
    - "tesla" matches "Tesla" but not "nikola tesla" (unless context fits)
    - "meta" matches "Meta" but not "metadata" or "metaphor"
    - Short tickers like "AI" match as whole words only
    """
    if not text or not search_terms:
        return False
    
    text_lower = text.lower()
    for term in search_terms:
        if len(term) < 4:
            if re.search(r'\b' + re.escape(term) + r'\b', text_lower):
                return True
        else:
            if term in text_lower:
                return True
    return False


def clean_articles(raw_data):
    """
    Clean news articles from bronze zone.
    
    Removes HTML entities, cookie boilerplate, truncation markers,
    duplicates, and articles with no meaningful content.
    """
    
    articles = raw_data.get('articles', [])
    cleaned = []
    seen_titles = set()
    boilerplate_removed = 0
    duplicates_removed = 0
    
    for article in articles:
        title = clean_text(article.get('title', ''))
        description = clean_text(article.get('description', ''))
        content = clean_text(article.get('content', ''))
        
        if not title:
            continue
        
        title_lower = title.lower().strip()
        if title_lower in seen_titles:
            duplicates_removed += 1
            continue
        seen_titles.add(title_lower)
        
        if description and is_boilerplate(description):
            description = ''
            boilerplate_removed += 1
        
        if content and is_boilerplate(content):
            content = ''
            boilerplate_removed += 1
        
        cleaned.append({
            'title': title,
            'description': description,
            'content': content,
            'source_name': article.get('source_name', ''),
            'author': article.get('author', ''),
            'url': article.get('url', ''),
            'published_at': article.get('published_at', ''),
        })
    
    print(f"Articles: {len(articles)} raw -> {len(cleaned)} cleaned "
          f"({duplicates_removed} duplicates, {boilerplate_removed} boilerplate strips)")
    
    return cleaned


def clean_social_posts(raw_data):
    """
    Clean HN stories and comments from bronze zone.
    
    Filters both stories AND comments for relevance.
    """
    
    ticker = raw_data.get('ticker', '')
    company_name = raw_data.get('company_name', '')
    search_terms = [t.lower() for t in [ticker, company_name] if t]
    
    stories = raw_data.get('stories', [])
    comments = raw_data.get('comments', [])
    
    cleaned_stories = []
    filtered_stories = 0
    
    for story in stories:
        title = clean_text(story.get('title', ''))
        story_text = clean_text(story.get('story_text', ''))
        
        combined = f"{title} {story_text}"
        if not is_relevant(combined, search_terms):
            filtered_stories += 1
            continue
        
        cleaned_stories.append({
            'object_id': story.get('object_id'),
            'type': 'story',
            'title': title,
            'story_text': story_text,
            'author': story.get('author', ''),
            'points': story.get('points', 0),
            'num_comments': story.get('num_comments', 0),
            'created_at': story.get('created_at', ''),
            'created_at_i': story.get('created_at_i', 0),
            'url': story.get('url', ''),
            'hn_url': story.get('hn_url', ''),
        })
    
    cleaned_comments = []
    empty_comments = 0
    filtered_comments = 0
    
    for comment in comments:
        comment_text = clean_text(comment.get('comment_text', ''))
        story_title = clean_text(comment.get('story_title', ''))
        
        if not comment_text or len(comment_text) < 20:
            empty_comments += 1
            continue
        
        combined = f"{story_title} {comment_text}"
        if not is_relevant(combined, search_terms):
            filtered_comments += 1
            continue
        
        cleaned_comments.append({
            'object_id': comment.get('object_id'),
            'type': 'comment',
            'comment_text': comment_text,
            'author': comment.get('author', ''),
            'points': comment.get('points', 0),
            'created_at': comment.get('created_at', ''),
            'created_at_i': comment.get('created_at_i', 0),
            'story_id': comment.get('story_id'),
            'story_title': story_title,
            'hn_url': comment.get('hn_url', ''),
        })
    
    print(f"Stories: {len(stories)} raw -> {len(cleaned_stories)} cleaned "
          f"({filtered_stories} irrelevant filtered)")
    print(f"Comments: {len(comments)} raw -> {len(cleaned_comments)} cleaned "
          f"({empty_comments} empty, {filtered_comments} irrelevant filtered)")
    
    return cleaned_stories, cleaned_comments


def lambda_handler(event, context):
    """
    Main handler. Reads raw data from bronze, cleans it, writes to silver.
    """
    
    ticker = event.get('ticker', 'TSLA')
    company_name = event.get('company_name', 'Tesla')
    news_s3_key = event.get('news_s3_key')
    social_s3_key = event.get('social_s3_key')
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    
    print(f"Cleaning data for {company_name} ({ticker})")
    
    # 1. Clean news articles
    articles_key = None
    cleaned_articles = []
    if news_s3_key:
        news_data = read_s3_json(news_s3_key)
        cleaned_articles = clean_articles(news_data)
        
        articles_package = {
            'ticker': ticker,
            'company_name': company_name,
            'cleaned_at': timestamp,
            'source': 'newsapi',
            'total_cleaned': len(cleaned_articles),
            'articles': cleaned_articles,
        }
        
        articles_key = f'silver/articles/{ticker}/{timestamp}.json'
        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=articles_key,
                Body=json.dumps(articles_package, default=str),
                ContentType='application/json'
            )
            print(f"Stored cleaned articles at {articles_key}")
        except Exception as e:
            print(f"Failed to write cleaned articles to S3: {e}")
            raise
    
    # 2. Clean social posts
    social_key = None
    cleaned_stories = []
    cleaned_comments = []
    if social_s3_key:
        social_data = read_s3_json(social_s3_key)
        cleaned_stories, cleaned_comments = clean_social_posts(social_data)
        
        social_package = {
            'ticker': ticker,
            'company_name': company_name,
            'cleaned_at': timestamp,
            'source': 'hackernews',
            'total_stories': len(cleaned_stories),
            'total_comments': len(cleaned_comments),
            'stories': cleaned_stories,
            'comments': cleaned_comments,
        }
        
        social_key = f'silver/social/{ticker}/{timestamp}.json'
        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=social_key,
                Body=json.dumps(social_package, default=str),
                ContentType='application/json'
            )
            print(f"Stored cleaned social at {social_key}")
        except Exception as e:
            print(f"Failed to write cleaned social to S3: {e}")
            raise
    
    # 3. Check if we have any cleaned data
    total = len(cleaned_articles) + len(cleaned_stories) + len(cleaned_comments)
    if total == 0:
        raise RuntimeError(f"No data remained after cleaning for {ticker}")
    
    # 4. Return metadata
    return {
        'statusCode': 200,
        'ticker': ticker,
        'company_name': company_name,
        'articles_s3_key': articles_key,
        'social_s3_key': social_key,
        'articles_cleaned': len(cleaned_articles),
        'stories_cleaned': len(cleaned_stories),
        'comments_cleaned': len(cleaned_comments),
        'cleaned_at': timestamp
    }