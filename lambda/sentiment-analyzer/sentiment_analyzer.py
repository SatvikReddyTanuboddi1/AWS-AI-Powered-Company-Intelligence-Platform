import json
import boto3
from datetime import datetime, timezone

s3 = boto3.client('s3')
comprehend = boto3.client('comprehend')

BUCKET = 'company-intel-datalake-satvik'

MAX_TEXT_BYTES = 4900
BATCH_SIZE = 25


def read_s3_json(key):
    """Read and parse a JSON file from S3."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"Failed to read {key}: {e}")
        return None


def prepare_text(title, body):
    """Combine title and body for analysis. Max 5000 bytes for Comprehend."""
    text = f"{title or ''}. {body or ''}"
    text = text.strip()
    
    if not text or text == '.':
        return None
    
    encoded = text.encode('utf-8')
    if len(encoded) > MAX_TEXT_BYTES:
        text = encoded[:MAX_TEXT_BYTES].decode('utf-8', errors='ignore')
    
    return text


def batch_analyze(texts, analysis_type='sentiment'):
    """
    Run Comprehend batch analysis on a list of texts.
    Uses Index field from ResultList to handle partial failures.
    """
    if not texts:
        return []
    
    all_results = []
    
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i + BATCH_SIZE]
        
        try:
            if analysis_type == 'sentiment':
                response = comprehend.batch_detect_sentiment(
                    TextList=batch, LanguageCode='en'
                )
            elif analysis_type == 'entities':
                response = comprehend.batch_detect_entities(
                    TextList=batch, LanguageCode='en'
                )
            elif analysis_type == 'key_phrases':
                response = comprehend.batch_detect_key_phrases(
                    TextList=batch, LanguageCode='en'
                )
            else:
                all_results.extend([{} for _ in batch])
                continue
            
            result_map = {r['Index']: r for r in response.get('ResultList', [])}
            for j in range(len(batch)):
                all_results.append(result_map.get(j, {}))
            
            errors = response.get('ErrorList', [])
            if errors:
                print(f"{analysis_type} batch {i // BATCH_SIZE}: {len(errors)} errors: "
                      f"{[e.get('ErrorMessage', '') for e in errors[:3]]}")
                
        except Exception as e:
            print(f"Batch analysis error ({analysis_type}): {e}")
            all_results.extend([{} for _ in batch])
    
    return all_results


def analyze_articles(articles):
    """
    Run full NLP analysis on news articles.
    Returns ALL original articles — unanalyzable ones get UNANALYZED.
    """
    
    texts = []
    valid_indices = []
    valid_indices_set = set()

    for i, article in enumerate(articles):
        text = prepare_text(
            article.get('title'),
            article.get('description') or article.get('content', '')
        )
        if text:
            texts.append(text)
            valid_indices.append(i)
            valid_indices_set.add(i)

    sentiments = batch_analyze(texts, 'sentiment') if texts else []
    entities = batch_analyze(texts, 'entities') if texts else []
    key_phrases = batch_analyze(texts, 'key_phrases') if texts else []

    print(f"Analyzed {len(texts)} of {len(articles)} articles through Comprehend")

    enriched = []
    valid_idx_pos = 0

    for i, article in enumerate(articles):
        enriched_article = article.copy()

        if i in valid_indices_set and valid_idx_pos < len(valid_indices):
            idx = valid_idx_pos
            valid_idx_pos += 1
            
            if idx < len(sentiments) and sentiments[idx]:
                s = sentiments[idx]
                enriched_article['sentiment'] = s.get('Sentiment', 'UNKNOWN')
                enriched_article['sentiment_scores'] = {
                    'positive': s.get('SentimentScore', {}).get('Positive', 0),
                    'negative': s.get('SentimentScore', {}).get('Negative', 0),
                    'neutral': s.get('SentimentScore', {}).get('Neutral', 0),
                    'mixed': s.get('SentimentScore', {}).get('Mixed', 0),
                }
            else:
                enriched_article['sentiment'] = 'UNANALYZED'
                enriched_article['sentiment_scores'] = {
                    'positive': 0, 'negative': 0, 'neutral': 0, 'mixed': 0
                }
            
            if idx < len(entities) and entities[idx]:
                e = entities[idx]
                enriched_article['entities'] = [
                    {'text': ent.get('Text'), 'type': ent.get('Type'),
                     'score': round(ent.get('Score', 0), 3)}
                    for ent in e.get('Entities', [])
                    if ent.get('Score', 0) > 0.8
                ]
            else:
                enriched_article['entities'] = []
            
            if idx < len(key_phrases) and key_phrases[idx]:
                kp = key_phrases[idx]
                phrases = sorted(
                    kp.get('KeyPhrases', []),
                    key=lambda x: x.get('Score', 0),
                    reverse=True
                )[:10]
                enriched_article['key_phrases'] = [p.get('Text') for p in phrases]
            else:
                enriched_article['key_phrases'] = []
        else:
            enriched_article['sentiment'] = 'UNANALYZED'
            enriched_article['sentiment_scores'] = {
                'positive': 0, 'negative': 0, 'neutral': 0, 'mixed': 0
            }
            enriched_article['entities'] = []
            enriched_article['key_phrases'] = []
        
        enriched.append(enriched_article)
    
    return enriched


def analyze_social_posts(posts):
    """Run NLP analysis on HN stories and comments."""
    
    texts = []
    valid_indices = []
    valid_indices_set = set()

    for i, post in enumerate(posts):
        if post.get('type') == 'story':
            text = prepare_text(post.get('title'), post.get('story_text', ''))
        else:
            text = prepare_text(post.get('story_title', ''), post.get('comment_text', ''))

        if text:
            texts.append(text)
            valid_indices.append(i)
            valid_indices_set.add(i)

    sentiments = batch_analyze(texts, 'sentiment') if texts else []
    entities = batch_analyze(texts, 'entities') if texts else []
    key_phrases = batch_analyze(texts, 'key_phrases') if texts else []

    print(f"Analyzed {len(texts)} of {len(posts)} social posts through Comprehend")

    enriched = []
    valid_idx_pos = 0

    for i, post in enumerate(posts):
        enriched_post = post.copy()

        if i in valid_indices_set and valid_idx_pos < len(valid_indices):
            idx = valid_idx_pos
            valid_idx_pos += 1
            
            if idx < len(sentiments) and sentiments[idx]:
                s = sentiments[idx]
                enriched_post['sentiment'] = s.get('Sentiment', 'UNKNOWN')
                enriched_post['sentiment_scores'] = {
                    'positive': s.get('SentimentScore', {}).get('Positive', 0),
                    'negative': s.get('SentimentScore', {}).get('Negative', 0),
                    'neutral': s.get('SentimentScore', {}).get('Neutral', 0),
                    'mixed': s.get('SentimentScore', {}).get('Mixed', 0),
                }
            else:
                enriched_post['sentiment'] = 'UNANALYZED'
                enriched_post['sentiment_scores'] = {
                    'positive': 0, 'negative': 0, 'neutral': 0, 'mixed': 0
                }
            
            if idx < len(entities) and entities[idx]:
                e = entities[idx]
                enriched_post['entities'] = [
                    {'text': ent.get('Text'), 'type': ent.get('Type'),
                     'score': round(ent.get('Score', 0), 3)}
                    for ent in e.get('Entities', [])
                    if ent.get('Score', 0) > 0.8
                ]
            else:
                enriched_post['entities'] = []
            
            if idx < len(key_phrases) and key_phrases[idx]:
                kp = key_phrases[idx]
                phrases = sorted(
                    kp.get('KeyPhrases', []),
                    key=lambda x: x.get('Score', 0),
                    reverse=True
                )[:10]
                enriched_post['key_phrases'] = [p.get('Text') for p in phrases]
            else:
                enriched_post['key_phrases'] = []
        else:
            enriched_post['sentiment'] = 'UNANALYZED'
            enriched_post['sentiment_scores'] = {
                'positive': 0, 'negative': 0, 'neutral': 0, 'mixed': 0
            }
            enriched_post['entities'] = []
            enriched_post['key_phrases'] = []
        
        enriched.append(enriched_post)
    
    return enriched


def compute_aggregates(enriched_articles, enriched_social):
    """Compute aggregate sentiment stats. Excludes UNANALYZED and UNKNOWN."""
    
    all_items = [
        item for item in (enriched_articles + enriched_social)
        if item.get('sentiment') not in ('UNANALYZED', 'UNKNOWN', None)
    ]
    
    sentiment_counts = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0, 'MIXED': 0}
    sentiment_scores_sum = {'positive': 0, 'negative': 0, 'neutral': 0, 'mixed': 0}
    
    for item in all_items:
        sentiment = item.get('sentiment', 'UNKNOWN')
        if sentiment in sentiment_counts:
            sentiment_counts[sentiment] += 1
        scores = item.get('sentiment_scores', {})
        for key in sentiment_scores_sum:
            sentiment_scores_sum[key] += scores.get(key, 0)
    
    total = len(all_items) or 1
    
    entity_freq = {}
    for item in (enriched_articles + enriched_social):
        for ent in item.get('entities', []):
            key = (ent['text'], ent['type'])
            if key not in entity_freq:
                entity_freq[key] = {'text': ent['text'], 'type': ent['type'], 'count': 0}
            entity_freq[key]['count'] += 1
    
    top_entities = sorted(entity_freq.values(), key=lambda x: x['count'], reverse=True)[:30]
    
    phrase_freq = {}
    for item in (enriched_articles + enriched_social):
        for phrase in item.get('key_phrases', []):
            phrase_freq[phrase] = phrase_freq.get(phrase, 0) + 1
    top_phrases = sorted(phrase_freq.items(), key=lambda x: x[1], reverse=True)[:20]
    
    return {
        'total_analyzed': len(all_items),
        'total_unanalyzed': len(enriched_articles) + len(enriched_social) - len(all_items),
        'articles_analyzed': len([a for a in enriched_articles if a.get('sentiment') not in ('UNANALYZED', 'UNKNOWN', None)]),
        'social_posts_analyzed': len([s for s in enriched_social if s.get('sentiment') not in ('UNANALYZED', 'UNKNOWN', None)]),
        'sentiment_distribution': sentiment_counts,
        'average_sentiment_scores': {
            k: round(v / total, 4) for k, v in sentiment_scores_sum.items()
        },
        'top_entities': top_entities,
        'top_key_phrases': [{'phrase': p, 'count': c} for p, c in top_phrases],
    }


def lambda_handler(event, context):
    """
    CHANGED: Now reads from silver (cleaned) instead of bronze (raw).
    Accepts both 'articles_s3_key' (from data cleaner) and 'news_s3_key'
    (backward compatible for direct testing).
    """
    
    ticker = event.get('ticker', 'TSLA')
    company_name = event.get('company_name', 'Tesla')
    # CHANGED: Accept both parameter names
    articles_s3_key = event.get('articles_s3_key') or event.get('news_s3_key')
    social_s3_key = event.get('social_s3_key')
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    
    print(f"Running sentiment analysis for {company_name} ({ticker})")
    print(f"Reading from: articles={articles_s3_key}, social={social_s3_key}")
    
    # 1. Read and analyze articles (from silver or bronze)
    enriched_articles = []
    if articles_s3_key:
        articles_data = read_s3_json(articles_s3_key)
        if articles_data:
            articles = articles_data.get('articles', [])
            enriched_articles = analyze_articles(articles)
    
    # 2. Read and analyze social posts (from silver or bronze)
    enriched_social = []
    if social_s3_key:
        social_data = read_s3_json(social_s3_key)
        if social_data:
            posts = social_data.get('stories', []) + social_data.get('comments', [])
            enriched_social = analyze_social_posts(posts)
    
    # 3. Check if we have any data
    total_items = len(enriched_articles) + len(enriched_social)
    if total_items == 0:
        raise RuntimeError(
            f"No data to analyze for {ticker}. "
            f"articles_key={articles_s3_key}, social_key={social_s3_key}"
        )
    
    # 4. Compute aggregates
    aggregates = compute_aggregates(enriched_articles, enriched_social)
    
    # 5. Build gold zone package
    gold_package = {
        'ticker': ticker,
        'company_name': company_name,
        'analyzed_at': timestamp,
        'aggregates': aggregates,
        'articles': enriched_articles,
        'social_posts': enriched_social,
    }
    
    # 6. Store in gold zone
    gold_key = f'gold/sentiment/{ticker}/{timestamp}.json'
    
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=gold_key,
            Body=json.dumps(gold_package, default=str),
            ContentType='application/json'
        )
        print(f"Stored enriched data at s3://{BUCKET}/{gold_key}")
    except Exception as e:
        print(f"Failed to write to S3: {e}")
        raise
    
    # 7. Return metadata
    return {
        'statusCode': 200,
        'ticker': ticker,
        'company_name': company_name,
        'articles_analyzed': aggregates['articles_analyzed'],
        'social_posts_analyzed': aggregates['social_posts_analyzed'],
        'sentiment_distribution': aggregates['sentiment_distribution'],
        'top_entities_count': len(aggregates['top_entities']),
        'gold_s3_key': gold_key,
        'analyzed_at': timestamp
    }