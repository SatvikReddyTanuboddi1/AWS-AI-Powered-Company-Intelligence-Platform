import json
import os
import re
import boto3
from datetime import datetime, timezone
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('DYNAMO_TABLE', 'company-intel-reports')

s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

BUCKET = os.environ.get('S3_BUCKET', 'company-intel-datalake-satvik')
MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'us.anthropic.claude-3-5-haiku-20241022-v1:0')

VALID_TICKER = re.compile(r'^[A-Z0-9\-.]{1,10}$')


def read_s3_json(key):
    """Read and parse a JSON file from S3."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))


def truncate(text, limit=300):
    if not text:
        return ''
    if len(text) <= limit:
        return text
    return text[:limit] + '... [truncated]'


def build_prompt(ticker, company_name, sentiment_data, sec_data, filing_data):
    """
    Build the report prompt with all data sources.
    CHANGED: Now accepts filing_data from silver/filings/ (Textract output).
    """

    agg = sentiment_data.get('aggregates', {})
    sentiment_dist = agg.get('sentiment_distribution', {})
    avg_scores = agg.get('average_sentiment_scores', {})
    top_entities = agg.get('top_entities', [])[:15]
    top_phrases = agg.get('top_key_phrases', [])[:10]

    articles = sentiment_data.get('articles', [])
    positive_articles = sorted(
        [a for a in articles if a.get('sentiment') == 'POSITIVE'],
        key=lambda x: x.get('sentiment_scores', {}).get('positive', 0),
        reverse=True
    )[:5]
    negative_articles = sorted(
        [a for a in articles if a.get('sentiment') == 'NEGATIVE'],
        key=lambda x: x.get('sentiment_scores', {}).get('negative', 0),
        reverse=True
    )[:5]
    mixed_articles = sorted(
        [a for a in articles if a.get('sentiment') == 'MIXED'],
        key=lambda x: x.get('sentiment_scores', {}).get('mixed', 0),
        reverse=True
    )[:3]

    social_posts = sentiment_data.get('social_posts', [])
    top_social = sorted(
        [p for p in social_posts if p.get('type') == 'story'],
        key=lambda x: x.get('points', 0), reverse=True
    )[:10]
    top_comments = sorted(
        [p for p in social_posts if p.get('type') == 'comment' and p.get('comment_text')],
        key=lambda x: x.get('points', 0), reverse=True
    )[:10]

    # Build XBRL financials section
    financials_section = "No XBRL financial data available."
    if sec_data:
        facts = sec_data.get('facts_summary') or {}
        fin = facts.get('financials', {})

        if fin:
            financials_section = "XBRL financial data from SEC filings:\n"

            for metric_name, display_name in [
                ('revenue', 'Revenue'),
                ('net_income', 'Net Income'),
                ('total_assets', 'Total Assets'),
                ('operating_income', 'Operating Income'),
                ('cash_and_equivalents', 'Cash & Equivalents'),
                ('eps_diluted', 'EPS (Diluted)'),
            ]:
                values = fin.get(metric_name, [])
                if values:
                    latest = values[0]
                    val = latest.get('value', 'N/A')
                    date = latest.get('end_date', 'N/A')
                    form = latest.get('form', '')

                    if isinstance(val, (int, float)) and abs(val) > 1_000_000:
                        if abs(val) >= 1_000_000_000:
                            formatted = f"${val / 1_000_000_000:.2f}B"
                        else:
                            formatted = f"${val / 1_000_000:.1f}M"
                    elif isinstance(val, (int, float)):
                        formatted = f"${val:.2f}"
                    else:
                        formatted = str(val)

                    financials_section += f"  - {display_name}: {formatted} (as of {date}, {form})\n"

                    if len(values) >= 2:
                        prev = values[1]
                        prev_val = prev.get('value')
                        if isinstance(val, (int, float)) and isinstance(prev_val, (int, float)) and prev_val != 0:
                            yoy = ((val - prev_val) / abs(prev_val)) * 100
                            financials_section += f"    YoY change: {yoy:+.1f}%\n"

    # NEW: Build filing narrative section from silver/filings/
    filing_narrative = "No filing narrative available."
    if filing_data:
        sections = filing_data.get('sections', {})
        filing_date = filing_data.get('filing_date', 'unknown')
        filing_narrative = f"Extracted from {filing_data.get('extraction_method', 'unknown')} "
        filing_narrative += f"(filed {filing_date}):\n\n"
        
        section_caps = {
            'risk_factors': 4000,
            'mda': 4000,
            'business': 2000,
            'legal_proceedings': 2000,
            'financial_statements': 1000,
        }
        
        for section_name, cap in section_caps.items():
            if section_name in sections:
                text = sections[section_name][:cap]
                if len(sections[section_name]) > cap:
                    text += '\n... [truncated]'
                filing_narrative += f"### {section_name.upper().replace('_', ' ')}\n{text}\n\n"
        
        if 'full_text' in sections:
            filing_narrative += f"### FULL TEXT EXCERPT\n{sections['full_text'][:3000]}\n"

    prompt = f"""You are a senior financial analyst generating a company intelligence report for {company_name} ({ticker}).

Below is data collected from multiple sources: SEC EDGAR filings (XBRL financials AND filing narrative text), news articles (analyzed by AWS Comprehend for sentiment), and Hacker News discussions. Generate a comprehensive intelligence report based ONLY on the data provided — do not make up information.

---

## FINANCIAL DATA (SEC EDGAR — XBRL)

{financials_section}

---

## FILING NARRATIVE (SEC EDGAR — Extracted Text)

{filing_narrative}

---

## NEWS SENTIMENT ANALYSIS (Comprehend)

Total articles analyzed: {agg.get('articles_analyzed', 0)}
Total social posts analyzed: {agg.get('social_posts_analyzed', 0)}

Sentiment Distribution:
  - Positive: {sentiment_dist.get('POSITIVE', 0)}
  - Negative: {sentiment_dist.get('NEGATIVE', 0)}
  - Neutral: {sentiment_dist.get('NEUTRAL', 0)}
  - Mixed: {sentiment_dist.get('MIXED', 0)}

Average Sentiment Scores:
  - Positive: {avg_scores.get('positive', 0):.4f}
  - Negative: {avg_scores.get('negative', 0):.4f}
  - Neutral: {avg_scores.get('neutral', 0):.4f}

Top Entities Mentioned:
{json.dumps(top_entities, indent=2)}

Top Key Phrases:
{json.dumps(top_phrases, indent=2)}

---

## MOST POSITIVE NEWS ARTICLES
{json.dumps([{'title': a.get('title'), 'source': a.get('source_name'), 'sentiment_scores': a.get('sentiment_scores')} for a in positive_articles], indent=2)}

## MOST NEGATIVE NEWS ARTICLES
{json.dumps([{'title': a.get('title'), 'source': a.get('source_name'), 'sentiment_scores': a.get('sentiment_scores')} for a in negative_articles], indent=2)}

## MIXED SENTIMENT ARTICLES
{json.dumps([{'title': a.get('title'), 'source': a.get('source_name'), 'sentiment_scores': a.get('sentiment_scores')} for a in mixed_articles], indent=2)}

---

## TOP HACKER NEWS DISCUSSIONS (by engagement)
{json.dumps([{'title': p.get('title'), 'points': p.get('points'), 'num_comments': p.get('num_comments'), 'sentiment': p.get('sentiment')} for p in top_social], indent=2)}

## TOP HACKER NEWS COMMENTS
{json.dumps([{'text': truncate(c.get('comment_text', '')), 'sentiment': c.get('sentiment'), 'points': c.get('points')} for c in top_comments], indent=2)}

---

## INSTRUCTIONS

Generate a structured intelligence report with EXACTLY these sections in JSON format:

{{
  "executive_summary": "3-4 sentence overall assessment combining financial performance, market sentiment, and key risks from the filing",
  "sentiment_analysis": "Detailed analysis of news and social sentiment — what themes dominate, what's driving positive/negative coverage, how social discussion differs from news coverage",
  "financial_overview": "Summary of key financial metrics, trends, and notable changes from both XBRL data and the MD&A section of the filing",
  "risk_signals": [
    {{"severity": "HIGH/MEDIUM/LOW", "signal": "Description of the risk", "source": "Which data source revealed this (e.g., 'SEC 10-K Risk Factors', 'News sentiment', 'HN discussion')"}}
  ],
  "opportunities": [
    {{"signal": "Description of the opportunity", "source": "Which data source revealed this"}}
  ],
  "key_entities": "Analysis of the most mentioned people, organizations, and their context",
  "management_outlook": "What management says about the company's direction based on the MD&A and business sections of the filing",
  "legal_regulatory": "Summary of legal proceedings and regulatory risks from the filing, if available",
  "forward_looking": "What the data suggests about near-term trajectory — combining sentiment trends, financial trajectory, management guidance, and discussion themes",
  "recommendation": "BULLISH / BEARISH / NEUTRAL with 2-3 sentence reasoning that references specific data points"
}}

IMPORTANT: Respond with ONLY the JSON object. No markdown, no code fences, no explanation before or after. Pure JSON."""

    return prompt


def call_bedrock(prompt):
    """Call Bedrock with the report generation prompt."""

    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps({
            'anthropic_version': 'bedrock-2023-05-31',
            'max_tokens': 8192,
            'temperature': 0.3,
            'messages': [
                {'role': 'user', 'content': prompt}
            ]
        })
    )

    result = json.loads(response['body'].read())

    usage = result.get('usage', {})
    print(f"Bedrock usage — input: {usage.get('input_tokens', '?')}, "
          f"output: {usage.get('output_tokens', '?')}")

    stop_reason = result.get('stop_reason')
    if stop_reason == 'max_tokens':
        raise RuntimeError(
            f"Bedrock response truncated (hit max_tokens). "
            f"Output tokens used: {usage.get('output_tokens', '?')}"
        )

    return result['content'][0]['text']


def parse_report(raw_text):
    """Parse JSON report from Bedrock response."""

    text = raw_text.strip()

    if text.startswith('```'):
        lines = text.split('\n')
        if lines[0].strip().startswith('```'):
            lines = lines[1:]
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        text = '\n'.join(lines)

    return json.loads(text)


def lambda_handler(event, context):
    """
    CHANGED: Now also reads filing_s3_key from silver/filings/
    for rich narrative content (Risk Factors, MD&A, etc.)
    """

    ticker = event.get('ticker', 'TSLA')
    company_name = event.get('company_name', 'Tesla')
    sentiment_s3_key = event.get('sentiment_s3_key')
    sec_s3_key = event.get('sec_s3_key')
    filing_s3_key = event.get('filing_s3_key')  # NEW: silver/filings/ path
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

    if not VALID_TICKER.match(ticker):
        raise ValueError(f"Invalid ticker format: {ticker!r}")

    print(f"Generating report for {company_name} ({ticker})")

    # 1. Read sentiment from gold (required)
    if not sentiment_s3_key:
        raise ValueError("sentiment_s3_key is required but was not provided")

    sentiment_data = read_s3_json(sentiment_s3_key)
    if not sentiment_data:
        raise RuntimeError(f"Sentiment data at {sentiment_s3_key} was empty")

    # 2. Read SEC XBRL financials from bronze (optional)
    sec_data = None
    if sec_s3_key:
        try:
            sec_data = read_s3_json(sec_s3_key)
        except Exception as e:
            print(f"Warning: failed to read SEC data from {sec_s3_key}: {e}")

    # 3. NEW: Read extracted filing text from silver (optional)
    filing_data = None
    if filing_s3_key:
        try:
            filing_data = read_s3_json(filing_s3_key)
            sections = filing_data.get('sections', {})
            print(f"Loaded filing sections: {list(sections.keys())}")
        except Exception as e:
            print(f"Warning: failed to read filing data from {filing_s3_key}: {e}")

    # 4. Build prompt with all sources — CHANGED: now passes filing_data
    prompt = build_prompt(ticker, company_name, sentiment_data, sec_data, filing_data)
    print(f"Prompt length: {len(prompt)} chars")

    # 5. Call Bedrock
    raw_response = call_bedrock(prompt)

    # 6. Parse report
    report = parse_report(raw_response)

    # 7. Build report package — CHANGED: includes filing_text source
    report_package = {
        'ticker': ticker,
        'company_name': company_name,
        'generated_at': timestamp,
        'model': MODEL_ID,
        'report': report,
        'data_sources': {
            'sentiment_analysis': sentiment_s3_key,
            'sec_filings': sec_s3_key,
            'filing_text': filing_s3_key,
        },
        'aggregates': sentiment_data.get('aggregates', {}),
    }

    # 8. Store in gold
    report_key = f'gold/reports/{ticker}/{timestamp}.json'

    s3.put_object(
        Bucket=BUCKET,
        Key=report_key,
        Body=json.dumps(report_package, default=str),
        ContentType='application/json'
    )
    print(f"Stored report at s3://{BUCKET}/{report_key}")

    # 9. Store metadata in DynamoDB for fast lookups
    table = dynamodb.Table(TABLE_NAME)
    try:
        table.put_item(Item={
            'ticker': ticker,
            'generated_at': timestamp,
            'company_name': company_name,
            'report_s3_key': report_key,
            'model': MODEL_ID,
            'recommendation': report.get('recommendation', 'N/A'),
            'executive_summary': str(report.get('executive_summary', '')),
            'sentiment_distribution': json.dumps(
                sentiment_data.get('aggregates', {}).get('sentiment_distribution', {})
            ),
            'articles_analyzed': str(
                sentiment_data.get('aggregates', {}).get('articles_analyzed', 0)
            ),
            'social_posts_analyzed': str(
                sentiment_data.get('aggregates', {}).get('social_posts_analyzed', 0)
            ),
            'data_sources': json.dumps({
                'sentiment_analysis': sentiment_s3_key,
                'sec_filings': sec_s3_key,
                'filing_text': filing_s3_key,
            }),
        })
        print(f"Stored report metadata in DynamoDB: {ticker}/{timestamp}")
    except Exception as e:
        print(f"Warning: failed to write to DynamoDB: {e}")

    # 10. Return metadata
    return {
        'statusCode': 200,
        'ticker': ticker,
        'company_name': company_name,
        'report_s3_key': report_key,
        'recommendation': report.get('recommendation', 'N/A'),
        'generated_at': timestamp
    }