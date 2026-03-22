import json
import os
import re
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
sfn = boto3.client('stepfunctions')

TABLE_NAME = os.environ.get('DYNAMO_TABLE', 'company-intel-reports')
BUCKET = os.environ.get('S3_BUCKET', 'company-intel-datalake-satvik')
STATE_MACHINE_ARN = os.environ.get(
    'STATE_MACHINE_ARN',
    'arn:aws:states:us-east-1:027355625929:stateMachine:company-intel-pipeline'
)

VALID_TICKER = re.compile(r'^[A-Z0-9\-.]{1,10}$')

# CORS headers — required for browser-based frontend to call the API
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
}


def respond(status_code, body):
    """Build API Gateway proxy response."""
    return {
        'statusCode': status_code,
        'headers': CORS_HEADERS,
        'body': json.dumps(body, default=str),
    }


def handle_analyze(body):
    """
    POST /analyze — Trigger the pipeline for a company.
    
    Starts a Step Functions execution. Returns the execution ID
    so the frontend can poll for status.
    """
    
    if not body:
        return respond(400, {'error': 'Request body is required'})
    
    ticker = body.get('ticker', '').upper().strip()
    company_name = body.get('company_name', '').strip()
    
    if not ticker or not company_name:
        return respond(400, {'error': 'Both ticker and company_name are required'})
    
    if not VALID_TICKER.match(ticker):
        return respond(400, {'error': f'Invalid ticker format: {ticker}'})
    
    try:
        execution = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps({
                'companies': [
                    {'ticker': ticker, 'company_name': company_name}
                ]
            })
        )
        
        execution_arn = execution['executionArn']
        execution_id = execution_arn.split(':')[-1]
        
        print(f"Started pipeline for {company_name} ({ticker}): {execution_id}")
        
        return respond(202, {
            'message': f'Analysis started for {company_name} ({ticker})',
            'execution_id': execution_id,
            'ticker': ticker,
        })
        
    except Exception as e:
        print(f"Failed to start pipeline: {e}")
        return respond(500, {'error': f'Failed to start analysis: {str(e)}'})


def handle_get_report(ticker):
    """
    GET /report/{ticker} — Get the latest report for a company.
    
    Queries DynamoDB for the most recent report metadata,
    then fetches the full report from S3.
    """
    
    ticker = ticker.upper().strip()
    
    if not VALID_TICKER.match(ticker):
        return respond(400, {'error': f'Invalid ticker format: {ticker}'})
    
    table = dynamodb.Table(TABLE_NAME)
    
    try:
        # Query with ScanIndexForward=False to get latest first
        response = table.query(
            KeyConditionExpression=Key('ticker').eq(ticker),
            ScanIndexForward=False,
            Limit=1
        )
        
        items = response.get('Items', [])
        
        if not items:
            return respond(404, {
                'error': f'No report found for {ticker}',
                'ticker': ticker,
            })
        
        metadata = items[0]
        report_s3_key = metadata.get('report_s3_key')
        
        # Fetch full report from S3
        if report_s3_key:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=report_s3_key)
                full_report = json.loads(obj['Body'].read().decode('utf-8'))
                
                return respond(200, {
                    'ticker': ticker,
                    'metadata': metadata,
                    'report': full_report,
                })
            except Exception as e:
                print(f"Failed to fetch report from S3: {e}")
                # Fall back to metadata only
                return respond(200, {
                    'ticker': ticker,
                    'metadata': metadata,
                    'report': None,
                    'warning': 'Full report unavailable from S3',
                })
        
        return respond(200, {
            'ticker': ticker,
            'metadata': metadata,
            'report': None,
        })
        
    except Exception as e:
        print(f"DynamoDB query failed: {e}")
        return respond(500, {'error': f'Failed to fetch report: {str(e)}'})


def handle_list_reports():
    """
    GET /reports — List all available reports.
    
    Scans DynamoDB for the latest report per company.
    Returns metadata only (not full reports) for fast loading.
    """
    
    table = dynamodb.Table(TABLE_NAME)
    
    try:
        # Scan all items — fine for <100 companies
        response = table.scan()
        items = response.get('Items', [])
        
        # Get latest report per ticker
        latest = {}
        for item in items:
            ticker = item['ticker']
            if ticker not in latest or item['generated_at'] > latest[ticker]['generated_at']:
                latest[ticker] = item
        
        # Sort by generated_at descending
        reports = sorted(
            latest.values(),
            key=lambda x: x.get('generated_at', ''),
            reverse=True
        )
        
        return respond(200, {
            'total': len(reports),
            'reports': reports,
        })
        
    except Exception as e:
        print(f"DynamoDB scan failed: {e}")
        return respond(500, {'error': f'Failed to list reports: {str(e)}'})


def handle_execution_status(execution_id):
    """
    GET /status/{execution_id} — Check pipeline execution status.
    
    Frontend polls this after triggering /analyze to know
    when the report is ready.
    """
    
    try:
        # Reconstruct the full ARN from the execution ID
        base_arn = STATE_MACHINE_ARN.replace(':stateMachine:', ':execution:')
        execution_arn = f"{base_arn}:{execution_id}"
        
        response = sfn.describe_execution(executionArn=execution_arn)
        
        status = response['status']
        result = {
            'execution_id': execution_id,
            'status': status,
            'start_time': response.get('startDate'),
            'end_time': response.get('stopDate'),
        }
        
        # If succeeded, parse the output to get report location
        if status == 'SUCCEEDED' and response.get('output'):
            try:
                output = json.loads(response['output'])
                results = output.get('results', [])
                if results and isinstance(results, list):
                    first = results[0]
                    result['ticker'] = first.get('ticker')
                    result['report_s3_key'] = first.get('report_s3_key')
                    result['recommendation'] = first.get('recommendation')
            except (json.JSONDecodeError, IndexError, TypeError):
                pass
        
        return respond(200, result)
        
    except sfn.exceptions.ExecutionDoesNotExist:
        return respond(404, {'error': f'Execution {execution_id} not found'})
    except Exception as e:
        print(f"Failed to get execution status: {e}")
        return respond(500, {'error': f'Failed to get status: {str(e)}'})


def lambda_handler(event, context):
    """
    API Gateway proxy handler. Routes requests based on
    HTTP method and path.
    """
    
    method = event.get('httpMethod', '')
    path = event.get('path', '')
    
    print(f"{method} {path}")
    
    # Handle CORS preflight
    if method == 'OPTIONS':
        return respond(200, {})
    
    # POST /analyze
    if method == 'POST' and path == '/analyze':
        body = None
        if event.get('body'):
            try:
                body = json.loads(event['body'])
            except json.JSONDecodeError:
                return respond(400, {'error': 'Invalid JSON in request body'})
        return handle_analyze(body)
    
    # GET /report/{ticker}
    if method == 'GET' and path.startswith('/report/'):
        ticker = path.split('/')[-1]
        return handle_get_report(ticker)
    
    # GET /reports
    if method == 'GET' and path == '/reports':
        return handle_list_reports()
    
    # GET /status/{execution_id}
    if method == 'GET' and path.startswith('/status/'):
        execution_id = path.split('/')[-1]
        return handle_execution_status(execution_id)
    
    return respond(404, {'error': f'Not found: {method} {path}'})
