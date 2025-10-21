import json
import boto3
from botocore.exceptions import ClientError
import os
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')
sfn_client = boto3.client('stepfunctions')
sns_client = boto3.client('sns')

GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']
STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda trigger para iniciar pipeline de dados quando novo arquivo chega no S3
    """
    try:

        records = event.get('Records', [])
        if not records:
            logger.warning("Evento sem registros S3")
            return create_response(400, 'No S3 records found in event')

        logger.info(f"Processando {len(records)} arquivo(s)")
        processed_files = []

        for record in records:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            validation_result = validate_file(bucket, key)
            if not validation_result['is_valid']:

                logger.error(f"Validação falhou: {validation_result['error']}", extra={
                    'bucket': bucket,
                    'key': key
                })
                send_notification('VALIDATION_FAILED', {
                    'file': f"s3://{bucket}/{key}",
                    'error': validation_result['error']
                })
                continue

            execution_input = {
                'bucket': bucket,
                'key': key,
                'file_size': validation_result['file_size'],
                'record_count': validation_result['record_count'],
                'execution_id': f"{key.replace('/', '_')}_{int(datetime.now().timestamp())}",
                'timestamp': datetime.now().isoformat()
            }

            response = sfn_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_input['execution_id'],
                input=json.dumps(execution_input)
            )

            logger.info(f"Pipeline iniciado: s3://{bucket}/{key}", extra={
                'execution_arn': response['executionArn'],
                'record_count': validation_result['record_count']
            })

            processed_files.append({
                'file': f"s3://{bucket}/{key}",
                'execution_arn': response['executionArn'],
                'status': 'STARTED'
            })

        if processed_files:
            send_notification('PIPELINE_STARTED', {
                'files_processed': len(processed_files),
                'executions': processed_files
            })

        return create_response(200, {
            'message': 'Pipeline triggered successfully',
            'processed_files': processed_files
        })

    except Exception as e:

        logger.error(f"Erro crítico no pipeline: {str(e)}", exc_info=True)
        send_notification('PIPELINE_ERROR', str(e))
        return create_response(500, str(e))

def validate_file(bucket: str, key: str) -> Dict[str, Any]:
    """
    Valida arquivo S3 antes de processar
    """
    try:
        # Obter metadados do arquivo
        response = s3_client.head_object(Bucket=bucket, Key=key)
        file_size = response['ContentLength']

        if file_size == 0:
            return {'valid': False, 'error': 'File is empty'}

        # Validação completa para arquivos pequenos
        if file_size <= 10 * 1024 * 1024:  # 10MB
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            content = obj['Body'].read().decode('utf-8')

            try:
                data = json.loads(content)
                records = data if isinstance(data, list) else [data]

                # Verificar se array está vazio
                if len(records) == 0:
                    return {'valid': False, 'error': 'File contains empty array'}

                # Validar campos obrigatórios
                required_fields = ['sale_id', 'product', 'amount', 'date']
                for idx, record in enumerate(records):
                    missing_fields = [
                        f for f in required_fields if f not in record]
                    if missing_fields:
                        return {
                            'valid': False,
                            'error': f"Record {idx} missing required fields: {missing_fields}"
                        }

                return {
                    'valid': True,
                    'file_size': file_size,
                    'record_count': len(records)
                }

            except json.JSONDecodeError as e:
                return {'valid': False, 'error': f'Invalid JSON: {str(e)}'}

        # Arquivo grande (>10MB) - retorna válido mas requer processamento em batch
        return {
            'valid': True,
            'file_size': file_size,
            'record_count': None,
            'requires_batch': True
        }

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {'valid': False, 'error': 'File not found'}
        # Re-lançar outros erros ClientError
        raise
    except Exception as e:
        logger.error(
            f"Erro validando s3://{bucket}/{key}: {str(e)}", exc_info=True)
        return {'valid': False, 'error': str(e)}


def send_notification(event_type: str, message: Any) -> None:
    """Envia notificação via SNS"""
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    
    if not topic_arn:
        logger.info("SNS_TOPIC_ARN not configured, skipping notification")
        return

    try:
        sns_client.publish(
            TopicArn=topic_arn,  # Usar a variável, não a constante
            Subject=f'Data Pipeline: {event_type}',
            Message=json.dumps(message, indent=2, default=str)
        )
        logger.info(f"Notification sent: {event_type}")
    except Exception as e:
        logger.warning(f"Falha ao enviar notificação: {str(e)}")


def send_notification(event_type: str, message: Any) -> None:
    """Envia notificação via SNS"""
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    if not topic_arn:
        logger.info("SNS_TOPIC_ARN not configured, skipping notification")
        return

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f'Data Pipeline: {event_type}',
            Message=json.dumps(message, indent=2, default=str)
        )
    except Exception as e:

        logger.warning(f"Falha ao enviar notificação: {str(e)}")


def create_response(status_code: int, body: Any) -> Dict[str, Any]:
    """Cria resposta padronizada"""
    return {
        'statusCode': status_code,
        'body': json.dumps(body, default=str),
        'headers': {
            'Content-Type': 'application/json'
        }
    }
