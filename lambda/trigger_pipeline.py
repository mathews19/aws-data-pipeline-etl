import json
import boto3
import os
import logging
from datetime import datetime
from typing import Dict, Any

# Configuração simples do logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Clientes AWS
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')
sfn_client = boto3.client('stepfunctions')
sns_client = boto3.client('sns')

# Variáveis de ambiente
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']
STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda trigger para iniciar pipeline de dados quando novo arquivo chega no S3
    """
    try:
        # Parse S3 event
        records = event.get('Records', [])
        if not records:
            logger.warning("Evento sem registros S3")
            return create_response(400, 'No S3 records found in event')

        logger.info(f"Processando {len(records)} arquivo(s)")
        processed_files = []

        for record in records:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            # Validar arquivo
            validation_result = validate_file(bucket, key)
            if not validation_result['is_valid']:
                # LOG IMPORTANTE: Falha de validação
                logger.error(f"Validação falhou: {validation_result['error']}", extra={
                    'bucket': bucket,
                    'key': key
                })
                send_notification('VALIDATION_FAILED', {
                    'file': f"s3://{bucket}/{key}",
                    'error': validation_result['error']
                })
                continue

            # Preparar parâmetros para Step Functions
            execution_input = {
                'bucket': bucket,
                'key': key,
                'file_size': validation_result['file_size'],
                'record_count': validation_result['record_count'],
                'execution_id': f"{key.replace('/', '_')}_{int(datetime.now().timestamp())}",
                'timestamp': datetime.now().isoformat()
            }

            # Iniciar Step Functions
            response = sfn_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_input['execution_id'],
                input=json.dumps(execution_input)
            )

            # LOG IMPORTANTE: Pipeline iniciado
            logger.info(f"Pipeline iniciado: s3://{bucket}/{key}", extra={
                'execution_arn': response['executionArn'],
                'record_count': validation_result['record_count']
            })

            processed_files.append({
                'file': f"s3://{bucket}/{key}",
                'execution_arn': response['executionArn'],
                'status': 'STARTED'
            })

        # Notificar sucesso
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
        # LOG CRÍTICO: Erro inesperado
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
            return {'is_valid': False, 'error': 'File is empty'}

        # Validação completa para arquivos pequenos
        if file_size < 10 * 1024 * 1024:  # 10MB
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            content = obj['Body'].read().decode('utf-8')

            try:
                data = json.loads(content)
                records = data if isinstance(data, list) else [data]

                # Validar campos obrigatórios
                required_fields = ['sale_id', 'product', 'amount', 'date']
                for idx, record in enumerate(records):
                    missing_fields = [
                        f for f in required_fields if f not in record]
                    if missing_fields:
                        return {
                            'is_valid': False,
                            'error': f"Record {idx} missing: {missing_fields}"
                        }

                return {
                    'is_valid': True,
                    'file_size': file_size,
                    'record_count': len(records)
                }

            except json.JSONDecodeError as e:
                return {'is_valid': False, 'error': f'Invalid JSON: {str(e)}'}

        # Validação básica para arquivos grandes
        return {
            'is_valid': True,
            'file_size': file_size,
            'record_count': None
        }

    except s3_client.exceptions.NoSuchKey:
        return {'is_valid': False, 'error': 'File not found'}
    except Exception as e:
        # LOG: Erro inesperado na validação
        logger.error(
            f"Erro validando s3://{bucket}/{key}: {str(e)}", exc_info=True)
        return {'is_valid': False, 'error': str(e)}


def send_notification(event_type: str, message: Any) -> None:
    """Envia notificação via SNS"""
    if not SNS_TOPIC_ARN:
        return

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f'Data Pipeline: {event_type}',
            Message=json.dumps(message, indent=2, default=str)
        )
    except Exception as e:
        # LOG: Falha na notificação (não crítico)
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
