"""
Testes para Lambda trigger_pipeline
Cobre cenários positivos, negativos e edge cases
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os
from botocore.exceptions import ClientError

# Adicionar path do lambda
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambda'))


# ========== FIXTURES ==========

@pytest.fixture(scope="function")
def mock_env_vars(monkeypatch):
    """Mock de variáveis de ambiente"""
    monkeypatch.setenv('GLUE_JOB_NAME', 'test-glue-job')
    monkeypatch.setenv('STATE_MACHINE_ARN', 'arn:aws:states:us-east-1:123456789012:stateMachine:test-sfn')
    monkeypatch.setenv('SNS_TOPIC_ARN', 'arn:aws:sns:us-east-1:123456789012:test-topic')
    monkeypatch.setenv('LOG_LEVEL', 'INFO')


@pytest.fixture
def mock_s3_client():
    """Mock do cliente S3"""
    with patch('trigger_pipeline.s3_client') as mock:
        yield mock


@pytest.fixture
def mock_sfn_client():
    """Mock do cliente Step Functions"""
    with patch('trigger_pipeline.sfn_client') as mock:
        yield mock


@pytest.fixture
def mock_sns_client():
    """Mock do cliente SNS"""
    with patch('trigger_pipeline.sns_client') as mock:
        yield mock


# Importar DEPOIS dos fixtures mas ANTES dos testes
from trigger_pipeline import (
    lambda_handler,
    validate_file,
    send_notification,
    create_response
)


@pytest.fixture
def valid_s3_event():
    """Evento S3 válido"""
    return {
        'Records': [
            {
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'raw/sales/2025-01-15.json'}
                }
            }
        ]
    }


@pytest.fixture
def multiple_files_event():
    """Evento com múltiplos arquivos"""
    return {
        'Records': [
            {
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'raw/sales/2025-01-15-01.json'}
                }
            },
            {
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'raw/sales/2025-01-15-02.json'}
                }
            }
        ]
    }


@pytest.fixture
def empty_event():
    """Evento vazio (sem registros)"""
    return {}


@pytest.fixture
def valid_sales_data():
    """Dados de vendas válidos"""
    return json.dumps([
        {
            "sale_id": "S001",
            "product": "Laptop",
            "amount": "1500.00",
            "date": "2025-01-15",
            "customer_id": "C001",
            "region": "SP"
        },
        {
            "sale_id": "S002",
            "product": "Mouse",
            "amount": "50.00",
            "date": "2025-01-15"
        }
    ])


@pytest.fixture
def invalid_sales_data_missing_fields():
    """Dados com campos obrigatórios faltando"""
    return json.dumps([
        {
            "sale_id": "S001",
            "product": "Laptop"
            # Faltando: amount, date
        }
    ])


@pytest.fixture
def invalid_json():
    """JSON inválido"""
    return "{ invalid json }"


@pytest.fixture
def mock_s3_client():
    """Mock do cliente S3"""
    with patch('lambda.trigger_pipeline.s3_client') as mock:
        yield mock


@pytest.fixture
def mock_sfn_client():
    """Mock do cliente Step Functions"""
    with patch('lambda.trigger_pipeline.sfn_client') as mock:
        yield mock


@pytest.fixture
def mock_sns_client():
    """Mock do cliente SNS"""
    with patch('lambda.trigger_pipeline.sns_client') as mock:
        yield mock


# ========== TESTES UNITÁRIOS - lambda_handler ==========

class TestLambdaHandler:
    """Testes da função principal lambda_handler"""
    
    def test_debug_mock(self, mock_s3_client):
        """Teste de debug para verificar os mocks"""
        print(f"\n=== DEBUG ===")
        print(f"mock_s3_client: {mock_s3_client}")
        print(f"mock_s3_client.head_object: {mock_s3_client.head_object}")
        
        # Configurar o mock
        mock_s3_client.head_object.return_value = {'ContentLength': 1024}
        print(f"Depois de configurar return_value: {mock_s3_client.head_object.return_value}")
        
        # Chamar e ver o resultado
        result = mock_s3_client.head_object(Bucket='test', Key='test')
        print(f"Resultado da chamada: {result}")
        print(f"ContentLength: {result['ContentLength']}")
        print(f"Tipo: {type(result['ContentLength'])}")
        
        # Verificar se validate_file usa o mesmo mock
        import trigger_pipeline
        print(f"trigger_pipeline.s3_client: {trigger_pipeline.s3_client}")
        print(f"São o mesmo? {trigger_pipeline.s3_client is mock_s3_client}")
        
        assert trigger_pipeline.s3_client is mock_s3_client
        assert result['ContentLength'] == 1024
    
    def test_handler_success_single_file(
        self, 
        mock_env_vars,
        valid_s3_event,
        mock_s3_client,
        mock_sfn_client,
        mock_sns_client,
        valid_sales_data
    ):
        """Teste: Processa 1 arquivo válido com sucesso"""
        # Arrange
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1024,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=valid_sales_data.encode()))
        }
        mock_sfn_client.start_execution.return_value = {
            'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test-sfn:exec-123'
        }
        
        # Act
        response = lambda_handler(valid_s3_event, None)
        
        # Assert
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Pipeline triggered successfully'
        assert len(body['processed_files']) == 1
        assert body['processed_files'][0]['status'] == 'STARTED'
        
        # Verificar chamadas
        mock_sfn_client.start_execution.assert_called_once()
        mock_sns_client.publish.assert_called()
    
    def test_handler_success_multiple_files(
        self,
        mock_env_vars,
        multiple_files_event,
        mock_s3_client,
        mock_sfn_client,
        mock_sns_client,
        valid_sales_data
    ):
        """Teste: Processa múltiplos arquivos com sucesso"""
        # Arrange
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1024,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=valid_sales_data.encode()))
        }
        mock_sfn_client.start_execution.return_value = {
            'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test-sfn:exec-123'
        }
        
        # Act
        response = lambda_handler(multiple_files_event, None)
        
        # Assert
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert len(body['processed_files']) == 2
        
        # Verificar que Step Functions foi chamado 2 vezes
        assert mock_sfn_client.start_execution.call_count == 2
    
    def test_handler_empty_event(self, mock_env_vars, empty_event):
        """Teste: Evento vazio retorna erro 400"""
        # Act
        response = lambda_handler(empty_event, None)
        
        # Assert
        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'No S3 records' in body
    
    def test_handler_validation_failure(
        self,
        mock_env_vars,
        valid_s3_event,
        mock_s3_client,
        mock_sns_client
    ):
        """Teste: Arquivo inválido não inicia pipeline"""
        # Arrange - Simular arquivo vazio
        mock_s3_client.head_object.return_value = {
            'ContentLength': 100,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=b'[]'))
        }
        
        # Act
        response = lambda_handler(valid_s3_event, None)
        
        # Assert
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert len(body['processed_files']) == 0  # Nenhum arquivo processado
        
        # Verificar que notificação de falha foi enviada
        mock_sns_client.publish.assert_called()
        call_args = mock_sns_client.publish.call_args
        assert 'VALIDATION_FAILED' in call_args[1]['Subject']
    
    def test_handler_step_functions_error(
        self,
        mock_env_vars,
        valid_s3_event,
        mock_s3_client,
        mock_sfn_client,
        mock_sns_client,
        valid_sales_data
    ):
        """Teste: Erro ao iniciar Step Functions"""
        # Arrange
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1024,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=valid_sales_data.encode()))
        }
        mock_sfn_client.start_execution.side_effect = Exception("Step Functions error")
        
        # Act
        response = lambda_handler(valid_s3_event, None)
        
        # Assert
        assert response['statusCode'] == 500
        
        # Verificar notificação de erro
        mock_sns_client.publish.assert_called()
        call_args = mock_sns_client.publish.call_args
        assert 'PIPELINE_ERROR' in call_args[1]['Subject']


# ========== TESTES UNITÁRIOS - validate_file ==========

class TestValidateFile:
    """Testes da função validate_file"""
    
    def test_validate_file_success_small_file(self, mock_s3_client):
        """Teste: Valida arquivo pequeno com sucesso"""
        # Mock head_object - arquivo de 1MB
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1 * 1024 * 1024,  # 1MB
            'ContentType': 'application/json'
        }
        
        # Mock get_object - dados JSON válidos
        mock_data = json.dumps([
            {'sale_id': 'S001', 'product': 'test1', 'amount': '100', 'date': '2025-01-15'},
            {'sale_id': 'S002', 'product': 'test2', 'amount': '200', 'date': '2025-01-15'}
        ])
        
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=mock_data.encode('utf-8')))
        }
        
        result = validate_file('test-bucket', 'small.json')
        
        assert result is not None
        assert result['valid'] is True
        assert result['record_count'] == 2
    
    def test_validate_file_success_large_file(self, mock_s3_client):
        """Teste: Valida arquivo grande (>10MB) sem ler conteúdo"""
        mock_s3_client.head_object.return_value = {
            'ContentLength': 15 * 1024 * 1024,  # 15MB
            'ContentType': 'application/json'
        }
        
        result = validate_file('test-bucket', 'large.json')
        
        assert result is not None
        assert result['valid'] is True
        assert result['requires_batch'] is True 
        
        # Verificar que get_object NÃO foi chamado
        mock_s3_client.get_object.assert_not_called()
    
    def test_validate_file_empty(self, mock_s3_client):
        """Teste: Arquivo vazio falha validação"""
        mock_s3_client.head_object.return_value = {
            'ContentLength': 100,
            'ContentType': 'application/json'
        }
        
        # Mock get_object - array vazio
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=b'[]'))
        }
        
        result = validate_file('test-bucket', 'empty.json')
        
        assert result is not None
        assert result['valid'] is False
        assert 'empty' in result['error'].lower()
    
    def test_validate_file_not_found(self, mock_s3_client):
        """Teste: Arquivo não existe"""
        # Configurar side_effect para lançar ClientError
        mock_s3_client.head_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'Not found'}},
            operation_name='HeadObject'
        )
        
        result = validate_file('test-bucket', 'missing.json')
        
        assert result is not None
        assert result['valid'] is False
        assert 'not found' in result['error'].lower()
    
    def test_validate_file_invalid_json(self, mock_s3_client):
        """Teste: JSON inválido falha validação"""
        mock_s3_client.head_object.return_value = {
            'ContentLength': 100,
            'ContentType': 'application/json'
        }
        
        # Mock get_object - JSON inválido
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=b'{invalid json'))
        }
        
        result = validate_file('test-bucket', 'invalid.json')
        
        assert result is not None
        assert result['valid'] is False
        assert 'json' in result['error'].lower() or 'parse' in result['error'].lower()
    
    def test_validate_file_missing_required_fields(
        self,
        mock_s3_client,
        invalid_sales_data_missing_fields
    ):
        """Teste: Dados sem campos obrigatórios"""
        # Arrange
        mock_s3_client.head_object.return_value = {
            'ContentLength': 200,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=invalid_sales_data_missing_fields.encode()))
        }
        
        # Act
        result = validate_file('test-bucket', 'incomplete.json')
        
        # Assert
        assert result is not None
        assert result['valid'] is False
        assert 'missing' in result['error'].lower() or 'required' in result['error'].lower()
    
    def test_validate_file_single_object_not_array(self, mock_s3_client):
        """Teste: JSON com objeto único (não array)"""
        # Arrange
        single_object = json.dumps({
            "sale_id": "S001",
            "product": "Laptop",
            "amount": "1500.00",
            "date": "2025-01-15"
        })
        mock_s3_client.head_object.return_value = {
            'ContentLength': 150,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=single_object.encode()))
        }
        
        # Act
        result = validate_file('test-bucket', 'single.json')
        
        # Assert
        assert result is not None
        assert result['valid'] is True
        assert result['record_count'] == 1


# ========== TESTES UNITÁRIOS - send_notification ==========

class TestSendNotification:
    """Testes da função send_notification"""
    
    def test_send_notification_success(
        self,
        mock_env_vars,
        mock_sns_client
    ):
        """Teste: Envia notificação com sucesso"""
        # Act
        send_notification('TEST_EVENT', {'message': 'test'})
        
        # Assert
        mock_sns_client.publish.assert_called_once()
        call_args = mock_sns_client.publish.call_args
        assert call_args[1]['Subject'] == 'Data Pipeline: TEST_EVENT'
        assert 'test' in call_args[1]['Message']
    
    def test_send_notification_no_topic_arn(self, monkeypatch, mock_sns_client):
        """Teste: Não envia notificação se SNS_TOPIC_ARN não configurado"""
        # Arrange
        monkeypatch.delenv('SNS_TOPIC_ARN', raising=False)
        
        # Act
        send_notification('TEST_EVENT', 'message')
        
        # Assert
        mock_sns_client.publish.assert_not_called()
    
    def test_send_notification_sns_error(
        self,
        mock_env_vars,
        mock_sns_client
    ):
        """Teste: Falha ao enviar notificação não quebra execução"""
        # Arrange
        mock_sns_client.publish.side_effect = Exception("SNS error")
        
        # Act - Não deve lançar exceção
        send_notification('TEST_EVENT', 'message')
        
        # Assert - SNS foi chamado mas erro foi capturado
        mock_sns_client.publish.assert_called_once()


# ========== TESTES UNITÁRIOS - create_response ==========

class TestCreateResponse:
    """Testes da função create_response"""
    
    def test_create_response_200(self):
        """Teste: Cria resposta 200 corretamente"""
        # Act
        response = create_response(200, {'message': 'success'})
        
        # Assert
        assert response['statusCode'] == 200
        assert response['headers']['Content-Type'] == 'application/json'
        body = json.loads(response['body'])
        assert body['message'] == 'success'
    
    def test_create_response_with_datetime(self):
        """Teste: Serializa datetime corretamente"""
        # Act
        now = datetime.now()
        response = create_response(200, {'timestamp': now})
        
        # Assert
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert 'timestamp' in body
    
    def test_create_response_error(self):
        """Teste: Cria resposta de erro"""
        # Act
        response = create_response(500, 'Internal error')
        
        # Assert
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body == 'Internal error'


# ========== TESTES DE INTEGRAÇÃO (LocalStack) ==========

@pytest.mark.integration
class TestLambdaIntegration:
    """Testes de integração com LocalStack"""
    
    @pytest.fixture
    def localstack_s3_client(self):
        """Cliente S3 apontando para LocalStack"""
        import boto3
        return boto3.client(
            's3',
            endpoint_url='http://localhost:4566',
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
    
    def test_integration_full_flow(
        self,
        localstack_s3_client,
        valid_sales_data,
        mock_env_vars,
        mock_sfn_client,
        mock_sns_client
    ):
        """Teste: Fluxo completo com S3 real (LocalStack)"""
        # Arrange
        bucket = 'test-integration-bucket'
        key = 'raw/sales/integration-test.json'
        
        # Criar bucket no LocalStack
        try:
            localstack_s3_client.create_bucket(Bucket=bucket)
        except:
            pass  # Bucket já existe
        
        # Upload arquivo
        localstack_s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=valid_sales_data.encode()
        )
        
        # Criar evento S3
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': bucket},
                    'object': {'key': key}
                }
            }]
        }
        
        # Mock Step Functions (não disponível no LocalStack free)
        mock_sfn_client.start_execution.return_value = {
            'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test'
        }
        
        # Act
        with patch('lambda.trigger_pipeline.s3_client', localstack_s3_client):
            response = lambda_handler(event, None)
        
        # Assert
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert len(body['processed_files']) == 1


# ========== TESTES DE EDGE CASES ==========

class TestEdgeCases:
    """Testes de casos extremos"""
    
    def test_very_long_filename(
        self,
        mock_env_vars,
        mock_s3_client,
        mock_sfn_client,
        mock_sns_client,
        valid_sales_data
    ):
        """Teste: Nome de arquivo muito longo"""
        # Arrange
        long_key = 'raw/sales/' + 'a' * 500 + '.json'
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': 'bucket'},
                    'object': {'key': long_key}
                }
            }]
        }
        
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1024,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=valid_sales_data.encode()))
        }
        mock_sfn_client.start_execution.return_value = {
            'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test'
        }
        
        # Act
        response = lambda_handler(event, None)
        
        # Assert
        assert response['statusCode'] == 200
    
    def test_special_characters_in_filename(
        self,
        mock_env_vars,
        mock_s3_client,
        mock_sfn_client,
        mock_sns_client,
        valid_sales_data
    ):
        """Teste: Caracteres especiais no nome do arquivo"""
        # Arrange
        special_key = 'raw/sales/file with spaces & special!@#$.json'
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': 'bucket'},
                    'object': {'key': special_key}
                }
            }]
        }
        
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1024,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=valid_sales_data.encode()))
        }
        mock_sfn_client.start_execution.return_value = {
            'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test'
        }
        
        # Act
        response = lambda_handler(event, None)
        
        # Assert
        assert response['statusCode'] == 200
    
    def test_exactly_10mb_file(self, mock_s3_client, valid_sales_data):
        """Teste: Arquivo exatamente 10MB (boundary condition)"""
        # Arrange
        exactly_10mb = 10 * 1024 * 1024
        
        mock_s3_client.head_object.return_value = {
            'ContentLength': exactly_10mb,
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=valid_sales_data.encode()))
        }
        
        # Act
        result = validate_file('bucket', 'exactly-10mb.json')
        
        # Assert
        assert result is not None
        assert result['valid'] is True
        # Com 10MB exato, ainda deve ler o conteúdo (<=)
        assert result['record_count'] == 2


# ========== TESTES DE PERFORMANCE ==========

@pytest.mark.performance
class TestPerformance:
    """Testes de performance"""
    
    def test_handles_100_records(self, mock_s3_client):
        """Teste: Processa arquivo com 100 registros"""
        # Arrange
        large_data = json.dumps([
            {
                "sale_id": f"S{i:05d}",
                "product": "Laptop",
                "amount": "1500.00",
                "date": "2025-01-15"
            }
            for i in range(100)
        ])
        
        mock_s3_client.head_object.return_value = {
            'ContentLength': len(large_data),
            'ContentType': 'application/json'
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=large_data.encode()))
        }
        
        # Act
        import time
        start = time.time()
        result = validate_file('bucket', 'large.json')
        duration = time.time() - start
        
        # Assert
        assert result is not None
        assert result['valid'] is True
        assert result['record_count'] == 100
        assert duration < 1.0  # Deve processar em menos de 1 segundo


# ========== MARKERS CUSTOMIZADOS ==========

# Para rodar apenas testes rápidos:
# pytest -m "not integration and not performance"

# Para rodar com LocalStack:
# pytest -m integration

# Para rodar testes de performance:
# pytest -m performance