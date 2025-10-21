"""
conftest.py - Configurações globais de teste
Este arquivo é carregado automaticamente pelo pytest ANTES de importar os módulos de teste
"""

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Adicionar path do lambda ANTES de qualquer importação
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambda'))

# IMPORTANTE: Definir variáveis de ambiente ANTES de importar o módulo
os.environ.setdefault('GLUE_JOB_NAME', 'test-glue-job')
os.environ.setdefault('STATE_MACHINE_ARN', 'arn:aws:states:us-east-1:123456789012:stateMachine:test-sfn')
os.environ.setdefault('SNS_TOPIC_ARN', 'arn:aws:sns:us-east-1:123456789012:test-topic')
os.environ.setdefault('LOG_LEVEL', 'INFO')
os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')

# Criar mocks globais ANTES de importar
_mock_s3 = MagicMock()
_mock_sfn = MagicMock()
_mock_sns = MagicMock()


# Fixture autouse que força os mocks antes de CADA teste
@pytest.fixture(autouse=True)
def setup_mocks():
    """Força os mocks antes de cada teste"""
    import trigger_pipeline
    
    # Forçar os mocks no módulo
    trigger_pipeline.s3_client = _mock_s3
    trigger_pipeline.sfn_client = _mock_sfn
    trigger_pipeline.sns_client = _mock_sns
    
    yield
    
    # Não fazer cleanup para não quebrar os testes


# Mockar boto3 para novos imports
mock_boto3 = MagicMock()
def mock_boto3_client(service_name, **kwargs):
    """Factory de mocks para boto3.client"""
    if service_name == 's3':
        return _mock_s3
    elif service_name == 'stepfunctions':
        return _mock_sfn
    elif service_name == 'sns':
        return _mock_sns
    return MagicMock()

mock_boto3.client = mock_boto3_client
sys.modules['boto3'] = mock_boto3


# Os fixtures retornam os mocks
@pytest.fixture
def mock_s3_client():
    """Acesso ao mock do S3"""
    return _mock_s3


@pytest.fixture
def mock_sfn_client():
    """Acesso ao mock do Step Functions"""
    return _mock_sfn


@pytest.fixture
def mock_sns_client():
    """Acesso ao mock do SNS"""
    return _mock_sns