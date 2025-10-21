#!/bin/bash

# ========================================
# Script de Testes Automático
# ========================================
# Roda todos os testes com interface bonita
# Uso: ./run_tests.sh [opção]
#
# Opções:
#   all        - Todos os testes (padrão)
#   unit       - Apenas testes unitários
#   integration - Apenas testes de integração
#   coverage   - Gera relatório de coverage
#   ci         - Modo CI/CD
#   watch      - Watch mode (reroda ao salvar)
# ========================================

set -e  # Parar em erro

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Funções de print
print_header() {
    echo -e "${CYAN}"
    echo "========================================="
    echo "$1"
    echo "========================================="
    echo -e "${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Verificar se pytest está instalado
check_dependencies() {
    print_header "Verificando Dependências"
    
    if ! command -v pytest &> /dev/null; then
        print_error "pytest não encontrado"
        print_info "Instale com: pip install -r requirements-dev.txt"
        exit 1
    fi
    
    print_success "pytest $(pytest --version | cut -d' ' -f2)"
    echo ""
}

# Rodar testes unitários
run_unit_tests() {
    print_header "🧪 Rodando Testes Unitários"
    
    pytest \
        -m "not integration and not performance" \
        -v \
        --tb=short \
        --color=yes
    
    if [ $? -eq 0 ]; then
        print_success "Testes unitários passaram!"
    else
        print_error "Testes unitários falharam"
        exit 1
    fi
    echo ""
}

# Rodar testes de integração
run_integration_tests() {
    print_header "🔌 Rodando Testes de Integração"
    
    # Verificar se LocalStack está rodando
    print_info "Verificando LocalStack..."
    if ! curl -s http://localhost:4566/_localstack/health > /dev/null 2>&1; then
        print_warning "LocalStack não está rodando"
        print_info "Iniciando LocalStack..."
        
        docker compose up -d
        
        print_info "Aguardando LocalStack inicializar (20 segundos)..."
        sleep 20
    fi
    
    if curl -s http://localhost:4566/_localstack/health > /dev/null 2>&1; then
        print_success "LocalStack OK"
    else
        print_error "LocalStack não respondeu"
        exit 1
    fi
    
    pytest \
        -m integration \
        -v \
        --tb=short \
        --color=yes
    
    if [ $? -eq 0 ]; then
        print_success "Testes de integração passaram!"
    else
        print_error "Testes de integração falharam"
        exit 1
    fi
    echo ""
}

# Rodar todos os testes
run_all_tests() {
    print_header "🚀 Rodando TODOS os Testes"
    
    pytest \
        -v \
        --tb=short \
        --color=yes
    
    if [ $? -eq 0 ]; then
        print_success "Todos os testes passaram!"
    else
        print_error "Alguns testes falharam"
        exit 1
    fi
    echo ""
}

# Gerar relatório de coverage
run_coverage() {
    print_header "📊 Gerando Relatório de Coverage"
    
    pytest \
        --cov=lambda \
        --cov-report=term-missing \
        --cov-report=html \
        --cov-fail-under=80 \
        -v
    
    if [ $? -eq 0 ]; then
        print_success "Coverage gerado com sucesso!"
        print_info "Abrir relatório: open htmlcov/index.html"
    else
        print_error "Coverage abaixo de 80%"
        exit 1
    fi
    echo ""
}

# Modo CI/CD
run_ci() {
    print_header "🔄 Modo CI/CD"
    
    print_info "Rodando testes unitários..."
    pytest \
        -m "not integration and not performance" \
        --cov=lambda \
        --cov-report=xml \
        --cov-report=term \
        --cov-fail-under=80 \
        --junitxml=test-results.xml \
        -v
    
    if [ $? -eq 0 ]; then
        print_success "CI passou!"
        print_info "Relatórios gerados:"
        print_info "  - coverage.xml"
        print_info "  - test-results.xml"
    else
        print_error "CI falhou"
        exit 1
    fi
    echo ""
}

# Watch mode
run_watch() {
    print_header "👀 Watch Mode"
    
    if ! command -v ptw &> /dev/null; then
        print_warning "pytest-watch não instalado"
        print_info "Instalando..."
        pip install pytest-watch
    fi
    
    print_info "Monitorando mudanças... (Ctrl+C para parar)"
    ptw -- -m "not integration" --tb=short
}

# Estatísticas
show_stats() {
    print_header "📊 Estatísticas dos Testes"
    
    TOTAL=$(grep -r "def test_" tests/ | wc -l | xargs)
    UNIT=$(grep -r "@pytest.mark.unit\|def test_" tests/ | grep -v "integration\|performance" | wc -l | xargs)
    INTEGRATION=$(grep -r "@pytest.mark.integration" tests/ | wc -l | xargs)
    PERFORMANCE=$(grep -r "@pytest.mark.performance" tests/ | wc -l | xargs)
    
    echo -e "${BLUE}Total de testes: ${NC}${TOTAL}"
    echo -e "${BLUE}Testes unitários: ${NC}${UNIT}"
    echo -e "${BLUE}Testes de integração: ${NC}${INTEGRATION}"
    echo -e "${BLUE}Testes de performance: ${NC}${PERFORMANCE}"
    echo ""
}

# Menu de ajuda
show_help() {
    echo "Uso: ./run_tests.sh [opção]"
    echo ""
    echo "Opções:"
    echo "  all          Rodar todos os testes (padrão)"
    echo "  unit         Apenas testes unitários (rápido)"
    echo "  integration  Apenas testes de integração (requer LocalStack)"
    echo "  coverage     Gerar relatório de coverage"
    echo "  ci           Modo CI/CD (gera XML)"
    echo "  watch        Watch mode (reroda ao salvar)"
    echo "  stats        Mostrar estatísticas"
    echo "  help         Mostrar esta ajuda"
    echo ""
    echo "Exemplos:"
    echo "  ./run_tests.sh              # Roda todos"
    echo "  ./run_tests.sh unit         # Só unitários"
    echo "  ./run_tests.sh coverage     # Com coverage"
}

# ========== MAIN ==========

# Banner
echo -e "${PURPLE}"
cat << "EOF"
╔═══════════════════════════════════════╗
║   🧪  Test Runner - Lambda Pipeline   ║
╚═══════════════════════════════════════╝
EOF
echo -e "${NC}"

# Verificar dependências
check_dependencies

# Pegar opção
OPTION=${1:-all}

# Timestamp
START_TIME=$(date +%s)

# Executar baseado na opção
case $OPTION in
    all)
        run_all_tests
        ;;
    unit)
        run_unit_tests
        ;;
    integration)
        run_integration_tests
        ;;
    coverage)
        run_coverage
        ;;
    ci)
        run_ci
        ;;
    watch)
        run_watch
        ;;
    stats)
        show_stats
        exit 0
        ;;
    help|--help|-h)
        show_help
        exit 0
        ;;
    *)
        print_error "Opção inválida: $OPTION"
        show_help
        exit 1
        ;;
esac

# Calcular tempo
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Sumário final
print_header "📋 Sumário"
print_success "Testes concluídos em ${DURATION}s"

# Mostrar próximos passos
echo ""
print_info "Próximos passos:"
echo "  - Ver coverage: open htmlcov/index.html"
echo "  - Rodar CI: ./run_tests.sh ci"
echo "  - Watch mode: ./run_tests.sh watch"

echo ""
print_success "Tudo certo! 🎉"