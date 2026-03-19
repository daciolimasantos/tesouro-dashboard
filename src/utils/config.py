import os
from pathlib import Path

# Diretórios base
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
ALERTS_DIR = DATA_DIR / 'alerts'

# Configurações de coleta
TESOURO_URL = 'http://www.tesouro.fazenda.gov.br/tesouro-direto-precos-e-taxas-dos-titulos'
BCB_API_URL = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados'

# Códigos de séries do BCB
SELIC_CODIGO = 432
IPCA_CODIGO = 433
CDI_CODIGO = 12

# URLs oficiais atualizadas (2026)
TESOURO_CSV_INVESTIR = "https://www.tesourodireto.com.br/documents/d/guest/rendimento-investir-csv?download=true"
TESOURO_CSV_RESGATAR = "https://www.tesourodireto.com.br/documents/d/guest/rendimento-resgatar-csv?download=true"

# URLs do Tesouro Transparente para cupons e vencimentos
TESOURO_CUPONS_URL = 'https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/de2af5cf-9dbd-4566-b933-da6871cce030/download/Cupom.csv'
TESOURO_VENCIMENTOS_URL = 'https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/34e34f7a-5d94-4e14-8d5c-0a7b0a7b0a7b/download/Vencimentos.csv'

# Configurações de alerta
ALERT_THRESHOLD = 0.02

# Logging
LOG_LEVEL = 'INFO'
LOG_FILE = BASE_DIR / 'pipeline.log'

# Timeout e retry
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# Criar diretórios
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, ALERTS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)