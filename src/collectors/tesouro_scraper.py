import requests
import pandas as pd
import io
from datetime import datetime
from ..utils.logger import setup_logger
from ..utils.config import RAW_DATA_DIR, TESOURO_CSV_INVESTIR, TESOURO_CSV_RESGATAR

class TesouroScraper:
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.urls = {
            'investir': TESOURO_CSV_INVESTIR,
            'resgatar': TESOURO_CSV_RESGATAR
        }
        
    def fetch_data(self):
        try:
            self.logger.info("Iniciando coleta de dados do Tesouro Direto (CSV oficial)")
            
            df_investir = self._fetch_csv(self.urls['investir'], 'investir')
            df_resgatar = self._fetch_csv(self.urls['resgatar'], 'resgatar')
            
            if df_investir is not None and df_resgatar is not None:
                df = self._merge_data(df_investir, df_resgatar)
            elif df_investir is not None:
                df = df_investir
            elif df_resgatar is not None:
                df = df_resgatar
            else:
                self.logger.error("Falha ao coletar dados das fontes oficiais")
                return self._generate_sample_data()
            
            filename = RAW_DATA_DIR / f"tesouro_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            
            self.logger.info(f"Dados salvos em {filename}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro na coleta: {str(e)}")
            self.logger.info("Usando dados simulados como fallback")
            return self._generate_sample_data()
    
    def _fetch_csv(self, url, tipo):
        try:
            self.logger.info(f"Baixando CSV de {tipo}...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            content = response.content.decode('latin-1')
            df = pd.read_csv(io.StringIO(content), sep=';', encoding='latin-1')
            
            self.logger.info(f"CSV de {tipo} baixado com sucesso: {len(df)} linhas")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao baixar CSV de {tipo}: {str(e)}")
            return None
    
    def _merge_data(self, df_investir, df_resgatar):
        df_investir.columns = [col.strip() for col in df_investir.columns]
        df_resgatar.columns = [col.strip() for col in df_resgatar.columns]
        
        df_merged = pd.DataFrame()
        
        if 'Título' in df_investir.columns:
            df_merged['titulo'] = df_investir['Título']
        elif 'Titulo' in df_investir.columns:
            df_merged['titulo'] = df_investir['Titulo']
        
        if 'Vencimento' in df_investir.columns:
            df_merged['vencimento'] = pd.to_datetime(df_investir['Vencimento'], errors='coerce', dayfirst=True)
        else:
            df_merged['vencimento'] = datetime.now()
        
        if 'Taxa' in df_investir.columns:
            df_merged['taxa_compra'] = pd.to_numeric(
                df_investir['Taxa'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
        
        if 'Taxa' in df_resgatar.columns:
            df_merged['taxa_venda'] = pd.to_numeric(
                df_resgatar['Taxa'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
        
        if 'Preço Unitário' in df_investir.columns:
            df_merged['preco_compra'] = pd.to_numeric(
                df_investir['Preço Unitário'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
        
        if 'Preço Unitário' in df_resgatar.columns:
            df_merged['preco_venda'] = pd.to_numeric(
                df_resgatar['Preço Unitário'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
        
        df_merged['data_referencia'] = datetime.now()
        df_merged['data_coleta'] = datetime.now()
        df_merged['tipo'] = df_merged['titulo'].apply(self._infer_tipo)
        
        return df_merged
    
    def _infer_tipo(self, titulo):
        titulo_str = str(titulo).upper()
        if 'IPCA' in titulo_str:
            return 'IPCA'
        elif 'SELIC' in titulo_str:
            return 'SELIC'
        elif 'PREFIXADO' in titulo_str:
            return 'PREFIXADO'
        else:
            return 'OUTROS'
    
    def _generate_sample_data(self):
        self.logger.info("Gerando dados simulados para fallback")
        
        import numpy as np
        import random
        
        dados = []
        titulos = [
            {'nome': 'Tesouro Prefixado 2026', 'tipo': 'PREFIXADO', 'vencimento': '2026-12-31'},
            {'nome': 'Tesouro Prefixado 2029', 'tipo': 'PREFIXADO', 'vencimento': '2029-12-31'},
            {'nome': 'Tesouro Prefixado 2032', 'tipo': 'PREFIXADO', 'vencimento': '2032-12-31'},
            {'nome': 'Tesouro IPCA+ 2035', 'tipo': 'IPCA', 'vencimento': '2035-05-15'},
            {'nome': 'Tesouro IPCA+ 2040', 'tipo': 'IPCA', 'vencimento': '2040-05-15'},
            {'nome': 'Tesouro IPCA+ 2045', 'tipo': 'IPCA', 'vencimento': '2045-05-15'},
            {'nome': 'Tesouro IPCA+ 2050', 'tipo': 'IPCA', 'vencimento': '2050-05-15'},
            {'nome': 'Tesouro IPCA+ Juros 2037', 'tipo': 'IPCA', 'vencimento': '2037-08-15'},
            {'nome': 'Tesouro IPCA+ Juros 2045', 'tipo': 'IPCA', 'vencimento': '2045-08-15'},
            {'nome': 'Tesouro IPCA+ Juros 2060', 'tipo': 'IPCA', 'vencimento': '2060-08-15'},
            {'nome': 'Tesouro Selic 2027', 'tipo': 'SELIC', 'vencimento': '2027-03-01'},
            {'nome': 'Tesouro Selic 2031', 'tipo': 'SELIC', 'vencimento': '2031-03-01'},
            {'nome': 'Tesouro Reserva 2029', 'tipo': 'SELIC', 'vencimento': '2029-06-01'},
            {'nome': 'Tesouro Educa+ 2031', 'tipo': 'IPCA', 'vencimento': '2031-12-15'},
            {'nome': 'Tesouro Educa+ 2036', 'tipo': 'IPCA', 'vencimento': '2036-12-15'},
            {'nome': 'Tesouro Educa+ 2041', 'tipo': 'IPCA', 'vencimento': '2041-12-15'},
            {'nome': 'Tesouro Renda+ 2045', 'tipo': 'IPCA', 'vencimento': '2045-12-15'},
            {'nome': 'Tesouro Renda+ 2050', 'tipo': 'IPCA', 'vencimento': '2050-12-15'},
        ]
        
        for titulo in titulos:
            taxa_base = np.random.uniform(0.10, 0.14)
            preco_base = np.random.uniform(800, 1000)
            dado = {
                'titulo': titulo['nome'],
                'tipo': titulo['tipo'],
                'vencimento': titulo['vencimento'],
                'data_referencia': datetime.now(),
                'taxa_compra': round(taxa_base - 0.001, 4),
                'taxa_venda': round(taxa_base, 4),
                'preco_compra': round(preco_base - random.uniform(5, 15), 2),
                'preco_venda': round(preco_base, 2),
                'data_coleta': datetime.now()
            }
            dados.append(dado)
        
        return pd.DataFrame(dados)