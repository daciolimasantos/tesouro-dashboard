import pandas as pd
import numpy as np
from datetime import datetime
from ..utils.logger import setup_logger

class DataCleaner:
    def __init__(self):
        self.logger = setup_logger(__name__)
    
    def clean_tesouro_data(self, df):
        self.logger.info("Limpando dados do Tesouro Direto")
        
        if df is None or df.empty:
            self.logger.warning("DataFrame vazio ou nulo")
            return None
        
        df_clean = df.copy()
        
        try:
            df_clean['vencimento'] = pd.to_datetime(df_clean['vencimento'], errors='coerce')
            df_clean['data_referencia'] = pd.to_datetime(df_clean['data_referencia'], errors='coerce')
        except:
            df_clean['vencimento'] = datetime.now()
            df_clean['data_referencia'] = datetime.now()
        
        df_clean['taxa_venda'] = pd.to_numeric(df_clean['taxa_venda'], errors='coerce').fillna(0.12)
        df_clean['preco_venda'] = pd.to_numeric(df_clean['preco_venda'], errors='coerce').fillna(850)
        df_clean['taxa_compra'] = pd.to_numeric(df_clean['taxa_compra'], errors='coerce').fillna(0.11)
        df_clean['preco_compra'] = pd.to_numeric(df_clean['preco_compra'], errors='coerce').fillna(840)
        
        df_clean['dias_ate_vencimento'] = (df_clean['vencimento'] - df_clean['data_referencia']).dt.days
        df_clean.loc[df_clean['dias_ate_vencimento'] < 0, 'dias_ate_vencimento'] = 365 * 5
        
        df_clean['categoria'] = df_clean['titulo'].apply(self._categorize_titulo)
        
        self.logger.info(f"Dados limpos: {len(df_clean)} registros")
        return df_clean
    
    def _categorize_titulo(self, titulo):
        titulo_str = str(titulo).upper()
        if 'SELIC' in titulo_str:
            return 'Pos-fixado'
        elif 'IPCA' in titulo_str:
            return 'Hibrido'
        elif 'PREFIXADO' in titulo_str:
            return 'Pre-fixado'
        elif 'EDUCA' in titulo_str or 'RENDA' in titulo_str:
            return 'Programado'
        else:
            return 'Outros'