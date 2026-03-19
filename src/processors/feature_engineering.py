import pandas as pd
import numpy as np
from ..utils.logger import setup_logger

class FeatureEngineering:
    def __init__(self):
        self.logger = setup_logger(__name__)
    
    def calculate_metrics(self, tesouro_df, selic_df, ipca_df):
        df = tesouro_df.copy()
        
        ultima_selic = selic_df['valor'].iloc[-1] if selic_df is not None and not selic_df.empty else 0.1325
        ultimo_ipca = ipca_df['valor'].iloc[-1] if ipca_df is not None and not ipca_df.empty else 0.045
        
        df['selic_atual'] = ultima_selic
        df['ipca_atual'] = ultimo_ipca
        
        mask_ipca = df['categoria'] == 'Hibrido'
        df.loc[mask_ipca, 'taxa_real'] = df.loc[mask_ipca, 'taxa_venda'] - df.loc[mask_ipca, 'ipca_atual']
        
        mask_selic = df['categoria'] == 'Pre-fixado'
        df.loc[mask_selic, 'spread_selic'] = df.loc[mask_selic, 'taxa_venda'] - ultima_selic
        
        df['duration_aprox'] = df['dias_ate_vencimento'] / 365
        
        df['preco_descontado'] = df['preco_venda'] / (1 + df['taxa_venda']) ** (df['duration_aprox'])
        
        return df