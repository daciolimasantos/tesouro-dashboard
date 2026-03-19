import numpy as np
import pandas as pd
from datetime import datetime
from ..utils.logger import setup_logger

class FairPriceCalculator:
    def __init__(self):
        self.logger = setup_logger(__name__)
    
    def calculate_fair_price(self, df):
        self.logger.info("Calculando preços teóricos")
        
        df_analyzed = df.copy()
        df_analyzed['preco_teorico'] = np.nan
        df_analyzed['desconto'] = np.nan
        df_analyzed['oportunidade_score'] = 0
        
        for idx, row in df_analyzed.iterrows():
            try:
                if 'IPCA' in str(row['titulo']).upper():
                    fair_price = self._price_ipca_plus(row)
                elif 'PREFIXADO' in str(row['titulo']).upper():
                    fair_price = self._price_prefixado(row)
                elif 'SELIC' in str(row['titulo']).upper():
                    fair_price = self._price_selic(row)
                else:
                    fair_price = self._price_generic(row)
                
                df_analyzed.loc[idx, 'preco_teorico'] = fair_price
                
                if fair_price > 0 and row['preco_venda'] > 0:
                    desconto = (row['preco_venda'] - fair_price) / fair_price
                    df_analyzed.loc[idx, 'desconto'] = desconto
                    df_analyzed.loc[idx, 'oportunidade_score'] = max(0, -desconto * 100)
                    
            except Exception as e:
                self.logger.error(f"Erro calculando preço para {row['titulo']}: {str(e)}")
                continue
        
        return df_analyzed
    
    def _price_ipca_plus(self, row):
        valor_nominal = 1000
        anos_ate_venc = max(row['dias_ate_vencimento'] / 365, 0.1)
        taxa_real = row['taxa_venda'] / 100
        
        # Ajuste para cupons se existirem
        if 'total_cupons_restantes' in row and row['total_cupons_restantes'] > 0:
            if 'valor_proximo_cupom' in row and pd.notna(row['valor_proximo_cupom']):
                valor_cupons = row['valor_proximo_cupom'] * row['total_cupons_restantes'] * 0.5
                preco = (valor_nominal + valor_cupons) / ((1 + taxa_real) ** anos_ate_venc)
            else:
                preco = valor_nominal / ((1 + taxa_real) ** anos_ate_venc)
        else:
            preco = valor_nominal / ((1 + taxa_real) ** anos_ate_venc)
        
        return preco
    
    def _price_prefixado(self, row):
        valor_nominal = 1000
        anos_ate_venc = max(row['dias_ate_vencimento'] / 365, 0.1)
        taxa = row['taxa_venda'] / 100
        preco = valor_nominal / ((1 + taxa) ** anos_ate_venc)
        return preco
    
    def _price_selic(self, row):
        # Títulos Selic têm preço próximo ao valor nominal
        return 1000 * (1 - (row['taxa_venda'] / 100) * 0.01)
    
    def _price_generic(self, row):
        valor_nominal = 1000
        anos_ate_venc = max(row['dias_ate_vencimento'] / 365, 0.1)
        taxa = row['taxa_venda'] / 100
        return valor_nominal / ((1 + taxa) ** anos_ate_venc)