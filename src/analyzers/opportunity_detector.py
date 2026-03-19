import pandas as pd
import numpy as np
from ..utils.logger import setup_logger
from ..utils.config import ALERT_THRESHOLD

class OpportunityDetector:
    def __init__(self):
        self.logger = setup_logger(__name__)
    
    def detect_opportunities(self, df):
        self.logger.info("Detectando oportunidades")
        
        opportunities = []
        
        for idx, row in df.iterrows():
            criterios = []
            
            if pd.notna(row.get('desconto')):
                if row['desconto'] < -ALERT_THRESHOLD:
                    criterios.append({
                        'tipo': 'desconto',
                        'intensidade': abs(row['desconto']),
                        'descricao': f"Desconto de {abs(row['desconto']*100):.2f}%"
                    })
            
            if row.get('alerta_vencimento_proximo', False):
                criterios.append({
                    'tipo': 'vencimento_proximo',
                    'intensidade': 1.0,
                    'descricao': f"Vence em {row['dias_ate_vencimento']} dias"
                })
            
            if row.get('total_cupons_restantes', 0) > 0 and row.get('dias_para_proximo_cupom', 999) < 60:
                criterios.append({
                    'tipo': 'cupom_proximo',
                    'intensidade': 0.5,
                    'descricao': f"Cupom de R$ {row.get('valor_proximo_cupom', 0):.2f} em {row.get('dias_para_proximo_cupom', 0)} dias"
                })
            
            if criterios:
                opportunity = {
                    'titulo': row['titulo'],
                    'data': row['data_referencia'],
                    'taxa_atual': row['taxa_venda'],
                    'preco_atual': row['preco_venda'],
                    'preco_teorico': row.get('preco_teorico'),
                    'criterios': criterios,
                    'score_geral': len(criterios) * 10 + sum(c['intensidade'] * 10 for c in criterios)
                }
                opportunities.append(opportunity)
        
        opportunities.sort(key=lambda x: x['score_geral'], reverse=True)
        self.logger.info(f"Detectadas {len(opportunities)} oportunidades")
        
        return opportunities