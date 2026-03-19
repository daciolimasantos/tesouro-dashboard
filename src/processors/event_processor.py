import pandas as pd
from datetime import datetime
from ..utils.logger import setup_logger

class EventProcessor:
    def __init__(self):
        self.logger = setup_logger(__name__)
        
    def process_cupons(self, cupons_df, titulos_df):
        if cupons_df is None or cupons_df.empty:
            self.logger.warning("Sem dados de cupons para processar")
            return titulos_df
        
        df_result = titulos_df.copy()
        
        # Inicializar colunas de cupons
        df_result['proximo_cupom'] = None
        df_result['valor_proximo_cupom'] = None
        df_result['dias_para_proximo_cupom'] = None
        df_result['total_cupons_restantes'] = 0
        
        hoje = datetime.now()
        
        for idx, row in df_result.iterrows():
            nome_titulo = str(row['titulo']).split(' ')[0] + ' ' + str(row['titulo']).split(' ')[1] if len(str(row['titulo']).split(' ')) > 1 else str(row['titulo'])
            
            cupons_titulo = cupons_df[
                cupons_df['titulo'].str.contains(nome_titulo, na=False, case=False)
            ].copy() if 'titulo' in cupons_df.columns else pd.DataFrame()
            
            if not cupons_titulo.empty and 'data_pagamento' in cupons_titulo.columns:
                cupons_titulo['data_pagamento'] = pd.to_datetime(cupons_titulo['data_pagamento'], errors='coerce')
                cupons_futuros = cupons_titulo[cupons_titulo['data_pagamento'] > hoje]
                
                if not cupons_futuros.empty:
                    prox_cupom = cupons_futuros.iloc[0]
                    df_result.at[idx, 'proximo_cupom'] = prox_cupom['data_pagamento']
                    df_result.at[idx, 'valor_proximo_cupom'] = prox_cupom.get('valor', 0)
                    df_result.at[idx, 'dias_para_proximo_cupom'] = (prox_cupom['data_pagamento'] - hoje).days
                    df_result.at[idx, 'total_cupons_restantes'] = len(cupons_futuros)
        
        return df_result
    
    def process_vencimentos(self, vencimentos_df, titulos_df):
        if vencimentos_df is None or vencimentos_df.empty:
            return titulos_df
        
        df_result = titulos_df.copy()
        
        hoje = datetime.now()
        
        # Adicionar colunas de alerta
        df_result['alerta_vencimento_proximo'] = (
            df_result['dias_ate_vencimento'] < 30
        ) & (df_result['dias_ate_vencimento'] > 0)
        
        df_result['titulo_vencido'] = df_result['dias_ate_vencimento'] <= 0
        
        # Verificar se a coluna oportunidade_score existe antes de usar
        mask_curto_prazo = (df_result['dias_ate_vencimento'] > 0) & (df_result['dias_ate_vencimento'] < 90)
        
        if 'oportunidade_score' in df_result.columns:
            df_result.loc[mask_curto_prazo, 'oportunidade_score'] = df_result.loc[mask_curto_prazo, 'oportunidade_score'].fillna(0) + 20
        else:
            # Criar a coluna se não existir
            df_result['oportunidade_score'] = 0
            df_result.loc[mask_curto_prazo, 'oportunidade_score'] = 20
        
        return df_result