#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

from src.collectors.tesouro_scraper import TesouroScraper
from src.collectors.bcb_api import BCBCollector
from src.collectors.cupons_scraper import CuponsScraper
from src.processors.cleaner import DataCleaner
from src.processors.feature_engineering import FeatureEngineering
from src.processors.event_processor import EventProcessor
from src.analyzers.fair_price import FairPriceCalculator
from src.analyzers.opportunity_detector import OpportunityDetector
from src.alerts.alert_manager import AlertManager
from src.utils.logger import setup_logger

class TesouroPipeline:
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.tesouro_scraper = TesouroScraper()
        self.bcb_collector = BCBCollector()
        self.cupons_scraper = CuponsScraper()
        self.cleaner = DataCleaner()
        self.feature_eng = FeatureEngineering()
        self.event_processor = EventProcessor()
        self.fair_price = FairPriceCalculator()
        self.opportunity_detector = OpportunityDetector()
        self.alert_manager = AlertManager()
        
    def run_collect(self):
        self.logger.info("Iniciando etapa de coleta completa")
        
        tesouro_df = self.tesouro_scraper.fetch_data()
        selic_df = self.bcb_collector.fetch_selic()
        ipca_df = self.bcb_collector.fetch_ipca()
        cupons_df = self.cupons_scraper.fetch_cupons()
        vencimentos_df = self.cupons_scraper.fetch_vencimentos()
        
        return tesouro_df, selic_df, ipca_df, cupons_df, vencimentos_df
    
    def run_process(self, tesouro_df, selic_df, ipca_df, cupons_df, vencimentos_df):
        self.logger.info("Iniciando etapa de processamento estendida")
        
        tesouro_clean = self.cleaner.clean_tesouro_data(tesouro_df)
        
        if tesouro_clean is None:
            self.logger.error("Falha na limpeza dos dados")
            return None, None
        
        tesouro_features = self.feature_eng.calculate_metrics(
            tesouro_clean, selic_df, ipca_df
        )
        
        tesouro_com_cupons = self.event_processor.process_cupons(
            cupons_df, tesouro_features
        )
        
        tesouro_completo = self.event_processor.process_vencimentos(
            vencimentos_df, tesouro_com_cupons
        )
        
        self.logger.info(f"Processamento concluído: {len(tesouro_completo)} títulos processados")
        return tesouro_completo, []
    
    def run_analyze(self, df):
        self.logger.info("Iniciando etapa de análise")
        df_with_prices = self.fair_price.calculate_fair_price(df)
        opportunities = self.opportunity_detector.detect_opportunities(df_with_prices)
        return df_with_prices, opportunities
    
    def run_alerts(self, opportunities):
        self.logger.info("Iniciando etapa de alertas")
        if opportunities:
            self.alert_manager.send_alerts(opportunities)
        else:
            self.logger.info("Nenhuma oportunidade detectada")
    
    def run_full_pipeline(self):
        self.logger.info("="*60)
        self.logger.info("INICIANDO PIPELINE COMPLETO")
        self.logger.info("="*60)
        
        try:
            tesouro_df, selic_df, ipca_df, cupons_df, vencimentos_df = self.run_collect()
            
            if tesouro_df is None:
                self.logger.error("Falha na coleta de dados")
                return False
            
            tesouro_processed, novos_titulos = self.run_process(
                tesouro_df, selic_df, ipca_df, cupons_df, vencimentos_df
            )
            
            if tesouro_processed is None:
                return False
            
            df_analyzed, opportunities = self.run_analyze(tesouro_processed)
            self.run_alerts(opportunities)
            
            # Salvar resultados
            output_file = Path(__file__).parent.parent / 'data' / 'processed' / f'analise_{datetime.now().strftime("%Y%m%d")}.csv'
            df_analyzed.to_csv(output_file, index=False)
            self.logger.info(f"Resultados salvos em {output_file}")
            
            self.logger.info("="*60)
            self.logger.info("PIPELINE CONCLUÍDO COM SUCESSO")
            self.logger.info("="*60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro no pipeline: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

def main():
    parser = argparse.ArgumentParser(description='Tesouro Pipeline')
    parser.add_argument('--full', action='store_true', help='Executar pipeline completo')
    
    args = parser.parse_args()
    
    pipeline = TesouroPipeline()
    
    if args.full:
        pipeline.run_full_pipeline()
    else:
        pipeline.run_full_pipeline()

if __name__ == "__main__":
    main()