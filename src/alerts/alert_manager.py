import json
from datetime import datetime
from pathlib import Path
from ..utils.logger import setup_logger
from ..utils.config import ALERTS_DIR

class AlertManager:
    def __init__(self, email_config=None):
        self.logger = setup_logger(__name__)
        self.email_config = email_config or {}
        self.alerts_history = []
        self.history_file = ALERTS_DIR / f"alerts_{datetime.now().strftime('%Y%m')}.json"
        self._load_history()
    
    def _load_history(self):
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r') as f:
                    self.alerts_history = json.load(f)
            except:
                self.alerts_history = []
    
    def _save_history(self):
        with open(self.history_file, 'w') as f:
            json.dump(self.alerts_history[-100:], f, indent=2, default=str)
    
    def send_alerts(self, opportunities):
        if not opportunities:
            self.logger.info("Nenhuma oportunidade para alertar")
            return
        
        self._console_alert(opportunities)
        
        alert_record = {
            'timestamp': str(datetime.now()),
            'total_oportunidades': len(opportunities),
            'oportunidades': opportunities[:10]
        }
        self.alerts_history.append(alert_record)
        self._save_history()
    
    def _console_alert(self, opportunities):
        print("\n" + "="*80)
        print("🔔 OPORTUNIDADES DETECTADAS - TESOURO DIRETO")
        print("="*80)
        
        for i, opp in enumerate(opportunities[:5], 1):
            print(f"\n{i}. {opp['titulo']}")
            print(f"   📅 Data: {opp['data']}")
            print(f"   📊 Taxa: {opp['taxa_atual']:.2%}")
            print(f"   💰 Preço: R$ {opp['preco_atual']:.2f}")
            if opp.get('preco_teorico'):
                print(f"   🎯 Preço Teórico: R$ {opp['preco_teorico']:.2f}")
            
            print("   🔍 Critérios:")
            for criterio in opp['criterios']:
                print(f"      - {criterio['descricao']}")
        
        print("\n" + "="*80)