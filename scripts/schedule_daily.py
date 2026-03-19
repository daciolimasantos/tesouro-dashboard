#!/usr/bin/env python3
import schedule
import time
from datetime import datetime
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from scripts.run_pipeline import TesouroPipeline

def run_daily_pipeline():
    print(f"\n[{datetime.now()}] Iniciando execução diária do pipeline")
    
    pipeline = TesouroPipeline()
    success = pipeline.run_full_pipeline()
    
    if success:
        print(f"[{datetime.now()}] Pipeline executado com sucesso")
    else:
        print(f"[{datetime.now()}] Falha na execução do pipeline")

def main():
    print("Iniciando agendador do Tesouro Pipeline")
    
    schedule.every().day.at("18:00").do(run_daily_pipeline)
    
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        run_daily_pipeline()
    
    print("Aguardando horário agendado...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()