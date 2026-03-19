import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import time
import os
import sys
from pathlib import Path

# Adicionar src ao path para importar o pipeline
sys.path.append(str(Path(__file__).parent))

from src.collectors.tesouro_scraper import TesouroScraper
from src.collectors.bcb_api import BCBCollector
from src.collectors.cupons_scraper import CuponsScraper
from src.processors.cleaner import DataCleaner
from src.processors.feature_engineering import FeatureEngineering
from src.processors.event_processor import EventProcessor
from src.analyzers.fair_price import FairPriceCalculator
from src.analyzers.opportunity_detector import OpportunityDetector

# Configuração da página
st.set_page_config(
    page_title="Tesouro Direto - Monitor de Oportunidades",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
    }
    .opportunity-card {
        background-color: #F0F9FF;
        border-left: 5px solid #2563EB;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
    }
    .alert-high {
        color: #DC2626;
        font-weight: bold;
    }
    .alert-medium {
        color: #F59E0B;
        font-weight: bold;
    }
    .alert-low {
        color: #10B981;
        font-weight: bold;
    }
    .footer {
        text-align: center;
        margin-top: 3rem;
        color: #6B7280;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

# Título principal
st.markdown('<h1 class="main-header">📈 Tesouro Direto - Monitor de Oportunidades 24/7</h1>', unsafe_allow_html=True)

# Sidebar para configurações
with st.sidebar:
    st.image("https://www.tesourodireto.com.br/titulo/public/img/logo-tesouro-direto.png", width=200)
    st.markdown("## ⚙️ Configurações")
    
    # Opção de atualização
    auto_refresh = st.checkbox("Atualização automática (a cada hora)", value=True)
    if auto_refresh:
        st.info("🔄 O dashboard será atualizado automaticamente a cada 60 minutos")
    
    # Filtros
    st.markdown("## 🔍 Filtros")
    desconto_minimo = st.slider(
        "Desconto mínimo (%)", 
        min_value=0.0, 
        max_value=5.0, 
        value=1.0, 
        step=0.5
    )
    
    mostrar_vencidos = st.checkbox("Mostrar títulos vencidos", value=False)
    
    # Botão de atualização manual
    if st.button("🔄 Atualizar Agora", type="primary"):
        st.session_state['ultima_atualizacao'] = datetime.now()
        st.rerun()
    
    st.markdown("---")
    st.markdown("### 📚 Ajuda Rápida")
    with st.expander("🎯 Entendendo as cores"):
        st.markdown("""
        - 🔴 **ALTO**: Oportunidade forte
        - 🟡 **MÉDIO**: Oportunidade moderada
        - 🟢 **BAIXO**: Oportunidade leve
        """)
    
    with st.expander("📊 Glossário"):
        st.markdown("""
        - **Taxa**: Rentabilidade anual
        - **PU**: Preço Unitário
        - **Vencimento**: Data de resgate
        - **Cupom**: Pagamento de juros
        - **Duration**: Prazo médio ponderado
        """)
    
    st.markdown("---")
    st.markdown("**🔗 Links úteis:**")
    st.markdown("[Tesouro Direto Oficial](https://www.tesourodireto.com.br)")
    st.markdown("[Banco Central](https://www.bcb.gov.br)")
    st.markdown("[Código Fonte](https://github.com/daciolimasantos/tesouro-dashboard)")

# Função para carregar dados do pipeline
@st.cache_data(ttl=3600)  # Cache de 1 hora
def load_data():
    """Executa o pipeline e retorna dados processados"""
    
    # Inicializar componentes
    tesouro_scraper = TesouroScraper()
    bcb_collector = BCBCollector()
    cupons_scraper = CuponsScraper()
    cleaner = DataCleaner()
    feature_eng = FeatureEngineering()
    event_processor = EventProcessor()
    fair_price = FairPriceCalculator()
    opportunity_detector = OpportunityDetector()
    
    # Coletar dados
    tesouro_df = tesouro_scraper.fetch_data()
    selic_df = bcb_collector.fetch_selic()
    ipca_df = bcb_collector.fetch_ipca()
    cupons_df = cupons_scraper.fetch_cupons()
    vencimentos_df = cupons_scraper.fetch_vencimentos()
    
    if tesouro_df is None:
        return None
    
    # Processar
    tesouro_clean = cleaner.clean_tesouro_data(tesouro_df)
    tesouro_features = feature_eng.calculate_metrics(tesouro_clean, selic_df, ipca_df)
    tesouro_com_cupons = event_processor.process_cupons(cupons_df, tesouro_features)
    tesouro_completo = event_processor.process_vencimentos(vencimentos_df, tesouro_com_cupons)
    
    # Analisar
    df_analyzed = fair_price.calculate_fair_price(tesouro_completo)
    opportunities = opportunity_detector.detect_opportunities(df_analyzed)
    
    return {
        'dados': df_analyzed,
        'oportunidades': opportunities,
        'timestamp': datetime.now()
    }

# Carregar dados
with st.spinner('🔄 Coletando dados do Tesouro Direto...'):
    data = load_data()

if data is None:
    st.error("❌ Erro ao carregar dados. Tentando novamente em alguns instantes...")
    time.sleep(5)
    st.rerun()

df = data['dados']
oportunidades = data['oportunidades']
ultima_atualizacao = data['timestamp']

# Header com timestamp
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f"<div class='metric-card'>📅 Última atualização<br><b>{ultima_atualizacao.strftime('%d/%m/%Y %H:%M')}</b></div>", unsafe_allow_html=True)
with col2:
    st.markdown(f"<div class='metric-card'>📊 Total de títulos<br><b>{len(df)}</b></div>", unsafe_allow_html=True)
with col3:
    st.markdown(f"<div class='metric-card'>🎯 Oportunidades detectadas<br><b>{len(oportunidades)}</b></div>", unsafe_allow_html=True)

# Abas
tab1, tab2, tab3, tab4, tab5 = st.tabs(["🎯 Oportunidades", "📊 Todos os Títulos", "📈 Curva de Juros", "📅 Calendário", "📖 Manual"])

# TAB 1: Oportunidades
with tab1:
    st.markdown("## 🎯 Oportunidades de Compra Detectadas")
    
    if not oportunidades:
        st.info("✅ Nenhuma oportunidade detectada no momento")
    else:
        # Filtrar por desconto mínimo
        oportunidades_filtradas = [
            opp for opp in oportunidades 
            if any(c['tipo'] == 'desconto' and c['intensidade']*100 >= desconto_minimo for c in opp['criterios'])
        ]
        
        for opp in oportunidades_filtradas:
            # Determinar classe de alerta baseada no score
            if opp['score_geral'] > 50:
                alert_class = "alert-high"
                nivel = "🔴 ALTO"
            elif opp['score_geral'] > 30:
                alert_class = "alert-medium"
                nivel = "🟡 MÉDIO"
            else:
                alert_class = "alert-low"
                nivel = "🟢 BAIXO"
            
            with st.container():
                st.markdown(f"""
                <div class='opportunity-card'>
                    <h3>{opp['titulo']} <span class='{alert_class}'>({nivel})</span></h3>
                    <p><b>📅 Data:</b> {opp['data'].strftime('%d/%m/%Y') if hasattr(opp['data'], 'strftime') else opp['data']}</p>
                    <p><b>📊 Taxa atual:</b> {opp['taxa_atual']:.2%}</p>
                    <p><b>💰 Preço atual:</b> R$ {opp['preco_atual']:.2f}</p>
                """, unsafe_allow_html=True)
                
                if opp.get('preco_teorico'):
                    st.markdown(f"<p><b>🎯 Preço teórico:</b> R$ {opp['preco_teorico']:.2f}</p>", unsafe_allow_html=True)
                
                st.markdown("<p><b>🔍 Critérios:</b></p>", unsafe_allow_html=True)
                for criterio in opp['criterios']:
                    st.markdown(f"<p>   • {criterio['descricao']}</p>", unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)

# TAB 2: Todos os Títulos
with tab2:
    st.markdown("## 📊 Todos os Títulos Disponíveis")
    
    # Filtros na tabela
    col1, col2 = st.columns(2)
    with col1:
        tipo_filtro = st.multiselect(
            "Filtrar por tipo",
            options=df['categoria'].unique(),
            default=df['categoria'].unique()
        )
    with col2:
        ordenar_por = st.selectbox(
            "Ordenar por",
            options=['taxa_venda', 'desconto', 'dias_ate_vencimento', 'oportunidade_score'],
            format_func=lambda x: {
                'taxa_venda': 'Taxa (%)',
                'desconto': 'Desconto (%)',
                'dias_ate_vencimento': 'Dias até vencimento',
                'oportunidade_score': 'Score de oportunidade'
            }[x]
        )
    
    # Aplicar filtros
    df_filtrado = df[df['categoria'].isin(tipo_filtro)]
    
    if not mostrar_vencidos:
        df_filtrado = df_filtrado[df_filtrado['dias_ate_vencimento'] > 0]
    
    # Preparar dados para exibição
    df_display = df_filtrado[[
        'titulo', 'categoria', 'taxa_venda', 'preco_venda', 
        'preco_teorico', 'desconto', 'dias_ate_vencimento', 
        'oportunidade_score', 'alerta_vencimento_proximo'
    ]].copy()
    
    # Formatar colunas
    df_display['taxa_venda'] = df_display['taxa_venda'].apply(lambda x: f"{x:.2%}")
    df_display['preco_venda'] = df_display['preco_venda'].apply(lambda x: f"R$ {x:.2f}")
    df_display['preco_teorico'] = df_display['preco_teorico'].apply(lambda x: f"R$ {x:.2f}" if pd.notna(x) else "-")
    df_display['desconto'] = df_display['desconto'].apply(lambda x: f"{x:.2%}" if pd.notna(x) else "-")
    df_display['oportunidade_score'] = df_display['oportunidade_score'].apply(lambda x: f"{x:.1f}")
    df_display['alerta_vencimento_proximo'] = df_display['alerta_vencimento_proximo'].apply(lambda x: "⚠️ Sim" if x else "✅ Não")
    
    # Renomear colunas
    df_display.columns = [
        'Título', 'Categoria', 'Taxa', 'Preço', 'Preço Teórico',
        'Desconto', 'Dias até Venc.', 'Score', 'Vencimento Próximo'
    ]
    
    # Ordenar
    if ordenar_por == 'taxa_venda':
        df_display = df_display.sort_values('Taxa', ascending=False)
    elif ordenar_por == 'desconto':
        df_display = df_display.sort_values('Desconto', ascending=True)
    elif ordenar_por == 'dias_ate_vencimento':
        df_display = df_display.sort_values('Dias até Venc.', ascending=True)
    else:
        df_display = df_display.sort_values('Score', ascending=False)
    
    st.dataframe(
        df_display,
        width='stretch',
        hide_index=True,
        column_config={
            "Score": st.column_config.NumberColumn(format="%.1f")
        }
    )
    
    # Gráfico de taxas
    st.markdown("### 📈 Taxas por Vencimento")
    fig = px.scatter(
        df_filtrado,
        x='dias_ate_vencimento',
        y='taxa_venda',
        color='categoria',
        hover_data=['titulo'],
        labels={
            'dias_ate_vencimento': 'Dias até Vencimento',
            'taxa_venda': 'Taxa (%)',
            'categoria': 'Categoria'
        }
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, width='stretch')

# TAB 3: Curva de Juros
with tab3:
    st.markdown("## 📈 Curva de Juros - Estrutura a Termo")
    
    # Agrupar por prazo
    curva = df.groupby('dias_ate_vencimento').agg({
        'taxa_venda': ['mean', 'min', 'max']
    }).reset_index()
    curva.columns = ['dias', 'taxa_media', 'taxa_min', 'taxa_max']
    curva = curva.sort_values('dias')
    
    # Criar gráfico interativo
    fig = go.Figure()
    
    # Área de mínimo/máximo
    fig.add_trace(go.Scatter(
        x=curva['dias'],
        y=curva['taxa_max'],
        mode='lines',
        line=dict(width=0),
        showlegend=False,
        name='Máximo'
    ))
    
    fig.add_trace(go.Scatter(
        x=curva['dias'],
        y=curva['taxa_min'],
        mode='lines',
        line=dict(width=0),
        fill='tonexty',
        fillcolor='rgba(37, 99, 235, 0.1)',
        showlegend=False,
        name='Mínimo'
    ))
    
    # Linha média
    fig.add_trace(go.Scatter(
        x=curva['dias'],
        y=curva['taxa_media'],
        mode='lines+markers',
        name='Taxa Média',
        line=dict(color='#2563EB', width=3),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title='Curva de Juros por Prazo',
        xaxis_title='Dias até Vencimento',
        yaxis_title='Taxa (%)',
        height=600,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, width='stretch')
    
    # Métricas da curva
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            "Taxa Curto Prazo (até 365 dias)",
            f"{curva[curva['dias'] <= 365]['taxa_media'].mean():.2%}" if len(curva[curva['dias'] <= 365]) > 0 else "N/A"
        )
    with col2:
        st.metric(
            "Taxa Médio Prazo (1-5 anos)",
            f"{curva[(curva['dias'] > 365) & (curva['dias'] <= 1825)]['taxa_media'].mean():.2%}" if len(curva[(curva['dias'] > 365) & (curva['dias'] <= 1825)]) > 0 else "N/A"
        )
    with col3:
        st.metric(
            "Taxa Longo Prazo (5+ anos)",
            f"{curva[curva['dias'] > 1825]['taxa_media'].mean():.2%}" if len(curva[curva['dias'] > 1825]) > 0 else "N/A"
        )

# TAB 4: Calendário
with tab4:
    st.markdown("## 📅 Calendário de Eventos")
    
    # Criar dataframe de eventos
    eventos = []
    
    # Eventos de vencimento
    for _, row in df.iterrows():
        if row['dias_ate_vencimento'] > 0 and row['dias_ate_vencimento'] < 365:
            eventos.append({
                'data': row['vencimento'],
                'titulo': row['titulo'],
                'tipo': 'Vencimento',
                'detalhe': f"Vence em {row['dias_ate_vencimento']} dias"
            })
    
    # Eventos de cupom
    if 'proximo_cupom' in df.columns:
        for _, row in df.iterrows():
            if pd.notna(row.get('proximo_cupom')):
                eventos.append({
                    'data': row['proximo_cupom'],
                    'titulo': row['titulo'],
                    'tipo': 'Cupom',
                    'detalhe': f"Valor: R$ {row.get('valor_proximo_cupom', 0):.2f}"
                })
    
    if eventos:
        df_eventos = pd.DataFrame(eventos)
        df_eventos = df_eventos.sort_values('data')
        
        # Timeline
        fig = px.timeline(
            df_eventos,
            x_start='data',
            x_end='data',
            y='titulo',
            color='tipo',
            hover_data=['detalhe'],
            title='Próximos Eventos'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, width='stretch')
        
        # Tabela de eventos
        st.dataframe(
            df_eventos,
            width='stretch',
            hide_index=True,
            column_config={
                "data": "Data",
                "titulo": "Título",
                "tipo": "Tipo",
                "detalhe": "Detalhes"
            }
        )
    else:
        st.info("Nenhum evento próximo detectado")

# TAB 5: Manual
with tab5:
    st.markdown("## 📖 Manual do Usuário - Como Usar o Dashboard")
    
    with st.expander("🎯 **O que é este dashboard?**", expanded=True):
        st.markdown("""
        Este é um **monitor automatizado do Tesouro Direto** que:
        - 📊 Coleta dados de todos os títulos públicos disponíveis
        - 🔍 Identifica oportunidades de compra quando os títulos estão com desconto
        - 📈 Analisa a curva de juros e tendências de mercado
        - ⏰ Funciona 24/7 e atualiza automaticamente a cada hora
        """)
    
    with st.expander("🔍 **Como as oportunidades são detectadas?**"):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            **Critérios de oportunidade:**
            
            1. **Desconto no preço** 📉
               - Quando o preço atual está abaixo do preço teórico
               - Quanto maior o desconto, melhor a oportunidade
               - Alerta a partir de 1% de desconto (configurável)
            
            2. **Taxa acima da média** 📊
               - Comparação com a média histórica do título
               - Indica momento favorável para compra
            
            3. **Vencimento próximo** ⏳
               - Títulos com menos de 90 dias para vencer
               - Podem oferecer liquidez rápida
            
            4. **Cupons futuros** 💰
               - Títulos que pagarão cupons em breve
               - Gera renda periódica para o investidor
            """)
        
        with col2:
            st.markdown("""
            **Níveis de oportunidade:**
            
            - 🔴 **ALTO** (score > 50): Oportunidade forte, múltiplos critérios atendidos
            - 🟡 **MÉDIO** (score 30-50): Oportunidade moderada, vale analisar
            - 🟢 **BAIXO** (score < 30): Oportunidade leve, observe o mercado
            
            *Quanto maior o score, melhor a oportunidade!*
            """)
    
    with st.expander("📊 **Entendendo os dados**"):
        st.markdown("""
        | Campo | Significado | Como usar |
        |-------|-------------|-----------|
        | **Taxa (%)** | Rentabilidade anual do título | Compare com outros investimentos |
        | **Preço atual** | Valor de compra hoje | Quanto você pagaria agora |
        | **Preço teórico** | Valor justo calculado | Base para saber se está barato/caro |
        | **Desconto** | Diferença % entre preço atual e teórico | Negativo = barato / Positivo = caro |
        | **Dias até venc.** | Tempo restante até o vencimento | Impacta na liquidez e risco |
        | **Score** | Pontuação da oportunidade (0-100) | Quanto maior, melhor |
        | **Venc. próximo** | Alerta se vence em < 30 dias | ⚠️ Atenção para reinvestir |
        """)
    
    with st.expander("📈 **Interpretando a Curva de Juros**"):
        st.markdown("""
        A curva de juros mostra a relação entre **prazo** e **taxa** dos títulos:
        Taxa (%)
↑
│ ●─────────● Curva normal
│ ● ● (quanto maior o prazo,
│ ● ● maior a taxa)
│ ● ●
│ ● ●
│ ● ●
└─────────────────────────→ Prazo (dias)


**O que observar:**
- 📈 **Curva inclinada para cima**: Normal, mercado saudável
- 📉 **Curva invertida**: Pode indicar expectativa de queda de juros
- 📊 **Achatamento**: Incerteza no mercado

**Áreas do gráfico:**
- Área azul clara = faixa entre mínima e máxima
- Linha azul escura = taxa média
- Pontos = títulos individuais
""")

    with st.expander("📅 **Usando o Calendário de Eventos**"):
        st.markdown("""
    O calendário mostra dois tipos de eventos:

    1. **Vencimentos** 📅
    - Data em que o título será resgatado
    - Você receberá o valor nominal
    - Planeje o reinvestimento com antecedência

    2. **Cupons** 💵
    - Pagamentos semestrais de juros
    - Ocorrem apenas em títulos com "Juros" no nome
    - Valor creditado na sua conta

    **Como usar:**
    - Acompanhe os próximos 12 meses
    - Planeje seu fluxo de caixa
    - Identifique períodos com mais eventos
    """)

    with st.expander("⚙️ **Configurações e Filtros**"):
    st.markdown("""
    **Na barra lateral você pode:**

    1. **🔄 Atualização automática**
    - Mantenha ativado para dados sempre frescos
    - Atualiza a cada 60 minutos

    2. **🔍 Filtros**
    - **Desconto mínimo**: Só mostra oportunidades acima deste valor
    - **Mostrar vencidos**: Inclui títulos já vencidos na tabela

    3. **🔄 Atualizar agora**
    - Botão para forçar atualização manual

    4. **📊 Ordenação**
    - Na tabela, clique nas colunas para ordenar
    - Use o seletor "Ordenar por" para classificação automática
    """)

    with st.expander("❓ **Perguntas Frequentes**"):

    faq_data = [
        ("**Os dados são reais?**", 
        "O dashboard tenta coletar dados das APIs oficiais do Tesouro Direto e Banco Central. Se as APIs estiverem indisponíveis, usamos dados simulados realistas para manter o funcionamento."),
        
        ("**Com que frequência os dados são atualizados?**", 
        "A cada 60 minutos automaticamente. Você também pode clicar em 'Atualizar Agora' na barra lateral."),
        
        ("**Posso confiar nas oportunidades detectadas?**", 
        "As oportunidades são baseadas em modelos matemáticos e análise de dados. Use como **ferramenta de apoio**, não como recomendação de investimento. Sempre consulte um assessor financeiro."),
        
        ("**O que é 'preço teórico'?**", 
        "É o preço que o título 'deveria' ter baseado em modelos de fluxo de caixa descontado. Considera taxa atual, tempo até vencimento e, quando aplicável, cupons futuros."),
        
        ("**Por que alguns títulos têm 'N/A' no preço teórico?**", 
        "Pode ocorrer para títulos muito atípicos ou quando há dados insuficientes para o cálculo. Nestes casos, focamos apenas nos dados observados de mercado."),
        
        ("**O dashboard funciona no celular?**", 
        "Sim! O Streamlit é responsivo e se adapta a telas menores. Acesse pelo navegador do seu smartphone."),
        
        ("**É gratuito mesmo?**", 
        "Sim! Este é um projeto open-source mantido pela comunidade. O código está disponível no GitHub para quem quiser contribuir ou customizar.")
    ]

    for pergunta, resposta in faq_data:
        with st.container():
            st.markdown(f"**{pergunta}**")
            st.markdown(f"{resposta}")
            st.markdown("---")

    with st.expander("🚀 **Dicas Avançadas**"):
    st.markdown("""
    ### Para usuários mais experientes:

    1. **Compare com outros indicadores** 📊
    - Observe a relação entre taxas prefixadas e IPCA+
    - A diferença indica a expectativa de inflação do mercado

    2. **Acompanhe a inclinação da curva** 📈
    - Curva muito inclinada: mercado espera juros mais altos no futuro
    - Curva invertida: possível recessão à frente

    3. **Estratégias comuns** 💡
    - **Buy & Hold**: Compre títulos com boas taxas e leve até o vencimento
    - **Trading**: Aproveite oportunidades de curto prazo (descontos > 2%)
    - **Escada de vencimentos**: Diversifique prazos para liquidez periódica

    4. **Monitore títulos com cupom** 💰
    - IPCA+ com Juros Semestrais pagam a cada 6 meses
    - Ideal para quem precisa de renda periódica
    """)

    with st.expander("📱 **Compartilhar e Feedback**"):
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        **Compartilhe este dashboard:** https://tesouro-dashboard.streamlit.app
        
        
    Ou use os botões:
    """)

    # Botões de compartilhamento
    st.markdown("""
    <a href="https://twitter.com/intent/tweet?text=Confira%20este%20dashboard%20do%20Tesouro%20Direto%20com%20oportunidades%20automáticas!&url=https://tesouro-dashboard.streamlit.app" target="_blank">
        <button style="background-color:#1DA1F2; color:white; padding:10px 20px; border:none; border-radius:5px; margin-right:10px;">🐦 Twitter</button>
    </a>
    <a href="https://www.linkedin.com/sharing/share-offsite/?url=https://tesouro-dashboard.streamlit.app" target="_blank">
        <button style="background-color:#0077B5; color:white; padding:10px 20px; border:none; border-radius:5px;">💼 LinkedIn</button>
    </a>
    """, unsafe_allow_html=True)

    with col2:
    st.markdown("""
    **Tem sugestões? Encontrou um bug?**

    Contribua no GitHub:
    [github.com/daciolimasantos/tesouro-dashboard](https://github.com/daciolimasantos/tesouro-dashboard)

    Ou abra uma issue diretamente:
    """)
    st.link_button("🐛 Reportar problema", "https://github.com/daciolimasantos/tesouro-dashboard/issues/new")

    with st.expander("⚠️ **Aviso Legal**"):
    st.warning("""
    **IMPORTANTE**: Este dashboard é uma ferramenta de **análise e informação**, não constituindo recomendação de investimento.

    - Os dados podem sofrer atrasos ou inconsistências
    - Decisões de investimento devem ser baseadas em sua própria análise
    - Consulte um profissional qualificado antes de investir
    - Rentabilidade passada não garante resultados futuros
    """)

    # Estatísticas de uso
    st.markdown("---")
    st.markdown("### 📊 Estatísticas do Dashboard")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
    st.metric("Versão", "2.0.0")
    with col2:
    st.metric("Última atualização", datetime.now().strftime("%d/%m/%Y"))
    with col3:
    st.metric("Títulos monitorados", len(df) if 'df' in locals() else 18)
    with col4:
    st.metric("APIs ativas", "BCB ✅ | Tesouro ⚠️")

# Rodapé
st.markdown("---")
st.markdown("""
<div class='footer'>
<p>🔄 Atualizado automaticamente a cada 1 hora • Dados fornecidos pelo Tesouro Direto e Banco Central</p>
<p>⚠️ Este é um projeto de código aberto. Não constitui recomendação de investimento.</p>
</div>
""", unsafe_allow_html=True)

# Atualização automática
if auto_refresh:
time_since_update = (datetime.now() - ultima_atualizacao).total_seconds() / 60
if time_since_update >= 60:
st.cache_data.clear()
st.rerun()