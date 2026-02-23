import streamlit as st
from supabase import create_client
from datetime import datetime
import pytz
import pandas as pd

# 1. Configurações do Supabase
SUPABASE_URL = "https://ynurfeprihookyehurbn.supabase.co"
SUPABASE_KEY = "sb_publishable_nOGOgL8109xmBQaieslQ3w_BIhDD5va"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="Gestão Reserva", layout="wide")

# Criando as Abas
tab_bipe, tab_base = st.tabs(["🎯 Registrar Bipe", "📊 Base de Dados"])

# --- ABA 1: REGISTRO ---
with tab_bipe:
    st.title("📦 Recebimento Reserva")
    
    def registrar_bipe():
        # Toda esta parte agora está corretamente indentada (4 espaços para dentro)
        chave = st.session_state.barcode_input.strip() 
        
        if chave:
            try:
                fuso_br = pytz.timezone('America/Sao_Paulo')
                agora_br = datetime.now(fuso_br).replace(tzinfo=None).isoformat()

                dados = {"chave_nfe": chave, "data_chegada": agora_br}
                supabase.table("conferencia_reserva").insert(dados).execute()
                
                # Mensagem de Sucesso
                st.success(f"✅ Volume registrado com sucesso! (Chave: {chave})")
                
            except Exception as e:
                erro_str = str(e).lower()
                # Captura o erro de duplicidade que configuramos no banco
                if "23505" in erro_str or "duplicate key" in erro_str:
                    st.warning(f"⚠️ **Atenção:** O volume {chave} já foi bipado anteriormente!")
                else:
                    st.error(f"❌ Erro inesperado ao salvar: {e}")
            
            # Limpa o campo de input para o próximo bipe
            st.session_state.barcode_input = ""

    # Campo de entrada de dados
    st.text_input("Bipe a chave aqui:", key="barcode_input", on_change=registrar_bipe)
    
    st.info("💡 O cursor deve estar no campo acima para o leitor funcionar.")

# --- ABA 2: BASE DE DADOS ---
with tab_base:
    st.title("📋 Histórico de Inserções")
    
    if st.button("🔄 Atualizar Dados"):
        try:
            # Busca registros ordenando pelo mais recente
            response = supabase.table("conferencia_reserva").select("*").order("data_chegada", desc=True).execute()
            
            if response.data:
                df = pd.DataFrame(response.data)
                
                # Formata a data para o padrão brasileiro no display
                df['data_chegada'] = pd.to_datetime(df['data_chegada'], format='mixed').dt.strftime('%d/%m/%Y %H:%M:%S')
                
                # Exibe métrica de total
                st.metric("Total de Volumes na Base", len(df))
                
                # Tabela de dados
                st.dataframe(df, use_container_width=True)
                
                # Exportação
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Baixar Planilha (CSV)", csv, "base_reserva.csv", "text/csv")
            else:
                st.info("Nenhum registro encontrado na base.")
                
        except Exception as e:
            st.error(f"Erro ao carregar base: {e}")