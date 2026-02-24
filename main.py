import streamlit as st
from supabase import create_client
from datetime import datetime
import pytz
import pandas as pd
import base64
import os

# --- 1. CONFIGURAÇÕES E CONEXÃO ---
SUPABASE_URL = "https://ynurfeprihookyehurbn.supabase.co"
SUPABASE_KEY = "sb_publishable_nOGOgL8109xmBQaieslQ3w_BIhDD5va"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="Gestão Reserva - AZZAS", layout="wide")

# --- 2. FUNÇÕES DE SUPORTE E ESTILO ---

def get_now_br():
    """Retorna o horário atual de Brasília limpo."""
    fuso = pytz.timezone('America/Sao_Paulo')
    return datetime.now(fuso).replace(tzinfo=None).isoformat()

def get_base64_of_bin_file(bin_file):
    if os.path.exists(bin_file):
        with open(bin_file, 'rb') as f:
            data = f.read()
        return base64.b64encode(data).decode()
    return ""

def imprimir_romaneio_html(id_romaneio, df_volumes, usuario):
    """Gera o componente HTML/JS para disparar a impressão profissional."""
    html_print = f"""
    <div id="printarea" style="font-family: sans-serif; padding: 20px;">
        <h2 style="text-align: center; border-bottom: 2px solid #000;">ROMANEIO DE EXPEDIÇÃO - AZZAS</h2>
        <p><strong>Nº Romaneio:</strong> {id_romaneio} | <strong>Origem:</strong> CD Reserva</p>
        <p><strong>Usuário Responsável:</strong> {usuario}</p>
        <p><strong>Data de Emissão:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
        <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
            <thead>
                <tr style="background: #eee;">
                    <th style="border: 1px solid #000; padding: 8px; text-align: left;">Chave NFe (Volumes)</th>
                </tr>
            </thead>
            <tbody>
                {"".join([f"<tr><td style='border: 1px solid #000; padding: 8px;'>{r['chave_nfe']}</td></tr>" for _, r in df_volumes.iterrows()])}
            </tbody>
        </table>
        <div style="margin-top: 60px; text-align: center;">
            <p>___________________________________________________</p>
            <p>Assinatura Responsável</p>
        </div>
    </div>
    <script>
        var content = document.getElementById('printarea').innerHTML;
        var win = window.open('', '', 'height=700,width=900');
        win.document.write('<html><head><title>Imprimir Romaneio</title></head><body>' + content + '</body></html>');
        win.document.close();
        setTimeout(function(){{ win.print(); win.close(); }}, 500);
    </script>
    """
    return st.components.v1.html(html_print, height=0)

def show_login():
    bg_img = get_base64_of_bin_file("Fundo tela login.png")
    st.markdown(f"""
        <style>
        .stApp {{ background-image: url("data:image/png;base64,{bg_img}"); background-size: cover; background-position: center; }}
        .brand-container {{ text-align: center; color: black; margin-top: 50px; }}
        .brand-title {{ font-size: 50px; font-weight: 300; letter-spacing: 12px; }}
        div[data-testid="stForm"] {{ background-color: rgba(255, 255, 255, 0.9); border-radius: 20px; padding: 40px; }}
        div[data-testid="stTextInput"] input {{ height: 55px; font-size: 18px; }}
        div.stButton > button {{ width: 100%; height: 55px; background-color: #000 !important; color: #fff !important; font-weight: bold; border-radius: 10px; }}
        </style>
    """, unsafe_allow_html=True)

    col_esq, col_meio, col_dir = st.columns([1, 1.4, 1])
    with col_meio:
        st.markdown('<div class="brand-container"><div class="brand-title">AZZAS</div><p style="letter-spacing:5px; font-weight:bold;">FASHION & LIFESTYLE</p></div>', unsafe_allow_html=True)
        with st.form("login"):
            email = st.text_input("E-mail", placeholder="seu@email.com").strip().lower()
            unidade = st.selectbox("Unidade", ["Selecione o CD", "CD Reserva", "CD Pavuna"])
            if st.form_submit_button("ENTRAR  →"):
                if email and unidade != "Selecione o CD":
                    st.session_state["auth"] = True
                    st.session_state["user_email"] = email
                    st.session_state["unidade"] = unidade
                    st.rerun()
                else: st.error("Preencha todos os campos.")
    st.stop()

# --- 3. EXECUÇÃO DO APP ---

if "auth" not in st.session_state:
    show_login()
else:
    # Sidebar
    st.sidebar.title(f"🏢 {st.session_state['unidade']}")
    st.sidebar.write(f"👤 {st.session_state['user_email']}")
    if st.sidebar.button("Sair"):
        st.session_state.clear()
        st.rerun()

    tab_op, tab_base = st.tabs(["🎯 Operação", "📊 Base de Dados"])

    with tab_op:
        # --- CD RESERVA ---
        if st.session_state['unidade'] == "CD Reserva":
            st.title("🚛 Expedição CD RESERVA")
            if "romaneio_id" not in st.session_state:
                if st.button("🚀 ABRIR NOVO ROMANEIO"):
                    res = supabase.table("romaneios").insert({
                        "usuario_criou": st.session_state['user_email'], 
                        "unidade_origem": "CD Reserva", "status": "Aberto"
                    }).execute()
                    st.session_state["romaneio_id"] = res.data[0]['id']
                    st.rerun()
            else:
                id_atual = st.session_state["romaneio_id"]
                st.info(f"📦 Romaneio Ativo: **#{id_atual}**")
                
                def reg_reserva():
                    chave = st.session_state.input_reserva.strip()
                    if chave:
                        try:
                            supabase.table("conferencia_reserva").insert({
                                "chave_nfe": chave, "romaneio_id": id_atual, "data_expedicao": get_now_br()
                            }).execute()
                            st.toast(f"✅ Bipado: {chave[-10:]}")
                        except Exception as e: st.error(f"Erro: {e}")
                        st.session_state.input_reserva = ""
                
                st.text_input("Bipe os volumes:", key="input_reserva", on_change=reg_reserva)
                
                if st.button("🏁 ENCERRAR ROMANEIO"):
                    supabase.table("romaneios").update({
                        "status": "Encerrado", "data_encerramento": get_now_br()
                    }).eq("id", id_atual).execute()
                    st.session_state["resumo_pronto"] = id_atual
                    del st.session_state["romaneio_id"]
                    st.rerun()

            if "resumo_pronto" in st.session_state:
                id_resumo = st.session_state["resumo_pronto"]
                bipes = supabase.table("conferencia_reserva").select("chave_nfe").eq("romaneio_id", id_resumo).execute()
                with st.container(border=True):
                    st.subheader(f"📄 Resumo Romaneio #{id_resumo}")
                    df_res = pd.DataFrame(bipes.data) if bipes.data else pd.DataFrame()
                    if not df_res.empty:
                        st.table(df_res)
                        if st.button("🖨️ Imprimir Agora"):
                            imprimir_romaneio_html(id_resumo, df_res, st.session_state['user_email'])
                    if st.button("➕ Iniciar Novo"):
                        del st.session_state["resumo_pronto"]
                        st.rerun()

        # --- CD PAVUNA ---
        elif st.session_state['unidade'] == "CD Pavuna":
            st.title("📥 Recebimento CD PAVUNA")
            if "romaneio_pavuna" not in st.session_state:
                id_input = st.text_input("Digite o Nº do Romaneio:")
                if st.button("🔍 Abrir Romaneio"):
                    check = supabase.table("romaneios").select("*").eq("id", id_input).eq("status", "Encerrado").execute()
                    if check.data:
                        st.session_state["romaneio_pavuna"] = id_input
                        st.session_state["conferidos_agora"] = []
                        st.rerun()
                    else: st.error("❌ Romaneio inválido ou aberto.")
            else:
                rom_id = st.session_state["romaneio_pavuna"]
                st.info(f"✅ Conferindo Romaneio: **#{rom_id}**")
                res_envio = supabase.table("conferencia_reserva").select("chave_nfe").eq("romaneio_id", rom_id).execute()
                lista_esperada = [item['chave_nfe'] for item in res_envio.data]
                def reg_pavuna():
                    chave = st.session_state.input_pavuna.strip()
                    if chave:
                        if chave in lista_esperada:
                            if chave not in st.session_state["conferidos_agora"]:
                                supabase.table("conferencia_reserva").update({"data_recebimento": get_now_br()}).eq("chave_nfe", chave).eq("romaneio_id", rom_id).execute()
                                st.session_state["conferidos_agora"].append(chave)
                                st.toast("✅ Validado!")
                            else: st.warning("Já bipado.")
                        else: st.error("Volume inválido!")
                        st.session_state.input_pavuna = ""
                st.text_input("Bipe a entrada:", key="input_pavuna", on_change=reg_pavuna)
                st.metric("Progresso", f"{len(st.session_state['conferidos_agora'])} / {len(lista_esperada)}")
                if st.button("🏁 FINALIZAR"):
                    faltas = [c for c in lista_esperada if c not in st.session_state["conferidos_agora"]]
                    if not faltas: st.success("OK!")
                    else: 
                        st.error(f"Faltas: {len(faltas)}")
                        st.table(pd.DataFrame(faltas, columns=["Chaves Faltantes"]))
                    if st.button("Sair"): del st.session_state["romaneio_pavuna"]; st.rerun()

    # --- ABA BASE DE DADOS (COM NOVA SINTAXE WIDTH='STRETCH') ---
    with tab_base:
        st.title("📊 Consulta e Reimpressão")
        with st.container(border=True):
            c1, c2, c3 = st.columns(3)
            f_rom = c1.text_input("Pesquisar Nº Romaneio", key="filter_rom")
            dt_ini = c2.date_input("Início", value=None)
            dt_fim = c3.date_input("Fim", value=None)
            
            btn_search = st.button("🔍 Pesquisar")

        if btn_search or f_rom:
            q = supabase.table("conferencia_reserva").select("*, romaneios(*)")
            if f_rom: q = q.eq("romaneio_id", f_rom)
            if dt_ini: q = q.gte("data_expedicao", dt_ini.strftime('%Y-%m-%d'))
            if dt_fim: q = q.lte("data_expedicao", dt_fim.strftime('%Y-%m-%d'))
            
            res = q.order("data_expedicao", desc=True).execute()
            
            if res.data:
                df = pd.json_normalize(res.data)
                for col in ['data_expedicao', 'data_recebimento', 'romaneios.data_encerramento']:
                    if col in df.columns and df[col].notnull().any():
                        df[col] = pd.to_datetime(df[col])
                        if df[col].dt.tz is None: df[col] = df[col].dt.tz_localize('UTC')
                        df[col] = df[col].dt.tz_convert('America/Sao_Paulo').dt.strftime('%d/%m/%Y %H:%M:%S')
                
                # ATUALIZADO: width='stretch' substitui use_container_width=True
                st.dataframe(df, width='stretch')

                if f_rom:
                    st.divider()
                    st.subheader(f"🖨️ Ações para o Romaneio #{f_rom}")
                    if st.button("📥 Gerar Impressão do Romaneio"):
                        df_print = df[['chave_nfe']]
                        user_origem = res.data[0]['romaneios']['usuario_criou']
                        imprimir_romaneio_html(f_rom, df_print, user_origem)
            else:
                st.warning("Nenhum registro encontrado.")