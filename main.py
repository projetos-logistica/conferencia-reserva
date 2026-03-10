import re
import streamlit as st
from supabase import create_client
from datetime import datetime, timezone
import pytz
import pandas as pd
import base64
import os

# -------------------------------------------------------------------
# APP CONFIG
# -------------------------------------------------------------------
st.set_page_config(page_title="Gestão Reserva - AZZAS", layout="wide")

# --- 1. CONFIGURAÇÕES E CONEXÃO (SUPABASE) ---
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except Exception:
    st.error("Erro: Credenciais do Supabase não encontradas nos Secrets.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. FUNÇÕES DE SUPORTE ---
FUSO_SP = pytz.timezone("America/Sao_Paulo")

def normalize_chave(value: str) -> str:
    return (value or "").strip().upper()

def get_now_utc():
    """Grava em UTC (padrão recomendado para banco)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def get_base64_of_bin_file(bin_file):
    if os.path.exists(bin_file):
        with open(bin_file, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode()
    return ""

# -------------------------------------------------------------------
# ✅ NOVO: Buscar DESTINO pela CAIXA na tabela SUPABASE "faturamento"
# -------------------------------------------------------------------
@st.cache_data(ttl=6 * 3600, show_spinner=False)
def buscar_destino_por_caixa(caixa: str):
    """
    Busca destino (e filial_origem) na tabela public.faturamento (Supabase)
    Estrutura esperada: caixa, filial_origem, destino, qtde_pecas, created_at
    Retorna: (destino, filial_origem)
    """
    caixa = normalize_chave(caixa)
    if not caixa:
        return None, None

    try:
        res = (
            supabase.table("faturamento")
            .select("destino, filial_origem")
            .eq("caixa", caixa)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            destino = res.data[0].get("destino")
            filial_origem = res.data[0].get("filial_origem")
            return destino, filial_origem
    except Exception as e:
        st.warning(f"⚠️ Falha ao buscar destino no faturamento: {e}")

    return None, None

# -------------------------------------------------------------------
# ✅ Extrair 1+ caixas do input (corrige "duas caixas coladas")
#   - Ex.: F2830233F2830222 -> F2830233 e F2830222
# -------------------------------------------------------------------
CAIXA_PATTERN = re.compile(r"[A-Z]\d{7,}")

def extrair_caixas(raw: str) -> list[str]:
    raw = normalize_chave(raw)
    if not raw:
        return []

    achadas = CAIXA_PATTERN.findall(raw)
    if achadas:
        seen = set()
        out = []
        for c in achadas:
            if c not in seen:
                out.append(c)
                seen.add(c)
        return out

    parts = re.split(r"[^A-Z0-9]+", raw)
    parts = [p for p in parts if p]
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out

def imprimir_romaneio_html(id_romaneio, df_volumes, usuario, origem):
    agora_br = datetime.now(FUSO_SP).strftime("%d/%m/%Y %H:%M")

    df_print = df_volumes.copy()
    if "destino" not in df_print.columns:
        df_print["destino"] = ""
    if "chave_nfe" not in df_print.columns:
        df_print["chave_nfe"] = ""

    df_print["destino"] = df_print["destino"].fillna("").astype(str)
    df_print["chave_nfe"] = df_print["chave_nfe"].fillna("").astype(str)
    df_print = df_print.sort_values(by=["destino", "chave_nfe"], ascending=[True, True])

    qtd_volumes = len(df_print)

    html_print = f"""
    <div id="printarea" style="font-family: sans-serif; padding: 20px;">
        <h2 style="text-align: center; border-bottom: 2px solid #000;">ROMANEIO DE EXPEDIÇÃO - AZZAS</h2>

        <p>
          <strong>Nº Romaneio:</strong> {id_romaneio} |
          <strong>Origem:</strong> {origem} |
          <strong>Qtd. Volumes:</strong> {qtd_volumes}
        </p>

        <p><strong>Usuário Responsável:</strong> {usuario}</p>
        <p><strong>Data de Emissão:</strong> {agora_br}</p>

        <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
            <thead>
                <tr style="background: #eee;">
                    <th style="border: 1px solid #000; padding: 8px; text-align: left; width: 35%;">Caixa</th>
                    <th style="border: 1px solid #000; padding: 8px; text-align: left;">Destino</th>
                </tr>
            </thead>
            <tbody>
                {"".join([
                    f"<tr>"
                    f"<td style='border: 1px solid #000; padding: 8px;'>{r.get('chave_nfe','')}</td>"
                    f"<td style='border: 1px solid #000; padding: 8px;'>{r.get('destino','')}</td>"
                    f"</tr>"
                    for _, r in df_print.iterrows()
                ])}
            </tbody>
        </table>

        <p style="margin-top: 10px;"><strong>Total de volumes:</strong> {qtd_volumes}</p>

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
    st.markdown(
        f"""
        <style>
        .stApp {{ background-image: url("data:image/png;base64,{bg_img}"); background-size: cover; background-position: center; }}
        .brand-container {{ text-align: center; color: black; margin-top: 50px; }}
        .brand-title {{ font-size: 50px; font-weight: 300; letter-spacing: 12px; }}
        div[data-testid="stForm"] {{ background-color: rgba(255, 255, 255, 0.9); border-radius: 20px; padding: 40px; }}
        div[data-testid="stTextInput"] input {{ height: 55px; font-size: 18px; }}
        div.stButton > button {{ width: 100%; height: 55px; background-color: #000 !important; color: #fff !important; font-weight: bold; border-radius: 10px; }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    col_esq, col_meio, col_dir = st.columns([1, 1.4, 1])
    with col_meio:
        st.markdown(
            '<div class="brand-container"><div class="brand-title">AZZAS</div><p style="letter-spacing:5px; font-weight:bold;">FASHION & LIFESTYLE</p></div>',
            unsafe_allow_html=True,
        )
        with st.form("login"):
            email = st.text_input("E-mail", placeholder="seu@email.com").strip().lower()
            unidade = st.selectbox("Unidade", ["Selecione o CD", "CD Reserva", "CD Pavuna"])
            if st.form_submit_button("ENTRAR  →"):
                if email and unidade != "Selecione o CD":
                    st.session_state["auth"] = True
                    st.session_state["user_email"] = email
                    st.session_state["unidade"] = unidade
                    st.rerun()
                else:
                    st.error("Preencha todos os campos.")
    st.stop()

def parse_romaneios(texto: str):
    if not texto:
        return []
    raw = (
        texto.replace(";", ",")
        .replace("\n", ",")
        .replace("\t", ",")
        .replace(" ", "")
    )
    parts = [p for p in raw.split(",") if p]
    ids = []
    for p in parts:
        if p.isdigit():
            ids.append(int(p))
    seen = set()
    out = []
    for i in ids:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out

# --- 3. EXECUÇÃO DO APP ---
if "auth" not in st.session_state:
    show_login()
else:
    st.sidebar.title(f"🏢 {st.session_state['unidade']}")
    st.sidebar.write(f"👤 {st.session_state['user_email']}")
    if st.sidebar.button("Sair"):
        st.session_state.clear()
        st.rerun()

    tab_op, tab_base = st.tabs(["🎯 Operação", "📊 Base de Dados"])

    # =========================
    # OPERAÇÃO
    # =========================
    with tab_op:
        # -------- CD RESERVA --------
        if st.session_state["unidade"] == "CD Reserva":
            st.title("🚛 Expedição CD RESERVA")

            # ✅ impressão aparece automaticamente após encerrar
            if st.session_state.get("print_romaneio_id_reserva"):
                rid = int(st.session_state["print_romaneio_id_reserva"])
                st.success(f"✅ Romaneio #{rid} encerrado.")

                colp1, colp2 = st.columns([1, 1])
                with colp1:
                    if st.button("🖨️ IMPRIMIR ROMANEIO (RESERVA)", type="primary", key="btn_print_reserva"):
                        rr = (
                            supabase.table("conferencia_reserva")
                            .select("chave_nfe, destino, romaneios(usuario_criou, unidade_origem)")
                            .eq("romaneio_id", rid)
                            .order("id", desc=False)
                            .execute()
                        )

                        if rr.data:
                            df_print = pd.DataFrame([
                                {"chave_nfe": x.get("chave_nfe", ""), "destino": x.get("destino", "")}
                                for x in rr.data
                            ])
                            usuario = rr.data[0]["romaneios"].get("usuario_criou", "")
                            origem = rr.data[0]["romaneios"].get("unidade_origem", "CD Reserva")
                            imprimir_romaneio_html(rid, df_print, usuario, origem)
                        else:
                            st.warning("Nenhum volume encontrado para este romaneio.")

                with colp2:
                    if st.button("✅ OK / NOVO ROMANEIO", key="btn_clear_print_reserva"):
                        del st.session_state["print_romaneio_id_reserva"]
                        st.rerun()

                st.divider()

            if "romaneio_id" not in st.session_state:
                if st.button("🚀 ABRIR NOVO ROMANEIO"):
                    res = supabase.table("romaneios").insert(
                        {
                            "usuario_criou": st.session_state["user_email"],
                            "unidade_origem": "CD Reserva",
                            "status": "Aberto",
                        }
                    ).execute()
                    st.session_state["romaneio_id"] = res.data[0]["id"]
                    st.rerun()

            else:
                id_atual = int(st.session_state["romaneio_id"])
                st.info(f"📦 Romaneio Ativo: **#{id_atual}**")

                res_count = (
                    supabase.table("conferencia_reserva")
                    .select("id", count="exact")
                    .eq("romaneio_id", id_atual)
                    .execute()
                )
                total_bipado = res_count.count if res_count.count else 0
                st.metric(label="Volumes Bipados", value=total_bipado)

                def reg_reserva():
                    raw = st.session_state.get("input_reserva")
                    caixas = extrair_caixas(raw)

                    # limpa input o quanto antes (evita concatenar no rerun)
                    st.session_state["input_reserva"] = ""

                    if not caixas:
                        return

                    if len(caixas) > 1:
                        st.warning(f"⚠️ Foram detectadas {len(caixas)} caixas no mesmo input. Vou registrar separadamente.")

                    for chave in caixas:
                        if len(chave) < 4:
                            st.warning(f"Chave muito curta ignorada: {chave}")
                            continue

                        try:
                            dup = (
                                supabase.table("conferencia_reserva")
                                .select("id")
                                .eq("romaneio_id", id_atual)
                                .eq("chave_nfe", chave)
                                .limit(1)
                                .execute()
                            )
                            if dup.data:
                                st.warning(f"⚠️ Já bipado neste romaneio: {chave}")
                                continue

                            # ✅ DESTINO via tabela faturamento
                            destino, filial_origem = buscar_destino_por_caixa(chave)

                            payload = {
                                "chave_nfe": chave,
                                "romaneio_id": id_atual,
                                "data_expedicao": get_now_utc(),
                            }

                            if destino:
                                payload["destino"] = destino

                            supabase.table("conferencia_reserva").insert(payload).execute()
                            st.toast(f"✅ Bipado: {chave[-10:]}")

                        except Exception as e:
                            st.error(f"Erro ao registrar {chave}: {e}")

                st.text_input("Bipe os volumes:", key="input_reserva", on_change=reg_reserva)

                if st.button("🏁 ENCERRAR ROMANEIO", key="btn_fecha_rom_reserva"):
                    supabase.table("romaneios").update({
                        "status": "Encerrado",
                        "data_encerramento": get_now_utc(),
                    }).eq("id", id_atual).execute()

                    st.session_state["print_romaneio_id_reserva"] = id_atual
                    del st.session_state["romaneio_id"]
                    st.rerun()

        # -------- CD PAVUNA --------
        elif st.session_state["unidade"] == "CD Pavuna":
            st.title("🏭 Operação CD PAVUNA")

            if st.session_state.get("force_modo_pavuna"):
                st.session_state["modo_pavuna"] = st.session_state.pop("force_modo_pavuna")

            modo_pavuna = st.radio(
                "Selecione a operação:",
                ["📥 Recebimento (da Reserva)", "🚛 Expedição CD Pavuna"],
                horizontal=True,
                key="modo_pavuna",
            )

            # ===== RECEBIMENTO =====
            if modo_pavuna == "📥 Recebimento (da Reserva)":
                st.subheader("📥 Recebimento de Romaneios vindos do CD Reserva")

                conferir_multiplos = st.toggle("Conferir múltiplos romaneios de uma vez", value=True)

                # -------------------------
                # MODO MULTI
                # -------------------------
                if conferir_multiplos:
                    if "romaneios_pavuna_multi" not in st.session_state:
                        st.session_state["romaneios_pavuna_multi"] = []
                    if "map_chave_para_rom" not in st.session_state:
                        st.session_state["map_chave_para_rom"] = {}
                    if "conferidos_agora_multi" not in st.session_state:
                        st.session_state["conferidos_agora_multi"] = set()
                    if "totais_por_rom" not in st.session_state:
                        st.session_state["totais_por_rom"] = {}

                    if not st.session_state["romaneios_pavuna_multi"]:
                        texto = st.text_area(
                            "Cole os Nº dos Romaneios (Reserva) — separados por vírgula, ponto e vírgula ou linha:",
                            key="rom_multi_input",
                            height=120,
                            placeholder="Ex:\n1234\n1235\n1236",
                        )
                        ids = parse_romaneios(texto)
                        colA, colB = st.columns([1, 2])
                        with colA:
                            abrir = st.button("🔍 Carregar Romaneios", key="btn_carregar_multi")
                        with colB:
                            st.caption("Dica: cole uma lista; o app extrai só os números.")

                        if abrir:
                            if not ids:
                                st.error("Informe ao menos 1 número de romaneio válido.")
                                st.stop()

                            roms = supabase.table("romaneios").select("id, status, unidade_origem").in_("id", ids).execute()
                            encontrados = {r["id"]: r for r in (roms.data or [])}

                            faltando = [i for i in ids if i not in encontrados]
                            invalidos = []
                            validos = []
                            for i in ids:
                                r = encontrados.get(i)
                                if not r:
                                    continue
                                if r.get("status") != "Encerrado" or r.get("unidade_origem") != "CD Reserva":
                                    invalidos.append(i)
                                else:
                                    validos.append(i)

                            if faltando:
                                st.warning(f"⚠️ Não encontrados no Supabase: {faltando}")
                            if invalidos:
                                st.error(f"❌ Inválidos (não encerrados ou não são da Reserva): {invalidos}")
                            if not validos:
                                st.error("Nenhum romaneio válido para conferência.")
                                st.stop()

                            res_envio = (
                                supabase.table("conferencia_reserva")
                                .select("chave_nfe, romaneio_id, data_recebimento")
                                .in_("romaneio_id", validos)
                                .execute()
                            )

                            map_chave = {}
                            totais = {}
                            conferidos_db = set()

                            for row in (res_envio.data or []):
                                c = normalize_chave(row.get("chave_nfe"))
                                rid = row.get("romaneio_id")
                                dr = row.get("data_recebimento")
                                if c and rid:
                                    map_chave[c] = rid
                                    totais[rid] = totais.get(rid, 0) + 1
                                    if dr:
                                        conferidos_db.add(c)

                            if not map_chave:
                                st.error("Não encontrei volumes em conferencia_reserva para esses romaneios.")
                                st.stop()

                            st.session_state["romaneios_pavuna_multi"] = validos
                            st.session_state["map_chave_para_rom"] = map_chave
                            st.session_state["totais_por_rom"] = totais
                            st.session_state["conferidos_agora_multi"] = conferidos_db
                            st.rerun()

                    else:
                        roms_multi = st.session_state["romaneios_pavuna_multi"]
                        map_chave = st.session_state["map_chave_para_rom"]
                        totais = st.session_state["totais_por_rom"]
                        conferidos = st.session_state["conferidos_agora_multi"]

                        st.info(f"✅ Conferindo múltiplos romaneios: **{', '.join(map(str, roms_multi))}**")

                        def reg_pavuna_multi():
                            raw = st.session_state.get("input_pavuna_multi")
                            caixas = extrair_caixas(raw)
                            st.session_state["input_pavuna_multi"] = ""

                            if not caixas:
                                return
                            if len(caixas) > 1:
                                st.warning(f"⚠️ Detectei {len(caixas)} caixas no mesmo input. Vou validar separadamente.")

                            for chave in caixas:
                                rid = map_chave.get(chave)
                                if not rid:
                                    st.error(f"❌ Volume não pertence aos romaneios carregados: {chave}")
                                    continue

                                if chave in conferidos:
                                    st.warning(f"Já bipado (já consta como recebido): {chave}")
                                    continue

                                try:
                                    supabase.table("conferencia_reserva").update(
                                        {"data_recebimento": get_now_utc()}
                                    ).eq("chave_nfe", chave).eq("romaneio_id", int(rid)).execute()

                                    conferidos.add(chave)
                                    st.session_state["conferidos_agora_multi"] = conferidos
                                    st.toast(f"✅ Validado {chave} no romaneio #{rid}!")

                                except Exception as e:
                                    st.error(f"Erro ao validar {chave}: {e}")

                        st.text_input("Bipe a entrada (multi-romaneio):", key="input_pavuna_multi", on_change=reg_pavuna_multi)

                        total_esperado = sum(totais.get(r, 0) for r in roms_multi)
                        st.metric("Qtd volumes (TOTAL esperada)", total_esperado)
                        st.metric("Progresso Total", f"{len(conferidos)} / {total_esperado}")

                        cont_por_rom = {r: 0 for r in roms_multi}
                        for c in conferidos:
                            rid = map_chave.get(c)
                            if rid in cont_por_rom:
                                cont_por_rom[rid] += 1

                        df_prog = pd.DataFrame(
                            [{
                                "Romaneio": r,
                                "Qtd esperada": totais.get(r, 0),
                                "Qtd conferida": cont_por_rom.get(r, 0),
                                "Faltam": max(totais.get(r, 0) - cont_por_rom.get(r, 0), 0)
                            } for r in roms_multi]
                        ).sort_values(["Faltam", "Romaneio"], ascending=[False, True])

                        st.dataframe(df_prog, width="stretch")

                        c1, c2 = st.columns(2)
                        with c1:
                            if st.button("🏁 FINALIZAR CONFERÊNCIA (MULTI)", key="btn_finalizar_multi"):
                                faltantes = []
                                for r in roms_multi:
                                    if cont_por_rom.get(r, 0) != totais.get(r, 0):
                                        faltantes.append(r)

                                if not faltantes:
                                    st.success("✅ Todos os romaneios conferidos com sucesso!")
                                    st.session_state["concluido_pavuna_multi"] = True
                                else:
                                    st.error(f"⚠️ Ainda há romaneios com faltas: {faltantes}")
                        with c2:
                            if st.button("🧹 LIMPAR / TROCAR ROMANEIOS", key="btn_clear_multi"):
                                for k in [
                                    "romaneios_pavuna_multi",
                                    "map_chave_para_rom",
                                    "conferidos_agora_multi",
                                    "totais_por_rom",
                                    "concluido_pavuna_multi",
                                    "rom_multi_input",
                                    "input_pavuna_multi",
                                ]:
                                    if k in st.session_state:
                                        del st.session_state[k]
                                st.rerun()

                        if st.session_state.get("concluido_pavuna_multi"):
                            st.success("Pronto! Você pode carregar novos romaneios quando quiser.")

                # -------------------------
                # MODO SINGLE
                # -------------------------
                else:
                    st.caption("Modo simples: abrir 1 romaneio por vez, com quantidade esperada.")

                    if "romaneio_pavuna_single" not in st.session_state:
                        id_input = st.text_input("Digite o Nº do Romaneio (Reserva):", key="rom_single_input")
                        if id_input and st.button("🔍 Abrir Romaneio", key="btn_abrir_single"):
                            check = supabase.table("romaneios") \
                                .select("*") \
                                .eq("id", int(id_input)) \
                                .eq("status", "Encerrado") \
                                .execute()
                            if check.data and check.data[0].get("unidade_origem") == "CD Reserva":
                                st.session_state["romaneio_pavuna_single"] = int(id_input)
                                st.session_state["conferidos_single"] = set()
                                st.rerun()
                            else:
                                st.error("❌ Romaneio inválido, ainda aberto ou não é da Reserva.")
                    else:
                        rom_id = int(st.session_state["romaneio_pavuna_single"])
                        st.info(f"✅ Conferindo Romaneio (Reserva): **#{rom_id}**")

                        res_count = supabase.table("conferencia_reserva") \
                            .select("id", count="exact") \
                            .eq("romaneio_id", rom_id) \
                            .execute()
                        total_esperado = res_count.count if res_count.count else 0

                        res_envio = supabase.table("conferencia_reserva") \
                            .select("chave_nfe, data_recebimento") \
                            .eq("romaneio_id", rom_id) \
                            .execute()

                        lista_esperada = [normalize_chave(x.get("chave_nfe")) for x in (res_envio.data or [])]
                        recebidos_db = set([normalize_chave(x.get("chave_nfe")) for x in (res_envio.data or []) if x.get("data_recebimento")])

                        conferidos = st.session_state.get("conferidos_single", set())
                        conferidos |= recebidos_db
                        st.session_state["conferidos_single"] = conferidos

                        st.metric("Qtd volumes (esperada)", total_esperado)
                        st.metric("Qtd conferida", len(conferidos))
                        st.metric("Faltam", max(total_esperado - len(conferidos), 0))

                        def reg_pavuna_single():
                            raw = st.session_state.get("input_pavuna_single")
                            caixas = extrair_caixas(raw)
                            st.session_state["input_pavuna_single"] = ""

                            if not caixas:
                                return
                            if len(caixas) > 1:
                                st.warning(f"⚠️ Detectei {len(caixas)} caixas no mesmo input. Vou validar separadamente.")

                            for chave in caixas:
                                if chave not in lista_esperada:
                                    st.error(f"❌ Volume não pertence a este romaneio: {chave}")
                                    continue
                                if chave in conferidos:
                                    st.warning(f"Já bipado: {chave}")
                                    continue

                                try:
                                    supabase.table("conferencia_reserva") \
                                        .update({"data_recebimento": get_now_utc()}) \
                                        .eq("chave_nfe", chave) \
                                        .eq("romaneio_id", rom_id) \
                                        .execute()

                                    conferidos.add(chave)
                                    st.session_state["conferidos_single"] = conferidos
                                    st.toast(f"✅ Validado: {chave}")

                                except Exception as e:
                                    st.error(f"Erro ao validar {chave}: {e}")

                        st.text_input("Bipe a entrada:", key="input_pavuna_single", on_change=reg_pavuna_single)

                        if st.button("🏁 FINALIZAR CONFERÊNCIA", key="btn_finalizar_single"):
                            faltas = [c for c in lista_esperada if c not in conferidos]
                            if not faltas:
                                st.success("✅ Tudo conferido com sucesso!")
                            else:
                                st.error(f"⚠️ Atenção! Faltam: {len(faltas)} volumes")
                                st.table(pd.DataFrame(faltas, columns=["Chaves Faltantes"]))

                        if st.button("📦 PRÓXIMO ROMANEIO", type="primary", key="btn_next_single"):
                            for k in ["romaneio_pavuna_single", "conferidos_single", "rom_single_input", "input_pavuna_single"]:
                                if k in st.session_state:
                                    del st.session_state[k]
                            st.rerun()

            # ===== EXPEDIÇÃO =====
            else:
                st.subheader("🚛 Expedição - CD Pavuna (Gerar Romaneio de Saída)")

                if st.session_state.get("print_romaneio_id"):
                    rid = int(st.session_state["print_romaneio_id"])
                    st.success(f"✅ Romaneio #{rid} encerrado.")

                    if st.button("🖨️ IMPRIMIR ROMANEIO", type="primary", key="btn_print_pavuna"):
                        rr = (
                            supabase.table("conferencia_reserva")
                            .select("chave_nfe, destino, romaneios(usuario_criou, unidade_origem)")
                            .eq("romaneio_id", rid)
                            .order("id", desc=False)
                            .execute()
                        )
                        if rr.data:
                            df_print = pd.DataFrame(
                                [{"chave_nfe": x.get("chave_nfe", ""), "destino": x.get("destino", "")} for x in rr.data]
                            )
                            usuario = rr.data[0]["romaneios"].get("usuario_criou", "")
                            origem = rr.data[0]["romaneios"].get("unidade_origem", "CD Pavuna")
                            imprimir_romaneio_html(rid, df_print, usuario, origem)
                        else:
                            st.warning("Nenhum volume encontrado para este romaneio.")

                    if st.button("✅ LIMPAR IMPRESSÃO", key="btn_clear_print"):
                        del st.session_state["print_romaneio_id"]
                        st.rerun()

                destino_padrao = st.text_input("Destino (opcional):", key="destino_pavuna")

                if "romaneio_id_pavuna_saida" not in st.session_state:
                    if st.button("🚀 ABRIR NOVO ROMANEIO (PAVUNA)", key="btn_abre_rom_pavuna"):
                        res = supabase.table("romaneios").insert(
                            {
                                "usuario_criou": st.session_state["user_email"],
                                "unidade_origem": "CD Pavuna",
                                "status": "Aberto",
                            }
                        ).execute()
                        st.session_state["romaneio_id_pavuna_saida"] = res.data[0]["id"]
                        st.rerun()
                else:
                    id_atual = int(st.session_state["romaneio_id_pavuna_saida"])
                    st.info(f"📦 Romaneio Ativo (Pavuna Saída): **#{id_atual}**")

                    res_count = (
                        supabase.table("conferencia_reserva")
                        .select("id", count="exact")
                        .eq("romaneio_id", id_atual)
                        .execute()
                    )
                    total_bipado = res_count.count if res_count.count else 0
                    st.metric(label="Volumes Bipados", value=total_bipado)

                    def reg_pavuna_saida():
                        raw = st.session_state.get("input_pavuna_saida")
                        caixas = extrair_caixas(raw)
                        st.session_state["input_pavuna_saida"] = ""

                        if not caixas:
                            return
                        if len(caixas) > 1:
                            st.warning(f"⚠️ Detectei {len(caixas)} caixas no mesmo input. Vou registrar separadamente.")

                        st.session_state["force_modo_pavuna"] = "🚛 Expedição CD Pavuna"

                        for chave in caixas:
                            try:
                                dup = (
                                    supabase.table("conferencia_reserva")
                                    .select("id")
                                    .eq("romaneio_id", id_atual)
                                    .eq("chave_nfe", chave)
                                    .limit(1)
                                    .execute()
                                )
                                if dup.data:
                                    st.warning(f"⚠️ Já bipado neste romaneio: {chave}")
                                    continue

                                # se não digitar destino, tenta buscar no faturamento
                                destino_db, _filial = buscar_destino_por_caixa(chave)

                                payload = {
                                    "chave_nfe": chave,
                                    "romaneio_id": id_atual,
                                    "data_expedicao": get_now_utc(),
                                }

                                if destino_padrao.strip():
                                    payload["destino"] = destino_padrao.strip()
                                elif destino_db:
                                    payload["destino"] = destino_db

                                supabase.table("conferencia_reserva").insert(payload).execute()
                                st.toast(f"✅ Bipado: {chave[-10:]}")

                            except Exception as e:
                                st.error(f"Erro ao registrar {chave}: {e}")

                    st.text_input(
                        "Bipe os volumes (saída Pavuna):",
                        key="input_pavuna_saida",
                        on_change=reg_pavuna_saida
                    )

                    if st.button("🏁 ENCERRAR ROMANEIO (PAVUNA)", key="btn_fecha_rom_pavuna"):
                        supabase.table("romaneios").update(
                            {"status": "Encerrado", "data_encerramento": get_now_utc()}
                        ).eq("id", id_atual).execute()

                        st.session_state["print_romaneio_id"] = id_atual
                        st.session_state["force_modo_pavuna"] = "🚛 Expedição CD Pavuna"
                        del st.session_state["romaneio_id_pavuna_saida"]
                        st.rerun()

    # =========================
    # BASE DE DADOS
    # =========================
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

            if f_rom and f_rom.isdigit():
                q = q.eq("romaneio_id", int(f_rom))
            if dt_ini:
                q = q.gte("data_expedicao", dt_ini.strftime("%Y-%m-%d"))
            if dt_fim:
                q = q.lte("data_expedicao", dt_fim.strftime("%Y-%m-%d"))

            res = q.order("data_expedicao", desc=True).execute()

            if res.data:
                df = pd.json_normalize(res.data)

                cols_data = [
                    "data_expedicao",
                    "data_recebimento",
                    "romaneios.created_at",
                    "romaneios.data_encerramento",
                ]
                for col in cols_data:
                    if col in df.columns and df[col].notnull().any():
                        dt = pd.to_datetime(df[col], utc=True, errors="coerce")
                        df[col] = dt.dt.tz_convert("America/Sao_Paulo").dt.strftime("%d/%m/%Y %H:%M:%S")

                sort_cols = []
                if "romaneio_id" in df.columns:
                    sort_cols.append("romaneio_id")
                if "destino" in df.columns:
                    sort_cols.append("destino")
                if "chave_nfe" in df.columns:
                    sort_cols.append("chave_nfe")

                if sort_cols:
                    df = df.sort_values(sort_cols, ascending=True)

                st.dataframe(df, width="stretch")

                if f_rom and f_rom.isdigit():
                    st.divider()
                    if st.button("📥 Reimprimir Romaneio"):
                        rid = int(f_rom)
                        rr = (
                            supabase.table("conferencia_reserva")
                            .select("chave_nfe, destino, romaneios(usuario_criou, unidade_origem)")
                            .eq("romaneio_id", rid)
                            .order("id", desc=False)
                            .execute()
                        )

                        if rr.data:
                            df_print = pd.DataFrame(
                                [{"chave_nfe": x.get("chave_nfe", ""), "destino": x.get("destino", "")} for x in rr.data]
                            )
                            usuario = rr.data[0]["romaneios"].get("usuario_criou", "")
                            origem = rr.data[0]["romaneios"].get("unidade_origem", "")
                            imprimir_romaneio_html(rid, df_print, usuario, origem)
                        else:
                            st.warning("Nenhum volume encontrado para este romaneio.")
            else:
                st.warning("Nenhum registro encontrado.")