import re
import streamlit as st
from supabase import create_client
from datetime import datetime, timezone, timedelta, time
import pytz
import pandas as pd
import base64
import os

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Gestão Reserva - AZZAS", layout="wide")
FUSO_SP = pytz.timezone("America/Sao_Paulo")


# =========================================================
# SUPABASE
# =========================================================
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except Exception:
    st.error("Erro: Credenciais do Supabase não encontradas nos Secrets.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# =========================================================
# HELPERS
# =========================================================
def normalize_chave(value: str) -> str:
    return (value or "").strip().upper()


def get_now_utc() -> str:
    """Grava em UTC."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def format_datetime_sp(value):
    """
    Converte datetime/string ISO para o fuso de São Paulo
    e retorna no formato dd/mm/aaaa HH:MM:SS.
    """
    if value is None or value == "":
        return ""

    try:
        dt = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(dt):
            return ""
        return dt.tz_convert("America/Sao_Paulo").strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(value)


def get_base64_of_bin_file(bin_file: str) -> str:
    if os.path.exists(bin_file):
        with open(bin_file, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode()
    return ""


def parse_romaneios(texto: str) -> list[int]:
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
    # unique mantendo ordem
    seen = set()
    out = []
    for i in ids:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


# =========================================================
# CAIXAS: extrair múltiplas em um input (ex.: F2830233F2830222)
# =========================================================
CAIXA_PATTERN = re.compile(r"[A-Z]\d{7,}")  # Ex.: F2830233


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


# =========================================================
# FATURAMENTO (SUPABASE): destino/filial/qtde por caixa
# =========================================================
@st.cache_data(ttl=6 * 3600, show_spinner=False)
def buscar_destino_por_caixa(caixa: str):
    """
    Busca destino (e filial_origem) na tabela public.faturamento (Supabase).
    Espera colunas: caixa, filial_origem, destino, qtde_pecas, created_at.
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
            return res.data[0].get("destino"), res.data[0].get("filial_origem")
    except Exception as e:
        st.warning(f"⚠️ Falha ao buscar destino no faturamento: {e}")

    return None, None


def chunk_list(items: list[str], size: int = 500) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


@st.cache_data(ttl=2 * 3600, show_spinner=False)
def buscar_faturamento_batch(caixas: list[str]) -> pd.DataFrame:
    """
    Busca em lote na tabela 'faturamento' do Supabase:
    caixa, filial_origem, destino, qtde_pecas, created_at
    Retorna 1 linha por caixa (mais recente por created_at).
    """
    caixas = [normalize_chave(c) for c in caixas if normalize_chave(c)]
    caixas = list(dict.fromkeys(caixas))
    if not caixas:
        return pd.DataFrame(columns=["caixa", "filial_origem", "destino", "qtde_pecas"])

    dfs = []
    for part in chunk_list(caixas, size=500):
        res = (
            supabase.table("faturamento")
            .select("caixa, filial_origem, destino, qtde_pecas, created_at")
            .in_("caixa", part)
            .execute()
        )
        if res.data:
            dfs.append(pd.DataFrame(res.data))

    if not dfs:
        return pd.DataFrame(columns=["caixa", "filial_origem", "destino", "qtde_pecas"])

    df = pd.concat(dfs, ignore_index=True)

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
        df = df.sort_values(["caixa", "created_at"], ascending=[True, False])

    df = df.drop_duplicates(subset=["caixa"], keep="first")

    for col, default in {
        "caixa": "",
        "filial_origem": "",
        "destino": "",
        "qtde_pecas": 0,
    }.items():
        if col not in df.columns:
            df[col] = default

    df["caixa"] = df["caixa"].fillna("").astype(str).str.upper().str.strip()
    df["filial_origem"] = df["filial_origem"].fillna("").astype(str)
    df["destino"] = df["destino"].fillna("").astype(str)
    df["qtde_pecas"] = pd.to_numeric(df["qtde_pecas"], errors="coerce").fillna(0).astype(int)

    return df[["caixa", "filial_origem", "destino", "qtde_pecas"]]


@st.cache_data(ttl=30 * 60, show_spinner=False)
def buscar_caixas_ja_expedidas(caixas: list[str]) -> pd.DataFrame:
    """
    Consulta romaneio_espelho_itens e retorna caixas que já foram expedidas.
    Espera colunas: caixa, romaneio_espelho_id
    """
    caixas = [normalize_chave(c) for c in caixas if normalize_chave(c)]
    caixas = list(dict.fromkeys(caixas))

    if not caixas:
        return pd.DataFrame(columns=["caixa", "romaneio_espelho_id"])

    dfs = []
    for part in chunk_list(caixas, size=500):
        res = (
            supabase.table("romaneio_espelho_itens")
            .select("caixa, romaneio_espelho_id")
            .in_("caixa", part)
            .execute()
        )
        if res.data:
            dfs.append(pd.DataFrame(res.data))

    if not dfs:
        return pd.DataFrame(columns=["caixa", "romaneio_espelho_id"])

    df = pd.concat(dfs, ignore_index=True)
    df["caixa"] = df["caixa"].fillna("").astype(str).str.upper().str.strip()
    df = df.drop_duplicates(subset=["caixa"], keep="first")
    return df[["caixa", "romaneio_espelho_id"]]


# =========================================================
# IMPRESSÃO - ROMANEIO RESERVA/PAVUNA (simples: caixa + destino)
# =========================================================
def imprimir_romaneio_html(id_romaneio, df_volumes, usuario, origem):
    agora_br = datetime.now(FUSO_SP).strftime("%d/%m/%Y %H:%M")

    df_print = df_volumes.copy()

    if "caixa" not in df_print.columns:
        if "chave_nfe" in df_print.columns:
            df_print["caixa"] = df_print["chave_nfe"]
        else:
            df_print["caixa"] = ""

    if "destino" not in df_print.columns:
        df_print["destino"] = ""

    df_print["caixa"] = df_print["caixa"].fillna("").astype(str)
    df_print["destino"] = df_print["destino"].fillna("").astype(str)
    df_print = df_print.sort_values(by=["destino", "caixa"], ascending=[True, True])

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
                    <th style="border: 1px solid #000; padding: 8px; text-align: left; width: 35%;">CAIXA</th>
                    <th style="border: 1px solid #000; padding: 8px; text-align: left;">Destino</th>
                </tr>
            </thead>
            <tbody>
                {"".join([
                    f"<tr>"
                    f"<td style='border: 1px solid #000; padding: 8px;'>{r.get('caixa','')}</td>"
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


# =========================================================
# IMPRESSÃO - ROMANEIO ESPELHO PAVUNA
# colunas: caixa + destino + qtde_pecas
# =========================================================
def imprimir_romaneio_espelho_html(id_romaneio, usuario, origem, df_itens: pd.DataFrame, rota=""):
    agora_br = datetime.now(FUSO_SP).strftime("%d/%m/%Y %H:%M")

    df = df_itens.copy()
    for col in ["caixa", "destino", "qtde_pecas"]:
        if col not in df.columns:
            df[col] = "" if col != "qtde_pecas" else 0

    df["caixa"] = df["caixa"].fillna("").astype(str)
    df["destino"] = df["destino"].fillna("").astype(str)
    df["qtde_pecas"] = pd.to_numeric(df["qtde_pecas"], errors="coerce").fillna(0).astype(int)

    df = df.sort_values(by=["destino", "caixa"], ascending=[True, True])

    qtd_caixas = len(df)
    total_pecas = int(df["qtde_pecas"].sum()) if qtd_caixas else 0

    html = f"""
    <div id="printarea" style="font-family:sans-serif;padding:20px;">
      <h2 style="text-align:center;border-bottom:2px solid #000;">ROMANEIO ESPELHO - CD PAVUNA</h2>

      <p>
        <strong>Nº Romaneio:</strong> {id_romaneio} |
        <strong>Origem:</strong> {origem} |
        <strong>Rota:</strong> {rota} |
        <strong>Qtd. Caixas:</strong> {qtd_caixas} |
        <strong>Qtd. Peças:</strong> {total_pecas}
      </p>

      <p><strong>Usuário Responsável:</strong> {usuario}</p>
      <p><strong>Data de Emissão:</strong> {agora_br}</p>

      <table style="width:100%;border-collapse:collapse;margin-top:15px;">
        <thead>
          <tr style="background:#eee;">
            <th style="border:1px solid #000;padding:8px;text-align:left;width:25%;">Caixa</th>
            <th style="border:1px solid #000;padding:8px;text-align:left;">Destino</th>
            <th style="border:1px solid #000;padding:8px;text-align:right;width:15%;">Qtde Peças</th>
          </tr>
        </thead>
        <tbody>
          {"".join([
            f"<tr>"
            f"<td style='border:1px solid #000;padding:8px;'>{r.get('caixa','')}</td>"
            f"<td style='border:1px solid #000;padding:8px;'>{r.get('destino','')}</td>"
            f"<td style='border:1px solid #000;padding:8px;text-align:right;'>{int(r.get('qtde_pecas',0) or 0)}</td>"
            f"</tr>"
            for _, r in df.iterrows()
          ])}
        </tbody>
      </table>

      <div style="margin-top:60px;text-align:center;">
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
    return st.components.v1.html(html, height=0)


# =========================================================
# LOGIN
# =========================================================
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


# =========================================================
# APP
# =========================================================
if "auth" not in st.session_state:
    show_login()

st.sidebar.title(f"🏢 {st.session_state['unidade']}")
st.sidebar.write(f"👤 {st.session_state['user_email']}")
if st.sidebar.button("Sair"):
    st.session_state.clear()
    st.rerun()

tab_op, tab_base = st.tabs(["🎯 Operação", "📊 Base de Dados"])

# =========================================================
# OPERAÇÃO
# =========================================================
with tab_op:
    # -------------------------
    # CD RESERVA (EXPEDIÇÃO)
    # -------------------------
    if st.session_state["unidade"] == "CD Reserva":
        st.title("🚛 Expedição CD RESERVA")

        # impressão automática após encerrar
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
                            {"caixa": x.get("chave_nfe", ""), "destino": x.get("destino", "")}
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

                        destino, _filial_origem = buscar_destino_por_caixa(chave)

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

    # -------------------------
    # CD PAVUNA (RECEBIMENTO / EXPEDIÇÃO ESPELHO)
    # -------------------------
    elif st.session_state["unidade"] == "CD Pavuna":
        st.title("🏭 Operação CD PAVUNA")

        if st.session_state.get("force_modo_pavuna"):
            st.session_state["modo_pavuna"] = st.session_state.pop("force_modo_pavuna")

        modo_pavuna = st.radio(
            "Selecione a operação:",
            ["📥 Recebimento (da Reserva)", "🚛 Expedição CD Pavuna (Romaneio Espelho)"],
            horizontal=True,
            key="modo_pavuna",
        )

        # =========================
        # RECEBIMENTO (sintaxe atual: multi + single)
        # =========================
        if modo_pavuna == "📥 Recebimento (da Reserva)":
            st.subheader("📥 Recebimento de Romaneios vindos do CD Reserva")
            conferir_multiplos = st.toggle("Conferir múltiplos romaneios de uma vez", value=True)

            # -------- MODO MULTI --------
            if conferir_multiplos:
                if "romaneios_pavuna_multi" not in st.session_state:
                    st.session_state["romaneios_pavuna_multi"] = []
                if "map_chave_para_rom" not in st.session_state:
                    st.session_state["map_chave_para_rom"] = {}
                if "conferidos_agora_multi" not in st.session_state:
                    st.session_state["conferidos_agora_multi"] = []
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
                                if c in map_chave and map_chave[c] != rid:
                                    st.error(f"Caixa duplicada em romaneios diferentes: {c}")
                                    st.stop()
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
                        st.session_state["conferidos_agora_multi"] = list(conferidos_db)
                        st.rerun()

                else:
                    roms_multi = st.session_state["romaneios_pavuna_multi"]
                    map_chave = st.session_state["map_chave_para_rom"]
                    totais = st.session_state["totais_por_rom"]
                    conferidos = set(st.session_state.get("conferidos_agora_multi", []))

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
                                st.session_state["conferidos_agora_multi"] = list(conferidos)
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
                            "Faltam": max(totais.get(r) - cont_por_rom.get(r, 0), 0)
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

            # -------- MODO SINGLE --------
            else:
                st.caption("Modo simples: abrir 1 romaneio por vez, com quantidade esperada.")

                if "romaneio_pavuna_single" not in st.session_state:
                    id_input = st.text_input("Digite o Nº do Romaneio (Reserva):", key="rom_single_input")
                    if id_input and st.button("🔍 Abrir Romaneio", key="btn_abrir_single"):
                        if not id_input.isdigit():
                            st.error("Digite um número de romaneio válido.")
                            st.stop()

                        check = (
                            supabase.table("romaneios")
                            .select("*")
                            .eq("id", int(id_input))
                            .eq("status", "Encerrado")
                            .execute()
                        )
                        if check.data and check.data[0].get("unidade_origem") == "CD Reserva":
                            st.session_state["romaneio_pavuna_single"] = int(id_input)
                            st.session_state["conferidos_single"] = []
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

                    conferidos = set(st.session_state.get("conferidos_single", []))
                    conferidos |= recebidos_db
                    st.session_state["conferidos_single"] = list(conferidos)

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
                                st.session_state["conferidos_single"] = list(conferidos)
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

        # =========================
        # EXPEDIÇÃO CD PAVUNA (ROMANEIO ESPELHO)
        # =========================
        else:
            st.subheader("🚛 Expedição CD Pavuna - Romaneio Espelho (somente recebido)")

            if "espelho_df_full" not in st.session_state:
                st.session_state["espelho_df_full"] = pd.DataFrame(
                    columns=["selecionar", "caixa", "filial_origem", "destino", "qtde_pecas", "ja_expedida", "romaneio_espelho_existente"]
                )
            if "espelho_df" not in st.session_state:
                st.session_state["espelho_df"] = pd.DataFrame(columns=["caixa", "filial_origem", "destino", "qtde_pecas"])
            if "roms_origem_espelho" not in st.session_state:
                st.session_state["roms_origem_espelho"] = []
            if "rota_espelho" not in st.session_state:
                st.session_state["rota_espelho"] = ""

            texto_roms = st.text_area(
                "Cole os Nº dos Romaneios (Reserva) — pode selecionar mais de 1 (linha/vírgula):",
                key="roms_espelho_input",
                height=120,
                placeholder="Ex:\n183\n184\n185",
            )

            colA, colB = st.columns([1, 2])
            with colA:
                btn_add = st.button("➕ Adicionar Romaneios", key="btn_add_roms_espelho")
            with colB:
                st.caption("O app puxa SOMENTE caixas recebidas, bloqueia caixas já expedidas e permite selecionar apenas as desejadas.")

            if btn_add:
                ids = parse_romaneios(texto_roms)
                if not ids:
                    st.error("Informe ao menos 1 romaneio válido.")
                    st.stop()

                roms = supabase.table("romaneios").select("id, status, unidade_origem").in_("id", ids).execute()
                encontrados = {r["id"]: r for r in (roms.data or [])}

                invalidos = []
                validos = []
                for i in ids:
                    r = encontrados.get(i)
                    if not r:
                        invalidos.append(i)
                    else:
                        if r.get("status") != "Encerrado" or r.get("unidade_origem") != "CD Reserva":
                            invalidos.append(i)
                        else:
                            validos.append(i)

                if invalidos:
                    st.error(f"❌ Romaneios inválidos (não encontrados / não encerrados / não são da Reserva): {invalidos}")
                if not validos:
                    st.stop()

                res = (
                    supabase.table("conferencia_reserva")
                    .select("chave_nfe, romaneio_id, data_recebimento")
                    .in_("romaneio_id", validos)
                    .execute()
                )

                caixas = []
                for row in (res.data or []):
                    if row.get("data_recebimento"):
                        caixas.append(normalize_chave(row.get("chave_nfe")))

                caixas = [c for c in caixas if c]
                caixas = list(dict.fromkeys(caixas))

                if not caixas:
                    st.warning("Nenhuma caixa RECEBIDA encontrada nesses romaneios.")
                    st.stop()

                df_batch = buscar_faturamento_batch(caixas)

                df_base = pd.DataFrame({"caixa": caixas})
                df_itens = df_base.merge(df_batch, on="caixa", how="left")
                df_itens["filial_origem"] = df_itens["filial_origem"].fillna("")
                df_itens["destino"] = df_itens["destino"].fillna("")
                df_itens["qtde_pecas"] = pd.to_numeric(df_itens["qtde_pecas"], errors="coerce").fillna(0).astype(int)

                # Bloqueio de caixas já expedidas em romaneio espelho anterior
                df_expedidas = buscar_caixas_ja_expedidas(caixas)

                if not df_expedidas.empty:
                    df_itens = df_itens.merge(
                        df_expedidas.rename(columns={"romaneio_espelho_id": "romaneio_espelho_existente"}),
                        on="caixa",
                        how="left"
                    )
                else:
                    df_itens["romaneio_espelho_existente"] = pd.NA

                df_itens["ja_expedida"] = df_itens["romaneio_espelho_existente"].notna()
                df_itens["selecionar"] = ~df_itens["ja_expedida"]

                st.session_state["espelho_df_full"] = df_itens[
                    ["selecionar", "caixa", "filial_origem", "destino", "qtde_pecas", "ja_expedida", "romaneio_espelho_existente"]
                ].copy()

                st.session_state["roms_origem_espelho"] = validos
                st.session_state["espelho_df"] = df_itens.loc[
                    ~df_itens["ja_expedida"],
                    ["caixa", "filial_origem", "destino", "qtde_pecas"]
                ].copy()

                qtd_bloqueadas = int(df_itens["ja_expedida"].sum())
                if qtd_bloqueadas > 0:
                    st.warning(f"⚠️ {qtd_bloqueadas} caixa(s) já haviam sido expedidas em romaneio espelho anterior e foram bloqueadas.")
                st.success(f"✅ {len(caixas)} caixas recebidas carregadas de {len(validos)} romaneios.")

            df_full = st.session_state.get("espelho_df_full", pd.DataFrame())
            tem_dados = isinstance(df_full, pd.DataFrame) and len(df_full) > 0

            if tem_dados:
                st.divider()
                csel1, csel2 = st.columns([1, 1])

                with csel1:
                    if st.button("✅ Selecionar todas disponíveis", key="btn_sel_all_espelho"):
                        df_full.loc[~df_full["ja_expedida"], "selecionar"] = True
                        st.session_state["espelho_df_full"] = df_full
                        st.rerun()

                with csel2:
                    if st.button("🚫 Limpar seleção", key="btn_unsel_all_espelho"):
                        df_full["selecionar"] = False
                        st.session_state["espelho_df_full"] = df_full
                        st.rerun()

                st.write("### Seleção de caixas para expedição")
                st.caption("Caixas já expedidas anteriormente ficam bloqueadas e não podem ser selecionadas.")

                df_view = df_full.copy()
                df_view["status"] = df_view.apply(
                    lambda r: f"Já expedida no espelho #{int(r['romaneio_espelho_existente'])}" if r["ja_expedida"] else "Disponível",
                    axis=1
                )

                edited_df = st.data_editor(
                    df_view[["selecionar", "caixa", "filial_origem", "destino", "qtde_pecas", "status"]],
                    hide_index=True,
                    width="stretch",
                    disabled=["caixa", "filial_origem", "destino", "qtde_pecas", "status"],
                    column_config={
                        "selecionar": st.column_config.CheckboxColumn("Selecionar"),
                        "caixa": "Caixa",
                        "filial_origem": "Filial Origem",
                        "destino": "Destino",
                        "qtde_pecas": "Qtde Peças",
                        "status": "Status",
                    },
                    key="editor_espelho"
                )

                df_full["selecionar"] = edited_df["selecionar"].astype(bool)
                df_full.loc[df_full["ja_expedida"], "selecionar"] = False
                st.session_state["espelho_df_full"] = df_full

                df_selecionado = df_full.loc[
                    (df_full["selecionar"]) & (~df_full["ja_expedida"]),
                    ["caixa", "filial_origem", "destino", "qtde_pecas"]
                ].copy()

                st.session_state["espelho_df"] = df_selecionado

            df_itens = st.session_state.get("espelho_df", pd.DataFrame())
            qtd_caixas = len(df_itens) if isinstance(df_itens, pd.DataFrame) else 0
            total_pecas = int(df_itens["qtde_pecas"].sum()) if isinstance(df_itens, pd.DataFrame) and "qtde_pecas" in df_itens.columns else 0

            st.divider()
            st.text_input(
                "Rota",
                key="rota_espelho",
                placeholder="Ex.: ROTA 01, ROTA 02, ROTA 100"
            )

            cM1, cM2, cM3 = st.columns(3)
            cM1.metric("Qtd. Caixas Selecionadas", qtd_caixas)
            cM2.metric("Qtd. Peças", total_pecas)
            cM3.metric("Romaneios origem", len(st.session_state.get("roms_origem_espelho", [])))

            if isinstance(df_itens, pd.DataFrame) and len(df_itens):
                st.dataframe(df_itens.sort_values(["destino", "caixa"], ascending=[True, True]), width="stretch")
            else:
                st.info("Nenhuma caixa selecionada ainda para o romaneio espelho.")

            colF1, colF2, colF3 = st.columns([1, 1, 2])
            with colF1:
                btn_finalizar = st.button("🏁 Finalizar Romaneio Espelho", type="primary", key="btn_finalizar_espelho")
            with colF2:
                btn_limpar = st.button("🧹 Limpar", key="btn_limpar_espelho")

            if btn_limpar:
                for k in ["espelho_df", "espelho_df_full", "roms_origem_espelho", "print_rom_espelho_id", "rota_espelho"]:
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()

            if btn_finalizar:
                if not isinstance(df_itens, pd.DataFrame) or len(df_itens) == 0:
                    st.error("Selecione ao menos 1 caixa para finalizar.")
                    st.stop()

                rota = (st.session_state.get("rota_espelho") or "").strip().upper()
                if not rota:
                    st.error("Informe a rota antes de finalizar.")
                    st.stop()

                # Revalidação no banco para evitar duplicidade entre usuários
                caixas_final = df_itens["caixa"].fillna("").astype(str).str.upper().str.strip().tolist()
                df_expedidas_now = buscar_caixas_ja_expedidas(caixas_final)

                if not df_expedidas_now.empty:
                    caixas_bloqueadas = df_expedidas_now["caixa"].tolist()
                    st.error(f"❌ Estas caixas já foram expedidas anteriormente e não podem seguir: {caixas_bloqueadas}")
                    st.stop()

                usuario = st.session_state["user_email"]

                res_rom = supabase.table("romaneios_espelho").insert({
                    "usuario_criou": usuario,
                    "unidade_origem": "CD Pavuna",
                    "status": "Encerrado",
                    "romaneios_origem": st.session_state.get("roms_origem_espelho", []),
                    "qtd_caixas": int(len(df_itens)),
                    "rota": rota,
                }).execute()

                rom_id = res_rom.data[0]["id"]

                itens_payload = []
                for _, r in df_itens.iterrows():
                    itens_payload.append({
                        "romaneio_espelho_id": int(rom_id),
                        "caixa": str(r.get("caixa", "")),
                        "filial_origem": str(r.get("filial_origem", "")),
                        "destino": str(r.get("destino", "")),
                        "qtde_pecas": int(r.get("qtde_pecas", 0) or 0),
                    })

                supabase.table("romaneio_espelho_itens").insert(itens_payload).execute()

                st.session_state["print_rom_espelho_id"] = rom_id
                st.success(f"✅ Romaneio espelho #{rom_id} finalizado na rota {rota}.")

            if st.session_state.get("print_rom_espelho_id"):
                rid = int(st.session_state["print_rom_espelho_id"])
                if st.button("🖨️ IMPRIMIR ROMANEIO ESPELHO", type="primary", key="btn_print_espelho"):
                    imprimir_romaneio_espelho_html(
                        id_romaneio=rid,
                        usuario=st.session_state["user_email"],
                        origem="CD Pavuna",
                        rota=st.session_state.get("rota_espelho", ""),
                        df_itens=st.session_state["espelho_df"],
                    )
                if st.button("✅ OK / NOVO", key="btn_ok_novo_espelho"):
                    for k in ["espelho_df", "espelho_df_full", "roms_origem_espelho", "print_rom_espelho_id", "roms_espelho_input", "rota_espelho"]:
                        if k in st.session_state:
                            del st.session_state[k]
                    st.rerun()


# =========================================================
# BASE DE DADOS
# =========================================================
with tab_base:
    st.title("📊 Consulta e Reimpressão")

    tipo_consulta = st.radio(
        "Tipo de consulta",
        ["Romaneio Reserva", "Romaneio Pavuna (Espelho)"],
        horizontal=True
    )

    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        f_rom = c1.text_input("Pesquisar Nº Romaneio", key="filter_rom")
        dt_ini = c2.date_input("Início", value=None, key="dt_ini_base")
        dt_fim = c3.date_input("Fim", value=None, key="dt_fim_base")
        btn_search = st.button("🔍 Pesquisar")

    # =====================================================
    # CONSULTA ROMANEIO RESERVA
    # =====================================================
    if tipo_consulta == "Romaneio Reserva":
        if btn_search or f_rom:
            q = supabase.table("conferencia_reserva").select("*, romaneios(*)")

            if f_rom and f_rom.isdigit():
                q = q.eq("romaneio_id", int(f_rom))
            if dt_ini:
                dt_ini_full = datetime.combine(dt_ini, time.min).strftime("%Y-%m-%dT%H:%M:%S")
                q = q.gte("data_expedicao", dt_ini_full)
            if dt_fim:
                dt_fim_full = datetime.combine(dt_fim + timedelta(days=1), time.min).strftime("%Y-%m-%dT%H:%M:%S")
                q = q.lt("data_expedicao", dt_fim_full)

            res = q.order("data_expedicao", desc=True).execute()

            if res.data:
                df = pd.json_normalize(res.data)

                cols_data = [
                    "created_at",
                    "criado_em",
                    "data_expedicao",
                    "data_recebimento",
                    "data_encerramento",
                    "romaneios.created_at",
                    "romaneios.criado_em",
                    "romaneios.data_encerramento",
                ]

                for col in cols_data:
                    if col in df.columns:
                        df[col] = df[col].apply(format_datetime_sp)

                sort_cols = []
                if "romaneio_id" in df.columns:
                    sort_cols.append("romaneio_id")
                if "destino" in df.columns:
                    sort_cols.append("destino")
                if "chave_nfe" in df.columns:
                    sort_cols.append("chave_nfe")
                if sort_cols:
                    df = df.sort_values(sort_cols, ascending=True)

                rename_map = {
                    "romaneio_id": "Romaneio",
                    "chave_nfe": "CAIXA",
                    "destino": "Destino",
                    "data_expedicao": "Data Expedição",
                    "data_recebimento": "Data Recebimento",
                    "created_at": "Criado em",
                    "criado_em": "Criado em",
                    "data_encerramento": "Data Encerramento",
                    "romaneios.usuario_criou": "Usuário",
                    "romaneios.unidade_origem": "Unidade Origem",
                    "romaneios.created_at": "Romaneio Criado em",
                    "romaneios.criado_em": "Romaneio Criado em",
                    "romaneios.data_encerramento": "Romaneio Encerrado em",
                }
                df = df.rename(columns=rename_map)

                st.dataframe(df, width="stretch")

                if f_rom and f_rom.isdigit():
                    st.divider()
                    if st.button("📥 Reimprimir Romaneio Reserva"):
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
                                [{"caixa": x.get("chave_nfe", ""), "destino": x.get("destino", "")} for x in rr.data]
                            )
                            usuario = rr.data[0]["romaneios"].get("usuario_criou", "")
                            origem = rr.data[0]["romaneios"].get("unidade_origem", "")
                            imprimir_romaneio_html(rid, df_print, usuario, origem)
                        else:
                            st.warning("Nenhum volume encontrado para este romaneio.")
            else:
                st.warning("Nenhum registro encontrado.")

    # =====================================================
    # CONSULTA ROMANEIO PAVUNA / ESPELHO
    # =====================================================
    else:
        if btn_search or f_rom:
            q = supabase.table("romaneios_espelho").select("*")

            if f_rom and f_rom.isdigit():
                q = q.eq("id", int(f_rom))
            if dt_ini:
                dt_ini_full = datetime.combine(dt_ini, time.min).strftime("%Y-%m-%dT%H:%M:%S")
                q = q.gte("created_at", dt_ini_full)
            if dt_fim:
                dt_fim_full = datetime.combine(dt_fim + timedelta(days=1), time.min).strftime("%Y-%m-%dT%H:%M:%S")
                q = q.lt("created_at", dt_fim_full)

            res = q.order("id", desc=True).execute()

            if res.data:
                df = pd.DataFrame(res.data)

                for col in ["created_at", "criado_em"]:
                    if col in df.columns:
                        df[col] = df[col].apply(format_datetime_sp)

                rename_map = {
                    "id": "Romaneio Espelho",
                    "usuario_criou": "Usuário",
                    "unidade_origem": "Unidade Origem",
                    "status": "Status",
                    "romaneios_origem": "Romaneios Origem",
                    "qtd_caixas": "Qtd Caixas",
                    "rota": "Rota",
                    "created_at": "Criado em",
                    "criado_em": "Criado em",
                }
                df = df.rename(columns=rename_map)

                st.dataframe(df, width="stretch")

                if f_rom and f_rom.isdigit():
                    st.divider()
                    if st.button("🖨️ Reimprimir Romaneio Pavuna"):
                        rid = int(f_rom)

                        rom = (
                            supabase.table("romaneios_espelho")
                            .select("*")
                            .eq("id", rid)
                            .limit(1)
                            .execute()
                        )

                        itens = (
                            supabase.table("romaneio_espelho_itens")
                            .select("caixa, destino, qtde_pecas")
                            .eq("romaneio_espelho_id", rid)
                            .order("destino", desc=False)
                            .execute()
                        )

                        if rom.data and itens.data:
                            df_print = pd.DataFrame(itens.data)
                            usuario = rom.data[0].get("usuario_criou", "")
                            origem = rom.data[0].get("unidade_origem", "CD Pavuna")
                            rota = rom.data[0].get("rota", "")
                            imprimir_romaneio_espelho_html(
                                id_romaneio=rid,
                                usuario=usuario,
                                origem=origem,
                                rota=rota,
                                df_itens=df_print,
                            )
                        else:
                            st.warning("Nenhum item encontrado para este romaneio espelho.")
            else:
                st.warning("Nenhum registro encontrado.")