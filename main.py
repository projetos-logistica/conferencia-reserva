import re
import base64
import os
import urllib.parse
from datetime import datetime, timezone

import pandas as pd
import pytz
import streamlit as st
from sqlalchemy import create_engine, text
from supabase import create_client

# -------------------------------------------------------------------
# APP CONFIG
# -------------------------------------------------------------------
st.set_page_config(page_title="Gestão Reserva - AZZAS", layout="wide")

# -------------------------------------------------------------------
# CONFIG / CONEXÕES
# -------------------------------------------------------------------
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except Exception:
    st.error("Erro: Credenciais do Supabase não encontradas nos Secrets.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

FUSO_SP = pytz.timezone("America/Sao_Paulo")

# -------------------------------------------------------------------
# FUNÇÕES DE SUPORTE
# -------------------------------------------------------------------
def normalize_chave(value) -> str:
    return str(value or "").strip().upper()


def get_now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_base64_of_bin_file(bin_file):
    if os.path.exists(bin_file):
        with open(bin_file, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode()
    return ""


def formatar_coluna_datetime(df: pd.DataFrame, colunas: list[str]) -> pd.DataFrame:
    for col in colunas:
        if col in df.columns:
            serie = df[col]
            if serie.notna().any():
                dt = pd.to_datetime(serie, errors="coerce", utc=True)
                df[col] = dt.dt.tz_convert("America/Sao_Paulo").dt.strftime("%d/%m/%Y %H:%M:%S")
    return df


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def buscar_destino_por_caixa(caixa: str):
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


def buscar_status_romaneio(id_romaneio: int):
    try:
        res = (
            supabase.table("romaneios")
            .select("id, status, unidade_origem")
            .eq("id", int(id_romaneio))
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0]
    except Exception as e:
        st.warning(f"⚠️ Falha ao buscar status do romaneio: {e}")
    return None


def excluir_romaneio_completo(id_romaneio: int):
    try:
        supabase.table("conferencia_reserva").delete().eq("romaneio_id", int(id_romaneio)).execute()
        supabase.table("romaneios").delete().eq("id", int(id_romaneio)).execute()
        return True, None
    except Exception as e:
        return False, str(e)


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


# -------------------------------------------------------------------
# EXTRATOR DE CAIXAS
# -------------------------------------------------------------------
CAIXA_PATTERN = re.compile(r"[A-Z]?\d{7,}")


def extrair_caixas(raw: str) -> list[str]:
    raw = normalize_chave(raw)
    if not raw:
        return []

    achadas = CAIXA_PATTERN.findall(raw)
    if achadas:
        seen = set()
        out = []
        for c in achadas:
            c = normalize_chave(c)
            if c and c not in seen:
                out.append(c)
                seen.add(c)
        return out

    parts = re.split(r"[^A-Z0-9]+", raw)
    parts = [normalize_chave(p) for p in parts if p]
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out


# -------------------------------------------------------------------
# SQL SERVER - PAVUNA
# -------------------------------------------------------------------
@st.cache_resource
def get_sqlserver_engine():
    driver = st.secrets["SQLSERVER_DRIVER"]
    server = st.secrets["SQLSERVER_SERVER"]
    database = st.secrets["SQLSERVER_DATABASE"]
    uid = st.secrets["SQLSERVER_UID"]
    pwd = st.secrets["SQLSERVER_PWD"]
    encrypt = st.secrets.get("SQLSERVER_ENCRYPT", "no")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
        f"Encrypt={encrypt};"
        "TrustServerCertificate=yes;"
    )
    params = urllib.parse.quote_plus(conn_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True)


sql_engine = get_sqlserver_engine()


def chunk_list(items: list[str], size: int = 700) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def buscar_dados_caixas_batch(caixas: list[str]) -> pd.DataFrame:
    caixas = [normalize_chave(c) for c in caixas if normalize_chave(c)]
    caixas = list(dict.fromkeys(caixas))
    if not caixas:
        return pd.DataFrame(columns=["caixa", "filial_origem", "destino", "qtde_pecas"])

    dfs = []
    for chunk in chunk_list(caixas, size=700):
        params = {f"c{i}": c for i, c in enumerate(chunk)}
        in_clause = ", ".join([f":c{i}" for i in range(len(chunk))])

        q = text(f"""
            WITH x AS (
                SELECT
                    FP.CAIXA       AS CAIXA,
                    F.FILIAL       AS FILIAL,
                    F.NOME_CLIFOR  AS DESTINO,
                    FP.QTDE        AS QTDE,
                    ROW_NUMBER() OVER (
                        PARTITION BY FP.CAIXA
                        ORDER BY FP.NF_SAIDA DESC
                    ) AS rn
                FROM FATURAMENTO F
                LEFT JOIN FATURAMENTO_PROD FP
                  ON F.SERIE_NF = FP.SERIE_NF
                 AND F.NF_SAIDA = FP.NF_SAIDA
                 AND F.FILIAL   = FP.FILIAL
                WHERE FP.CAIXA IN ({in_clause})
            )
            SELECT
                CAIXA,
                FILIAL,
                DESTINO,
                QTDE
            FROM x
            WHERE rn = 1;
        """)

        with sql_engine.connect() as conn:
            rows = conn.execute(q, params).mappings().all()

        if rows:
            df = pd.DataFrame(rows).rename(columns={
                "CAIXA": "caixa",
                "FILIAL": "filial_origem",
                "DESTINO": "destino",
                "QTDE": "qtde_pecas",
            })
            df["caixa"] = df["caixa"].astype(str).str.upper().str.strip()
            df["filial_origem"] = df["filial_origem"].astype(str).fillna("")
            df["destino"] = df["destino"].astype(str).fillna("")
            df["qtde_pecas"] = pd.to_numeric(df["qtde_pecas"], errors="coerce").fillna(0).astype(int)
            dfs.append(df)

    if not dfs:
        return pd.DataFrame(columns=["caixa", "filial_origem", "destino", "qtde_pecas"])

    out = pd.concat(dfs, ignore_index=True)
    out = out.drop_duplicates(subset=["caixa"], keep="first")
    return out


# -------------------------------------------------------------------
# IMPRESSÃO
# -------------------------------------------------------------------
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
                    <th style="border: 1px solid #000; padding: 8px; text-align: left; width: 35%;">CAIXA</th>
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


def imprimir_romaneio_pavuna_html(rom_id, usuario, origem, df_itens: pd.DataFrame):
    agora_br = datetime.now(FUSO_SP).strftime("%d/%m/%Y %H:%M")

    df = df_itens.copy()
    for col in ["destino", "caixa", "filial_origem"]:
        if col not in df.columns:
            df[col] = ""
    if "qtde_pecas" not in df.columns:
        df["qtde_pecas"] = 0

    df["destino"] = df["destino"].fillna("").astype(str)
    df["caixa"] = df["caixa"].fillna("").astype(str)
    df["filial_origem"] = df["filial_origem"].fillna("").astype(str)
    df["qtde_pecas"] = pd.to_numeric(df["qtde_pecas"], errors="coerce").fillna(0).astype(int)

    df = df.sort_values(by=["destino", "caixa"], ascending=[True, True])

    qtd_caixas = len(df)
    total_pecas = int(df["qtde_pecas"].sum()) if len(df) else 0

    html = f"""
    <div id="printarea" style="font-family:sans-serif;padding:20px;">
      <h2 style="text-align:center;border-bottom:2px solid #000;">ROMANEIO CD PAVUNA</h2>

      <p>
        <strong>Nº Romaneio:</strong> {rom_id} |
        <strong>Origem:</strong> {origem} |
        <strong>Qtd. Caixas:</strong> {qtd_caixas} |
        <strong>Qtd. Peças:</strong> {total_pecas}
      </p>
      <p><strong>Usuário Responsável:</strong> {usuario}</p>
      <p><strong>Data de Emissão:</strong> {agora_br}</p>

      <table style="width:100%;border-collapse:collapse;margin-top:15px;">
        <thead>
          <tr style="background:#eee;">
            <th style="border:1px solid #000;padding:8px;text-align:left;width:18%;">CAIXA</th>
            <th style="border:1px solid #000;padding:8px;text-align:left;width:18%;">Filial Origem</th>
            <th style="border:1px solid #000;padding:8px;text-align:left;">Destino</th>
            <th style="border:1px solid #000;padding:8px;text-align:right;width:12%;">Qtde Peças</th>
          </tr>
        </thead>
        <tbody>
          {"".join([
            f"<tr>"
            f"<td style='border:1px solid #000;padding:8px;'>{r.get('caixa','')}</td>"
            f"<td style='border:1px solid #000;padding:8px;'>{r.get('filial_origem','')}</td>"
            f"<td style='border:1px solid #000;padding:8px;'>{r.get('destino','')}</td>"
            f"<td style='border:1px solid #000;padding:8px;text-align:right;'>{int(r.get('qtde_pecas',0) or 0)}</td>"
            f"</tr>"
            for _, r in df.iterrows()
          ])}
        </tbody>
      </table>

      <p style="margin-top:10px;">
        <strong>Total de caixas:</strong> {qtd_caixas} |
        <strong>Total de peças:</strong> {total_pecas}
      </p>

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


# -------------------------------------------------------------------
# LOGIN
# -------------------------------------------------------------------
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


# -------------------------------------------------------------------
# APP
# -------------------------------------------------------------------
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
        if st.session_state["unidade"] == "CD Reserva":
            st.title("🚛 Expedição CD RESERVA")

            if st.session_state.get("print_romaneio_id_reserva"):
                rid = int(st.session_state["print_romaneio_id_reserva"])
                st.success(f"✅ Romaneio #{rid} encerrado.")

                colp1, colp2 = st.columns([1, 1])
                with colp1:
                    if st.button("🖨️ IMPRIMIR ROMANEIO (RESERVA)", type="primary", key="btn_print_reserva"):
                        info_rom = buscar_status_romaneio(rid)
                        if not info_rom:
                            st.error("❌ Romaneio não encontrado.")
                        else:
                            status_rom = info_rom.get("status", "")
                            origem_rom = info_rom.get("unidade_origem", "")

                            if origem_rom == "CD Reserva" and status_rom != "Encerrado":
                                st.error("❌ Romaneio não encerrado. Favor finalizá-lo para concluir a operação.")
                            else:
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

                else:
                    st.caption("Modo simples: abrir 1 romaneio por vez, com quantidade esperada.")

                    if "romaneio_pavuna_single" not in st.session_state:
                        id_input = st.text_input("Digite o Nº do Romaneio (Reserva):", key="rom_single_input")
                        if id_input and st.button("🔍 Abrir Romaneio", key="btn_abrir_single"):
                            check = (
                                supabase.table("romaneios")
                                .select("*")
                                .eq("id", int(id_input))
                                .eq("status", "Encerrado")
                                .execute()
                            )
                            if check.data and check.data[0].get("unidade_origem") == "CD Reserva":
                                st.session_state["romaneio_pavuna_single"] = int(id_input)
                                st.session_state["conferidos_single"] = set()
                                st.rerun()
                            else:
                                st.error("❌ Romaneio inválido, ainda aberto ou não é da Reserva.")
                    else:
                        rom_id = int(st.session_state["romaneio_pavuna_single"])
                        st.info(f"✅ Conferindo Romaneio (Reserva): **#{rom_id}**")

                        res_count = (
                            supabase.table("conferencia_reserva")
                            .select("id", count="exact")
                            .eq("romaneio_id", rom_id)
                            .execute()
                        )
                        total_esperado = res_count.count if res_count.count else 0

                        res_envio = (
                            supabase.table("conferencia_reserva")
                            .select("chave_nfe, data_recebimento")
                            .eq("romaneio_id", rom_id)
                            .execute()
                        )

                        lista_esperada = [normalize_chave(x.get("chave_nfe")) for x in (res_envio.data or [])]
                        recebidos_db = set([
                            normalize_chave(x.get("chave_nfe"))
                            for x in (res_envio.data or [])
                            if x.get("data_recebimento")
                        ])

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
                                    (
                                        supabase.table("conferencia_reserva")
                                        .update({"data_recebimento": get_now_utc()})
                                        .eq("chave_nfe", chave)
                                        .eq("romaneio_id", rom_id)
                                        .execute()
                                    )

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
                st.subheader("🚛 Expedição - CD Pavuna")

                if "pavuna_saida_itens" not in st.session_state:
                    st.session_state["pavuna_saida_itens"] = pd.DataFrame(
                        columns=["caixa", "filial_origem", "destino", "qtde_pecas", "romaneio_origem"]
                    )
                if "pavuna_saida_roms_origem" not in st.session_state:
                    st.session_state["pavuna_saida_roms_origem"] = []
                if "print_romaneio_pavuna_id" not in st.session_state:
                    st.session_state["print_romaneio_pavuna_id"] = None

                if st.session_state.get("print_romaneio_pavuna_id"):
                    rid = int(st.session_state["print_romaneio_pavuna_id"])
                    st.success(f"✅ Romaneio Pavuna #{rid} encerrado.")

                    col_print_1, col_print_2 = st.columns(2)
                    with col_print_1:
                        if st.button("🖨️ IMPRIMIR ROMANEIO PAVUNA", type="primary", key="btn_print_pavuna_novo"):
                            df_print = st.session_state.get("pavuna_saida_itens", pd.DataFrame()).copy()
                            if len(df_print):
                                imprimir_romaneio_pavuna_html(
                                    rom_id=rid,
                                    usuario=st.session_state["user_email"],
                                    origem="CD Pavuna",
                                    df_itens=df_print,
                                )
                            else:
                                st.warning("Nenhum item disponível para impressão.")

                    with col_print_2:
                        if st.button("✅ OK / NOVO ROMANEIO", key="btn_clear_print_pavuna_novo"):
                            for k in [
                                "pavuna_saida_itens",
                                "pavuna_saida_roms_origem",
                                "print_romaneio_pavuna_id",
                                "roms_pavuna_saida_input",
                                "editor_pavuna_saida",
                            ]:
                                if k in st.session_state:
                                    del st.session_state[k]
                            st.rerun()

                    st.divider()

                st.markdown("### 1) Selecionar romaneios recebidos da Reserva")
                roms_texto = st.text_area(
                    "Cole os números dos romaneios da Reserva:",
                    key="roms_pavuna_saida_input",
                    height=100,
                    placeholder="Ex:\n183\n184\n185"
                )

                col_a, col_b = st.columns([1, 2])
                with col_a:
                    btn_carregar_pavuna_saida = st.button("➕ Carregar romaneios", key="btn_carregar_pavuna_saida")
                with col_b:
                    st.caption("Serão carregadas apenas as caixas já recebidas em Pavuna (com data_recebimento preenchida).")

                if btn_carregar_pavuna_saida:
                    rom_ids = parse_romaneios(roms_texto)

                    if not rom_ids:
                        st.error("Informe ao menos 1 romaneio.")
                        st.stop()

                    roms = supabase.table("romaneios").select("id, status, unidade_origem").in_("id", rom_ids).execute()
                    encontrados = {r["id"]: r for r in (roms.data or [])}

                    faltando = [i for i in rom_ids if i not in encontrados]
                    invalidos = []
                    validos = []

                    for i in rom_ids:
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
                        st.stop()

                    res = (
                        supabase.table("conferencia_reserva")
                        .select("chave_nfe, romaneio_id, data_recebimento")
                        .in_("romaneio_id", validos)
                        .execute()
                    )

                    caixas_rows = []
                    for row in (res.data or []):
                        if row.get("data_recebimento"):
                            caixa = normalize_chave(row.get("chave_nfe"))
                            if caixa:
                                caixas_rows.append({
                                    "caixa": caixa,
                                    "romaneio_origem": row.get("romaneio_id")
                                })

                    if not caixas_rows:
                        st.warning("Nenhuma caixa com data_recebimento encontrada nesses romaneios.")
                        st.stop()

                    df_caixas = pd.DataFrame(caixas_rows).drop_duplicates(subset=["caixa"], keep="first")

                    df_batch = buscar_dados_caixas_batch(df_caixas["caixa"].tolist())

                    df_itens = df_caixas.merge(df_batch, on="caixa", how="left")
                    df_itens["filial_origem"] = df_itens["filial_origem"].fillna("")
                    df_itens["destino"] = df_itens["destino"].fillna("")
                    df_itens["qtde_pecas"] = pd.to_numeric(df_itens["qtde_pecas"], errors="coerce").fillna(0).astype(int)

                    st.session_state["pavuna_saida_itens"] = df_itens
                    st.session_state["pavuna_saida_roms_origem"] = validos

                    if "editor_pavuna_saida" in st.session_state:
                        del st.session_state["editor_pavuna_saida"]

                    st.success(f"✅ Carregado: {len(df_itens)} caixas de {len(validos)} romaneios.")

                st.markdown("### 2) Revisar caixas do romaneio Pavuna")
                df_itens = st.session_state.get("pavuna_saida_itens", pd.DataFrame()).copy()

                if isinstance(df_itens, pd.DataFrame) and len(df_itens):
                    st.metric("Qtd. Caixas", len(df_itens))
                    st.metric("Qtd. Peças", int(df_itens["qtde_pecas"].sum()) if "qtde_pecas" in df_itens.columns else 0)

                    df_exib = df_itens.copy()
                    df_exib["EXCLUIR"] = False

                    edited_df = st.data_editor(
                        df_exib[["EXCLUIR", "caixa", "filial_origem", "destino", "qtde_pecas", "romaneio_origem"]],
                        width="stretch",
                        hide_index=True,
                        disabled=["caixa", "filial_origem", "destino", "qtde_pecas", "romaneio_origem"],
                        column_config={
                            "EXCLUIR": st.column_config.CheckboxColumn("Excluir"),
                            "caixa": st.column_config.TextColumn("CAIXA"),
                            "filial_origem": st.column_config.TextColumn("FILIAL_ORIGEM"),
                            "destino": st.column_config.TextColumn("DESTINO"),
                            "qtde_pecas": st.column_config.NumberColumn("QTDE_PEÇAS"),
                            "romaneio_origem": st.column_config.NumberColumn("ROMANEIO_ORIGEM"),
                        },
                        key="editor_pavuna_saida"
                    )

                    selecionadas_excluir = edited_df.loc[edited_df["EXCLUIR"] == True, "caixa"].tolist()

                    col_x, col_y = st.columns(2)
                    with col_x:
                        if st.button("🗑️ Excluir caixas marcadas", key="btn_excluir_caixas_pavuna"):
                            if not selecionadas_excluir:
                                st.warning("Nenhuma caixa marcada.")
                            else:
                                df_filtrado = df_itens[~df_itens["caixa"].isin(selecionadas_excluir)].copy()
                                st.session_state["pavuna_saida_itens"] = df_filtrado
                                if "editor_pavuna_saida" in st.session_state:
                                    del st.session_state["editor_pavuna_saida"]
                                st.success(f"✅ {len(selecionadas_excluir)} caixa(s) removida(s).")
                                st.rerun()

                    with col_y:
                        if st.button("🔄 Limpar seleção / recarregar lista", key="btn_reset_caixas_pavuna"):
                            if "editor_pavuna_saida" in st.session_state:
                                del st.session_state["editor_pavuna_saida"]
                            st.rerun()

                    st.markdown("### 3) Finalizar romaneio da Pavuna")
                    if st.button("🏁 FINALIZAR ROMANEIO (PAVUNA)", type="primary", key="btn_finalizar_romaneio_pavuna_novo"):
                        df_final = st.session_state.get("pavuna_saida_itens", pd.DataFrame()).copy()

                        if not isinstance(df_final, pd.DataFrame) or len(df_final) == 0:
                            st.error("Sem caixas para finalizar.")
                            st.stop()

                        qtd_caixas = int(len(df_final))
                        qtd_pecas = int(df_final["qtde_pecas"].sum()) if "qtde_pecas" in df_final.columns else 0
                        now_iso = get_now_utc()

                        res_rom = supabase.table("romaneios_pavuna").insert(
                            {
                                "usuario_criou": st.session_state["user_email"],
                                "unidade_origem": "CD Pavuna",
                                "status": "Encerrado",
                                "romaneios_origem": st.session_state.get("pavuna_saida_roms_origem", []),
                                "qtd_caixas": qtd_caixas,
                                "qtd_pecas": qtd_pecas,
                                "criado_em": now_iso,
                                "data_encerramento": now_iso,
                            }
                        ).execute()

                        romaneio_id_pavuna = res_rom.data[0]["id"]

                        payload_itens = []
                        for _, r in df_final.iterrows():
                            payload_itens.append({
                                "romaneio_pavuna_id": int(romaneio_id_pavuna),
                                "caixa": str(r.get("caixa", "")),
                                "filial_origem": str(r.get("filial_origem", "")),
                                "destino": str(r.get("destino", "")),
                                "qtde_pecas": int(r.get("qtde_pecas", 0) or 0),
                                "criado_em": now_iso,
                            })

                        if payload_itens:
                            supabase.table("romaneios_pavuna_itens").insert(payload_itens).execute()

                        st.session_state["print_romaneio_pavuna_id"] = romaneio_id_pavuna
                        st.success(f"✅ Romaneio Pavuna #{romaneio_id_pavuna} finalizado.")
                        st.rerun()

                else:
                    st.info("Nenhuma caixa carregada ainda.")

    # =========================
    # BASE DE DADOS
    # =========================
    with tab_base:
        st.title("📊 Consulta e Reimpressão")

        with st.container(border=True):
            c1, c2, c3, c4 = st.columns(4)
            f_rom = c1.text_input("Pesquisar Nº Romaneio", key="filter_rom")
            f_caixa = c2.text_input("Pesquisar CAIXA", key="filter_caixa")
            dt_ini = c3.date_input("Início", value=None)
            dt_fim = c4.date_input("Fim", value=None)
            btn_search = st.button("🔍 Pesquisar")

        if btn_search or f_rom or f_caixa:
            q = supabase.table("conferencia_reserva").select("*, romaneios(*)")

            if f_rom and f_rom.isdigit():
                q = q.eq("romaneio_id", int(f_rom))

            if f_caixa:
                q = q.ilike("chave_nfe", f"%{normalize_chave(f_caixa)}%")

            if dt_ini:
                q = q.gte("data_expedicao", dt_ini.strftime("%Y-%m-%d"))

            if dt_fim:
                q = q.lte("data_expedicao", dt_fim.strftime("%Y-%m-%d"))

            res = q.order("data_expedicao", desc=True).execute()

            if res.data:
                df = pd.json_normalize(res.data)

                df = formatar_coluna_datetime(
                    df,
                    [
                        "data_expedicao",
                        "data_recebimento",
                        "romaneios.criado_em",
                        "romaneios.data_encerramento",
                    ]
                )

                colunas_remover = ["volumes"]
                df = df.drop(columns=[c for c in colunas_remover if c in df.columns], errors="ignore")

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
                    "chave_nfe": "CAIXA",
                    "destino": "DESTINO",
                    "romaneio_id": "ROMANEIO",
                    "data_expedicao": "DATA_EXPEDICAO",
                    "data_recebimento": "DATA_RECEBIMENTO",
                    "romaneios.usuario_criou": "USUARIO_CRIADOR",
                    "romaneios.unidade_origem": "UNIDADE_ORIGEM",
                    "romaneios.status": "STATUS_ROMANEIO",
                    "romaneios.criado_em": "CRIADO_EM",
                    "romaneios.data_encerramento": "DATA_ENCERRAMENTO_ROMANEIO",
                }
                df = df.rename(columns=rename_map)

                st.dataframe(df, width="stretch")

                if f_rom and f_rom.isdigit():
                    rid = int(f_rom)
                    st.divider()
                    st.warning("A exclusão remove o romaneio e todos os volumes vinculados.")
                    confirmar_exclusao = st.checkbox("Confirmo que desejo excluir este romaneio permanentemente")

                    col_a, col_b = st.columns(2)

                    with col_a:
                        if st.button("📥 Reimprimir Romaneio", key="btn_reprint_rom"):
                            info_rom = buscar_status_romaneio(rid)
                            if not info_rom:
                                st.error("❌ Romaneio não encontrado.")
                            else:
                                status_rom = info_rom.get("status", "")
                                origem_rom = info_rom.get("unidade_origem", "")

                                if origem_rom == "CD Reserva" and status_rom != "Encerrado":
                                    st.error("❌ Romaneio não encerrado. Favor finalizá-lo para concluir a operação.")
                                else:
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

                    with col_b:
                        if st.button("🗑️ Excluir Romaneio", type="secondary", key="btn_delete_rom"):
                            if not confirmar_exclusao:
                                st.warning("Marque a confirmação para excluir o romaneio.")
                            else:
                                ok, erro = excluir_romaneio_completo(rid)
                                if ok:
                                    st.success(f"✅ Romaneio #{rid} excluído com sucesso.")
                                else:
                                    st.error(f"❌ Erro ao excluir romaneio #{rid}: {erro}")
            else:
                st.warning("Nenhum registro encontrado.")