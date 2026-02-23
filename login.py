import streamlit as st
import base64
import os

# Função para converter imagem para o CSS
def get_base64_of_bin_file(bin_file):
    with open(bin_file, 'rb') as f:
        data = f.read()
    return base64.b64encode(data).decode()

# Função que aplica o visual
def apply_login_theme(bg_path):
    if os.path.exists(bg_path):
        bin_str = get_base64_of_bin_file(bg_path)
        st.markdown(f'''
            <style>
            .stApp {{
                background-image: url("data:image/png;base64,{bin_str}");
                background-size: cover;
            }}
            [data-testid="stForm"] {{
                background-color: rgba(255, 255, 255, 0.9);
                border-radius: 15px;
            }}
            </style>
            ''', unsafe_allow_html=True)

# Função que desenha os elementos
def show_login():
    apply_login_theme("Fundo tela login.png")
    
    col_esq, col_meio, col_dir = st.columns([1, 1.2, 1])
    with col_meio:
        st.markdown('<div style="text-align:center; color:white; margin-top:50px;">'
                    '<h1 style="letter-spacing:10px;">AZZAS</h1>'
                    '<p style="letter-spacing:5px; font-size:10px;">FASHION & LIFESTYLE</p>'
                    '</div>', unsafe_allow_html=True)
        
        with st.form("login"):
            st.text_input("E-mail")
            st.selectbox("Unidade", ["CD Reserva", "CD Pavuna"])
            st.form_submit_button("ENTRAR →")

# --- EXECUÇÃO ---
if "auth" not in st.session_state:
    show_login()
else:
    st.write("Você está logado!")