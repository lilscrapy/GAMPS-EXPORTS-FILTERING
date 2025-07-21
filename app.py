import streamlit as st
import pandas as pd
import asyncio
import aiohttp
from openai import OpenAI
import io
import zipfile
from dotenv import load_dotenv
import os

load_dotenv()

# ==================================
# Configuration de la Page
# ==================================
st.set_page_config(
    page_title="GMaps File Cleaner",
    page_icon="🧹",
    layout="wide"
)

# ==================================
# Authentification
# ==================================

def check_password():
    """Retourne True si l'utilisateur est authentifié."""
    try:
        correct_password = st.secrets["APP_PASSWORD"]
    except (KeyError, st.errors.StreamlitAPIException):
        st.error("Application password is not configured in secrets.")
        return False

    if st.session_state.get("password_entered") != True:
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            if password == correct_password:
                st.session_state["password_entered"] = True
                st.rerun()
            else:
                st.error("The password you entered is incorrect.")
        return False
    else:
        return True

# ==================================
# Fonctions Principales
# ==================================

CATEGORY_COL = "category"

def build_prompt(category, target_keyword):
    return [
        {"role": "user", "content": f"Is the following business category likely related to a **{target_keyword}**? Only reply 'yes' or 'no'.\n\nCategory: {category}"}
    ]

async def classify_category(session, semaphore, category, api_key, target_keyword):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "gpt-4",
        "messages": build_prompt(category, target_keyword),
        "temperature": 0
    }

    async with semaphore:
        try:
            async with session.post(url, json=payload, headers=headers, timeout=30) as resp:
                data = await resp.json()
                if "choices" not in data or not data["choices"]:
                    error_msg = data.get("error", {}).get("message", "Unknown API error")
                    st.toast(f"⚠️ API Error: {error_msg} for category: {category}", icon="🚨")
                    return category, False, "error_api"
                reply = data["choices"][0]["message"]["content"].strip().lower()
                is_relevant = "yes" in reply
                return category, is_relevant, reply
        except Exception as e:
            st.toast(f"⚠️ Error: {e} for category: {category}", icon="🚨")
            return category, False, "error"

# ==================================
# Fonctions d'Export
# ==================================

def prepare_and_set_download_file(df_to_export, do_batch, rows_per_file, base_filename):
    """Prépare les données du fichier pour le téléchargement et les stocke dans st.session_state."""
    if do_batch and rows_per_file > 0:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            num_chunks = (len(df_to_export) - 1) // rows_per_file + 1
            for i in range(num_chunks):
                start_row = i * rows_per_file
                end_row = start_row + rows_per_file
                chunk_df = df_to_export.iloc[start_row:end_row]
                file_name = f"results_batch_{i+1}.csv"
                zip_file.writestr(file_name, chunk_df.to_csv(index=False).encode('utf-8'))
        
        st.session_state.download_file_bytes = zip_buffer.getvalue()
        st.session_state.download_file_name = f"{base_filename}.zip"
        st.session_state.download_file_mime = "application/zip"
    else:
        csv_bytes = df_to_export.to_csv(index=False).encode('utf-8')
        st.session_state.download_file_bytes = csv_bytes
        st.session_state.download_file_name = f"{base_filename}.csv"
        st.session_state.download_file_mime = "text/csv"
    
    st.success("File is ready!")
    st.rerun()

def display_export_ui(df_for_export, keyword_for_filename):
    """Affiche l'interface utilisateur pour l'exportation et gère la préparation du fichier."""
    st.header("4. Export")
    st.write(f"The final file contains **{len(df_for_export)}** rows.")

    do_batch = st.toggle("Split file into multiple batches", key=f"batch_toggle_{keyword_for_filename}")
    rows_per_file = 0
    if do_batch:
        rows_per_file = st.number_input("Max rows per file", min_value=1, value=40000, step=1000, key=f"rows_input_{keyword_for_filename}")
    
    if st.button("Prepare Download", type="primary", key=f"prepare_download_{keyword_for_filename}"):
        base_filename = f"filtered_results_{keyword_for_filename.replace(' ', '_')}"
        prepare_and_set_download_file(df_for_export, do_batch, rows_per_file, base_filename)

# ==================================
# Interface Utilisateur (UI)
# ==================================

def on_ai_toggle_change():
    """Nettoie le session state lorsque le toggle AI est changé."""
    keys_to_clear = [
        'relevant_categories', 'df_classified', 'target_keyword', 'final_df', 
        'download_file_bytes', 'download_file_name', 'download_file_mime'
    ]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]

if check_password():
    st.title("🧹 GMaps File Cleaner")
    st.subheader("Filter your Google Maps exports with AI assistance")

    with st.sidebar:
        st.header("1. Settings")

        api_key = os.getenv("OPENAI_API_KEY") or ""
        if not api_key:
            try:
                if 'OPENAI_API_KEY' in st.secrets:
                    api_key = st.secrets['OPENAI_API_KEY']
            except st.errors.StreamlitAPIException:
                api_key = ""

        api_key_input = st.text_input(
            "OpenAI API Key",
            type="password",
            value=api_key,
            help="Your API key will not be stored publicly. You can set it in a .env file to avoid entering it manually."
        )
        
        uploaded_file = st.file_uploader(
            "Upload CSV file to analyze", 
            type=['csv']
        )

    if uploaded_file is not None and api_key_input:
        try:
            df = pd.read_csv(uploaded_file)
            
            if CATEGORY_COL not in df.columns:
                st.error(f"Error: Column '{CATEGORY_COL}' not found in your file. Please check the column name.")
            else:
                st.success(f"File '{uploaded_file.name}' uploaded successfully. It contains {len(df)} rows.", icon="✅")
                
                df_current = df.copy()

                if 'rating' in df_current.columns and 'ratingCount' in df_current.columns:
                    with st.expander("⭐ Optional Pre-filters (rating and rating count)", expanded=True):
                        col1, col2 = st.columns(2)
                        with col1:
                            min_rating = st.number_input("Minimum rating (e.g., 4.0)", min_value=0.0, max_value=5.0, step=0.1, value=0.0)
                        with col2:
                            min_rating_count = st.number_input("Minimum rating count (e.g., 50)", min_value=0, step=1, value=0)
                        
                        original_rows = len(df_current)

                        if min_rating > 0:
                            df_current = df_current[pd.to_numeric(df_current['rating'], errors='coerce').fillna(0) >= min_rating]
                        if min_rating_count > 0:
                            df_current = df_current[pd.to_numeric(df_current['ratingCount'], errors='coerce').fillna(0) >= min_rating_count]
                        
                        st.metric(label="Rows remaining after pre-filtering", value=f"{len(df_current)}", delta=f"{len(df_current) - original_rows} rows")
                
                # --- NOUVELLE SECTION : TOGGLE POUR L'IA ---
                st.header("2. AI-Powered Filtering")
                use_ai_filtering = st.toggle(
                    "Enable AI category filtering", 
                    key='use_ai_filtering_toggle',
                    on_change=on_ai_toggle_change
                )

                if use_ai_filtering:
                    # --- WORKFLOW AVEC IA ---
                    st.header("3. Search Criteria")
                    target_keyword = st.text_input(
                        "Enter your search criteria",
                        placeholder="E.g., 'medical weight loss clinic'",
                        help="The AI will look for categories related to this keyword."
                    )

                    if st.button("Start Analysis and Classification", type="primary"):
                        if not target_keyword:
                            st.warning("Please enter a search criteria.")
                        else:
                            # Nettoyage du state avant une nouvelle analyse
                            on_ai_toggle_change()
                            if 'relevant_categories' in st.session_state:
                                for cat in st.session_state.relevant_categories:
                                    if f'cb_{cat}' in st.session_state:
                                        del st.session_state[f'cb_{cat}']
                            
                            unique_categories = df_current[CATEGORY_COL].dropna().unique()
                            st.session_state.target_keyword = target_keyword

                            progress_bar = st.progress(0, text=f"🔍 Analyzing {len(unique_categories)} unique categories...")

                            async def run_classification():
                                results = {}
                                semaphore = asyncio.Semaphore(10)
                                async with aiohttp.ClientSession() as session:
                                    tasks = [classify_category(session, semaphore, cat, api_key_input, target_keyword) for cat in unique_categories]
                                    
                                    processed_count = 0
                                    for f in asyncio.as_completed(tasks):
                                        cat, is_relevant, reply = await f
                                        results[cat] = is_relevant
                                        processed_count += 1
                                        progress_bar.progress(processed_count / len(unique_categories), text=f"🔍 Analysis: {processed_count}/{len(unique_categories)} categories processed...")
                                
                                return results

                            results = asyncio.run(run_classification())
                            
                            progress_bar.empty()
                            
                            result_col_name = f"is_{target_keyword.replace(' ', '_').lower()}"
                            df_current[result_col_name] = df_current[CATEGORY_COL].map(results)
                            
                            relevant_df = df_current[df_current[result_col_name] == True]
                            kept_categories = relevant_df[CATEGORY_COL].dropna().unique().tolist()
                            
                            if kept_categories:
                                st.session_state.df_classified = df_current
                                st.session_state.relevant_categories = kept_categories
                                st.success(f"{len(kept_categories)} relevant categories found!", icon="🎉")
                                st.rerun()
                            else:
                                st.warning("No relevant categories were found for this criteria.")
                else:
                    # --- WORKFLOW SANS IA (EXPORT DIRECT) ---
                    st.session_state.final_df = df_current
                    st.session_state.target_keyword = "prefiltered"


        except Exception as e:
            st.error(f"An error occurred while reading the file: {e}")

    if 'relevant_categories' in st.session_state and st.session_state.relevant_categories:
        st.header("3. Refine Your Selection")

        def select_all():
            for cat in st.session_state.relevant_categories:
                st.session_state[f'cb_{cat}'] = True

        def deselect_all():
            for cat in st.session_state.relevant_categories:
                st.session_state[f'cb_{cat}'] = False

        col1, col2, _ = st.columns([1, 1, 3])
        with col1:
            st.button("Select All", on_click=select_all, use_container_width=True)
        with col2:
            st.button("Deselect All", on_click=deselect_all, use_container_width=True)

        st.write("Uncheck the categories you do NOT want to keep in the final file.")
        
        with st.form(key='selection_form'):
            for cat in st.session_state.relevant_categories:
                st.checkbox(cat, key=f'cb_{cat}', value=st.session_state.get(f'cb_{cat}', True))
            
            submitted = st.form_submit_button("Generate File")

            if submitted:
                final_selected_categories = [
                    cat for cat in st.session_state.relevant_categories 
                    if st.session_state.get(f'cb_{cat}')
                ]
                
                if not final_selected_categories:
                    st.error("You have not selected any category.")
                else:
                    final_df = st.session_state.df_classified[st.session_state.df_classified[CATEGORY_COL].isin(final_selected_categories)]
                    st.session_state.final_df = final_df
                    
                    if 'download_file_bytes' in st.session_state:
                        del st.session_state['download_file_bytes']

                    st.rerun()

    if 'final_df' in st.session_state:
        display_export_ui(st.session_state.final_df, st.session_state.get('target_keyword', 'results'))

    if 'download_file_bytes' in st.session_state:
        st.download_button(
           label="⬇️ Download File",
           data=st.session_state.download_file_bytes,
           file_name=st.session_state.download_file_name,
           mime=st.session_state.download_file_mime,
        )

else:
    st.info("Please provide your API key and upload a CSV file to begin.", icon="💡")
