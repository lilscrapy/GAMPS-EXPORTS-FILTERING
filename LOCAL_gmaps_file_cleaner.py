# Google Maps File Cleaner - Local Version
# This script filters CSV files based on business categories using OpenAI GPT-4
# 
# Usage:
# 1. Install required packages: pip install pandas asyncio aiohttp tqdm openpyxl
# 2. Run the script: python gmaps_file_cleaner_local.py
# 3. Follow the prompts to upload your file, enter API key, and define search criteria

import pandas as pd
import asyncio
import aiohttp
from tqdm import tqdm
import os
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading

CATEGORY_COL = "category"  # Make sure this is the correct column name

# === GPT PROMPT ===
def build_prompt(category, target_keyword):
    return [
        {"role": "user", "content": f"Is the following business category likely related to a **{target_keyword}**? Only reply 'yes' or 'no'.\n\nCategory: {category}"}
    ]

# === ASYNC GPT CALL ===
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
                    print(f"⚠️  API Error: {error_msg} for category: {category}")
                    return category, False, "error_api"
                reply = data["choices"][0]["message"]["content"].strip().lower()
                is_relevant = "yes" in reply
                return category, is_relevant, reply
        except Exception as e:
            print(f"⚠️  Error: {e} for category: {category}")
            return category, False, "error"

# === GUI CATEGORY SELECTION ===
class CategorySelector:
    def __init__(self, categories):
        self.categories = categories
        self.selected_categories = []
        self.result = {'selected': None, 'confirmed': False}
        
        # Create main window
        self.root = tk.Tk()
        self.root.title("Category Selection")
        self.root.geometry("600x500")
        
        # Create main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title
        title_label = ttk.Label(main_frame, text=f"Select categories to keep ({len(categories)} found):", font=("Arial", 12, "bold"))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 10))
        
        # Buttons frame
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=1, column=0, columnspan=3, pady=(0, 10))
        
        # Action buttons
        select_all_btn = ttk.Button(button_frame, text="Select All", command=self.select_all)
        select_all_btn.pack(side=tk.LEFT, padx=5)
        
        deselect_all_btn = ttk.Button(button_frame, text="Deselect All", command=self.deselect_all)
        deselect_all_btn.pack(side=tk.LEFT, padx=5)
        
        confirm_btn = ttk.Button(button_frame, text="Confirm Selection", command=self.confirm, style="Accent.TButton")
        confirm_btn.pack(side=tk.LEFT, padx=5)
        
        # Create scrollable frame for checkboxes
        canvas = tk.Canvas(main_frame, height=350)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.grid(row=2, column=0, sticky="nsew")
        scrollbar.grid(row=2, column=1, sticky="ns")
        
        # Configure grid weights
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        
        # Create checkboxes
        self.checkboxes = {}
        for i, category in enumerate(categories):
            var = tk.BooleanVar(value=False)
            checkbox = ttk.Checkbutton(scrollable_frame, text=category, variable=var)
            checkbox.grid(row=i, column=0, sticky="w", padx=5, pady=2)
            self.checkboxes[category] = var
        
        # Bind mouse wheel to canvas
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        # Center window
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() // 2) - (self.root.winfo_width() // 2)
        y = (self.root.winfo_screenheight() // 2) - (self.root.winfo_height() // 2)
        self.root.geometry(f"+{x}+{y}")
    
    def select_all(self):
        for var in self.checkboxes.values():
            var.set(True)
    
    def deselect_all(self):
        for var in self.checkboxes.values():
            var.set(False)
    
    def confirm(self):
        self.selected_categories = [cat for cat, var in self.checkboxes.items() if var.get()]
        self.result['selected'] = self.selected_categories
        self.result['confirmed'] = True
        print(f"\n✅ {len(self.selected_categories)} categories selected.")
        self.root.destroy()
    
    def run(self):
        self.root.mainloop()
        return self.result['selected']

def select_categories_with_gui(categories):
    """
    Allows user to select categories via GUI
    """
    print(f"\n📋 {len(categories)} categories have been marked as relevant by AI.")
    print("A GUI window will open for category selection...")
    
    # Run GUI in main thread
    selector = CategorySelector(categories)
    selected_categories = selector.run()
    
    return selected_categories

# === DRIVER FUNCTION ===
async def process_and_export(df, api_key, input_filename):
    # Ask for output filename
    output_filename = input("\n💾 Enter output filename (e.g., result.csv): ").strip()
    if not output_filename:
        output_filename = f"classified_results_{input_filename}"
    # Force .csv extension if not present
    if not output_filename.lower().endswith('.csv'):
        output_filename += '.csv'

    initial_row_count = len(df)
    print(f"📄 File contains {initial_row_count} rows.")
    rows_after_filtering = initial_row_count

    # --- Optional Filtering ---
    filtered_df = df.copy()
    if 'rating' in filtered_df.columns and 'ratingCount' in filtered_df.columns:
        try:
            temp_df = filtered_df.copy()
            while True:
                rating_input = input("  - Enter minimum rating (e.g., 4.0, leave blank for none): ")
                rating_count_input = input("  - Enter minimum rating count (e.g., 50, leave blank for none): ")

                temp_df = filtered_df.copy()
                if rating_input:
                    min_rating = float(rating_input)
                    temp_df = temp_df[pd.to_numeric(temp_df['rating'], errors='coerce').fillna(0) >= min_rating]
                if rating_count_input:
                    min_rating_count = int(rating_count_input)
                    temp_df = temp_df[pd.to_numeric(temp_df['ratingCount'], errors='coerce').fillna(0) >= min_rating_count]

                print(f"➡️ {len(temp_df)} rows remaining with these filters.")

                confirm = input("Do you want to keep these filters? (yes to confirm, no to try again): ").lower().strip()
                if confirm == 'yes':
                    filtered_df = temp_df
                    break

            rows_after_filtering = len(filtered_df)

        except ValueError:
            print("⚠️ Invalid number format. Skipping rating/count filters.")
        except Exception as e:
            print(f"⚠️ An error occurred during filtering: {e}")

    rows_removed_by_filter = initial_row_count - rows_after_filtering

    # === MANUAL SELECTION QUESTION (moved before search criteria) ===
    manual_selection = input("\n🔧 Do you want to manually select categories to keep? (yes/no): ").strip().lower()
    enable_manual_selection = manual_selection == 'yes'

    target_keyword = input("\n🎯 Please enter the search criteria (e.g., medical weight loss clinic): ")
    if not target_keyword:
        print("❌ Search criteria not provided. Halting script.")
        return

    unique_categories = filtered_df[CATEGORY_COL].dropna().unique()
    print(f"\n🔍 Processing {len(unique_categories)} unique categories from {rows_after_filtering} rows.")

    results = {}
    semaphore = asyncio.Semaphore(50)  # Reduced from 400 to 50 to respect TPM limits

    async with aiohttp.ClientSession() as session:
        tasks = [classify_category(session, semaphore, cat, api_key, target_keyword) for cat in unique_categories]
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Classifying"):
            cat, is_relevant, reply = await f
            results[cat] = is_relevant

    # Merge results and filter to keep only TRUE values
    result_col_name = f"is_{target_keyword.replace(' ', '_').lower()}"
    filtered_df[result_col_name] = filtered_df[CATEGORY_COL].map(results)

    relevant_df = filtered_df[filtered_df[result_col_name] == True]

    # Get categories kept in the final file
    kept_categories = relevant_df[CATEGORY_COL].dropna().unique()
    
    # === MANUAL SELECTION WITH GUI ===
    if len(kept_categories) > 0 and enable_manual_selection:
        selected_categories = select_categories_with_gui(kept_categories)
        if selected_categories:
            # Filter DataFrame to keep only selected categories
            relevant_df = relevant_df[relevant_df[CATEGORY_COL].isin(selected_categories)]
            print(f"\n✅ Manual filtering applied: {len(selected_categories)} categories selected.")
        else:
            print("\n❌ No categories selected. No data will be exported.")
            relevant_df = pd.DataFrame(columns=filtered_df.columns)  # Empty DataFrame
    elif len(kept_categories) > 0:
        print("\n✅ Using all categories marked as relevant by AI.")
    else:
        print("\n⚠️ No categories were marked as relevant by AI.")

    # Save file
    relevant_df.to_csv(output_filename, index=False)

    # --- Final Stats ---
    final_relevant_rows = len(relevant_df)
    print("\n📊 --- Final Statistics ---")
    print(f"Initial rows in file: {initial_row_count}")
    if rows_removed_by_filter > 0:
        print(f"Rows removed by rating/count filter: {rows_removed_by_filter}")
    print(f"Rows removed by category filter: {rows_after_filtering - final_relevant_rows}")
    print(f"Unique categories processed: {len(unique_categories)}")
    print(f"Rows kept after classification ('TRUE'): {final_relevant_rows}")
    print(f"Total rows removed from original file: {initial_row_count - final_relevant_rows}")
    print("--------------------------\n")

    print(f"✅ DONE. The results file '{output_filename}' with {final_relevant_rows} relevant rows has been created.")
    print(f"📁 File saved to: {os.path.abspath(output_filename)}")

async def main():
    print("🚀 Google Maps File Cleaner - Local Version")
    print("=" * 50)
    
    # Get API key
    api_key = input("🔑 Please enter your OpenAI API key: ")
    if not api_key:
        print("❌ API key not provided. Halting script.")
        return

    # File selection using tkinter
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    
    print("\n📤 Please select the CSV file to analyze:")
    input_filename = filedialog.askopenfilename(
        title="Select CSV file",
        filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx"), ("All files", "*.*")]
    )
    
    if not input_filename:
        print("❌ No file selected. Halting script.")
        return

    print(f"📄 File '{os.path.basename(input_filename)}' selected successfully.")

    try:
        # Read file based on extension
        if input_filename.lower().endswith('.csv'):
            df = pd.read_csv(input_filename)
        elif input_filename.lower().endswith('.xlsx'):
            df = pd.read_excel(input_filename)
        else:
            print("❌ Unsupported file format. Please use CSV or Excel files.")
            return
            
        if CATEGORY_COL not in df.columns:
            print(f"❌ Error: Column '{CATEGORY_COL}' not found in the file. Please check the column name.")
            print(f"Available columns: {list(df.columns)}")
            return
    except Exception as e:
        print(f"❌ Error reading the file: {e}")
        return

    while True:
        await process_and_export(df, api_key, os.path.basename(input_filename))
        again = input("\n🔁 Do you want to process another file from the same CSV? (yes/no): ").strip().lower()
        if again != 'yes':
            print("👋 End of script.")
            break

# === RUN ===
if __name__ == "__main__":
    asyncio.run(main()) 