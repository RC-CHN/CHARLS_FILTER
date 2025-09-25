import pandas as pd
import os
from functools import reduce
import sys

# --- Configuration ---
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1200)

# --- File & Directory Paths ---
OUTPUT_DIR = 'processed_data'
# Updated to reflect the new plan: using 2013, 2015, and 2018 data.
FILE_MAPPING = {
    '2013': {
        'demo': 'Demographic_Background.dta',
        'biomarker': 'Biomarker.dta', 
        'health_status': 'Health_Status_and_Functioning.dta',
        'health_care': 'Health_Care_and_Insurance.dta'
    },
    '2015': {
        'demo': 'Demographic_Background.dta',
        'biomarker': 'Biomarker.dta',
        'health_status': 'Health_Status_and_Functioning.dta',
        'health_care': 'Health_Care_and_Insurance.dta'
    },
    '2018': {
        'demo': 'Demographic_Background.dta',
        'health_status': 'Health_Status_and_Functioning.dta',
        'health_care': 'Health_Care_and_Insurance.dta'
        # Note: 2018 does not have a biomarker file in the provided list.
        # This is acceptable for finding common IDs.
    }
}

# --- Main Functions ---
def get_merged_dataframe_for_year(year: str) -> pd.DataFrame | None:
    """Loads and merges all key datasets for a specific year into a single DataFrame."""
    print(f"\n{'='*20} Processing Year: {year} {'='*20}")
    
    base_dir = year
    year_files_map = FILE_MAPPING.get(year, {})
    
    dataframes = []
    for file_type, filename in year_files_map.items():
        path = os.path.join(base_dir, filename)
        if not os.path.exists(path):
            print(f"Warning: File not found and will be skipped: {path}")
            continue
        
        try:
            print(f"Loading data from: {path}")
            df = pd.read_stata(path)
            
            if 'ID' in df.columns:
                df.rename(columns={'ID': 'id'}, inplace=True)
            elif 'id' not in df.columns:
                print(f"Error: No 'ID' or 'id' column in {path}. Skipping file.")
                continue
            
            df['id'] = df['id'].astype(str).str.strip()
            dataframes.append(df)
            print(f" -> Loaded {len(df)} records.")
        except Exception as e:
            print(f"An error occurred while loading {path}: {e}")
            continue
            
    if not dataframes:
        print(f"Failed to load any data for year {year}.")
        return None

    print(f"\nMerging {len(dataframes)} dataframes for year {year}...")
    merged_df = reduce(lambda left, right: pd.merge(
        left, 
        right[[col for col in right.columns if col not in left.columns or col == 'id']], 
        on='id', 
        how='left'
    ), dataframes)
    print(f" -> Merge complete. Shape: {merged_df.shape}")
    return merged_df

if __name__ == "__main__":
    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    # Step 1: Load and merge data for the new set of years
    df_2013 = get_merged_dataframe_for_year('2013')
    df_2015 = get_merged_dataframe_for_year('2015')
    df_2018 = get_merged_dataframe_for_year('2018')

    if df_2013 is None or df_2015 is None or df_2018 is None:
        print("\nAborting due to failure in loading/merging data for one or more years.")
    else:
        # Step 2: Find the common set of participant IDs across the three waves
        print("\nFinding common participants across 2013, 2015, and 2018...")
        ids_2013 = set(df_2013['id'])
        ids_2015 = set(df_2015['id'])
        ids_2018 = set(df_2018['id'])
        
        common_ids = ids_2013.intersection(ids_2015).intersection(ids_2018)
        print(f" -> Found {len(common_ids)} participants present in all three waves.")

        if not common_ids:
            print("\nError: No common participants found. Aborting.")
            sys.exit(1) # Exit the script with an error code
        else:
            # Step 3: Filter each dataframe
            panel_2013 = df_2013[df_2013['id'].isin(common_ids)].copy()
            panel_2015 = df_2015[df_2015['id'].isin(common_ids)].copy()
            panel_2018 = df_2018[df_2018['id'].isin(common_ids)].copy()

            # Step 4: Save the resulting panel dataframes as DTA files
            print("\nSaving panel data to .dta files in 'processed_data/' directory...")
            try:
                # Using version 118 which corresponds to Stata 14, offering better encoding support.
                stata_version = 118
                panel_2013.to_stata(os.path.join(OUTPUT_DIR, 'panel_2013.dta'), write_index=False, version=stata_version)
                print(" -> Successfully saved panel_2013.dta")
                panel_2015.to_stata(os.path.join(OUTPUT_DIR, 'panel_2015.dta'), write_index=False, version=stata_version)
                print(" -> Successfully saved panel_2015.dta")
                panel_2018.to_stata(os.path.join(OUTPUT_DIR, 'panel_2018.dta'), write_index=False, version=stata_version)
                print(" -> Successfully saved panel_2018.dta")
                
                print("\n--- Longitudinal Data Preparation Complete (2013-2018) ---")
            except Exception as e:
                print(f"\nAn error occurred while saving the DTA files: {e}")