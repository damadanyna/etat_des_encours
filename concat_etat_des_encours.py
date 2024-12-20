import os
import pandas as pd

# Directory containing the Excel files
directory = "./etat_des_encours"  # Replace with your folder path

# List to store the data from all files
dataframes = []

# Loop through all files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".xlsx") and filename.startswith("output_offset_"):
        file_path = os.path.join(directory, filename)
        # Read the Excel file into a DataFrame
        df = pd.read_excel(file_path)
        dataframes.append(df)

# Concatenate all the DataFrames
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the combined DataFrame to a new Excel file
output_file = os.path.join(directory, "combined_output.xlsx")
combined_df.to_excel(output_file, index=False)

print(f"All tables have been concatenated and saved to {output_file}")
