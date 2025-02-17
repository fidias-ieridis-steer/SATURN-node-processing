import pandas as pd
import numpy as np
import glob
import os
import openpyxl
import xlwings as xw


excel_file_path  = 'Junction Coding Template V2.1 - Reformated with examples.xlsm'
# excel_file_path = 'Junction Coding Template V2.1 - Reformated with outputs.xlsm'
# wb = openpyxl.load_workbook(excel_file)
# Load the Excel file
excel_file = pd.ExcelFile(excel_file_path)

# Get the sheet names
sheet_names = excel_file.sheet_names
print(sheet_names)
junctions = [j for j in sheet_names if j.isdigit()]
# read individual ouput files (infer node/sheet name from filename)

# Replace 'file_path.xlsm' with your actual file path
sheet_name = '1003'  # Replace with the actual sheet name if necessary
folder_path = os.getcwd()

# Starting cell in Excel
start_row = 21
start_column = 41  # Column AO is the 41st column
new_excel_file = 'Junction Coding Template V2.1 - Reformated with outputs.xlsm'  # Specify the new name for the saved file
sheet_name = junctions[2]
# Read the .xlsm file
for sheet_name in junctions:
    # Write content to Excel starting from AO21
    start_row = 21
    start_column = 'AO'

    file_path = os.path.join(folder_path, f'output_{sheet_name}.txt')
    output_file = f'filtered_output_{sheet_name}.txt'
    # Initialize variables
    content_to_save = []
    count = 0

    with open(file_path, 'r') as file:
        for line in file:
            # Check if the line contains '1003' as the start of a new block
            if line.strip().startswith(sheet_name):
                count += 1
            # Start saving lines once the third occurrence of '1003' is reached
            if count >= 3:
                content_to_save.append(line)
        print(f"Contents of {file_path}:\n{content_to_save}\n")

    with open(output_file,'w') as file:
        file.writelines(content_to_save)
        # content = file.readlines()

    try:
        if sheet_name == junctions[0]:
            app = xw.App(visible=False)
            wb = app.books.open(excel_file_path)
        wb_sheet = wb.sheets[sheet_name]   # Select the active sheet or specify by name, e.g., wb.sheets['SheetName']

        # Read the content from the output file
        with open(output_file, 'r') as file:
            content = file.readlines()

        for i, line in enumerate(content):
            cell_address = f"{start_column}{start_row + i}"
            wb_sheet.range(cell_address).value = line.strip()  # Stripping newline characters if needed

        # Save the workbook
        # wb.save(new_excel_file)
        print(f"Content successfully pasted into {sheet_name} in  {new_excel_file} starting from AO21.")
        # wb.close()
        # app.quit()
    except Exception as e:
        # Handle errors if any
        print("An error occurred:", e)
    # finally:
        # Close the workbook and quit the app
        # wb.save(new_excel_file)

        # wb.close()
        # app.quit()
    # break

    # break
wb.save(new_excel_file)
wb.close()
app.quit()
#

# read spreadsheet and all sheet names


# paste the output part of the txt file into the respective cell of the respective sheet

