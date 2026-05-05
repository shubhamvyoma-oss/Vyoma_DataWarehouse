# Import the json module to handle reading and writing JSON files
import json
# Import the csv module to handle reading and writing CSV files
import csv
# Import the os module to work with file paths and folders
import os
# Import the datetime class to get the current date and time
from datetime import datetime

# This function creates a folder path where we can save our data files
def create_directory_path(category_name):
    # Get the path to the current folder where this script is running
    # We do this to know where to start building our new folder path
    current_working_directory = os.getcwd()
    
    # Combine the current folder path with 'local_storage', 'raw', and the category name
    # We use os.path.join so it works correctly on both Windows and Linux
    folder_path = os.path.join(current_working_directory, 'local_storage', 'raw', category_name)
    
    # Create the folder and any missing parent folders in the path
    # We set exist_ok to True so the script does not crash if the folder already exists
    os.makedirs(folder_path, exist_ok=True)
    
    # Return the full folder path so other parts of the program can use it
    return folder_path

# This function creates a unique filename using a name and the current time
def generate_unique_filename(file_name_prefix, file_type_extension):
    # Get the current date and time from the computer
    # This helps us make every filename unique so we do not overwrite old files
    current_date_and_time = datetime.now()
    
    # Convert the date and time into a simple text string like 20231027_153045
    # YearMonthDay_HourMinuteSecond format is easy to read and sort
    time_string = current_date_and_time.strftime("%Y%m%d_%H%M%S")
    
    # Combine the prefix, the time string, and the file extension together
    # For example: attendance_20231027_153045.json
    final_file_name = file_name_prefix + "_" + time_string + "." + file_type_extension
    
    # Return the finished filename
    return final_file_name

# This function saves data into a JSON file
def save_data_as_json(data_to_save, full_path_to_file):
    # We use a try block to catch any errors that might happen during saving
    # This prevents the whole program from crashing if a file cannot be written
    try:
        # Open the file in 'w' mode which stands for writing
        # We use utf-8 encoding to make sure special characters are handled correctly
        with open(full_path_to_file, 'w', encoding='utf-8') as json_output_file:
            # Write the data into the file in a format called JSON
            # We use indent=4 to make the file look "pretty" and easy for humans to read
            json.dump(data_to_save, json_output_file, indent=4, default=str)
    # If an error happens, the program jumps here
    except Exception as error:
        # Print a message telling the user what went wrong
        # We convert the error to a string so it can be printed easily
        print("An error occurred while saving JSON file: " + str(error))

# This function saves a list of information into a CSV file
def save_data_as_csv(data_list_to_save, full_path_to_file):
    # Check if the list has any data inside it
    # We cannot save an empty list because there would be no columns or rows
    if len(data_list_to_save) > 0:
        # Use try to handle any errors during the file saving process
        try:
            # Take the first item in the list to find the names of the columns
            # This assumes all items in the list have the same columns
            first_data_item = data_list_to_save[0]
            column_header_names = first_data_item.keys()
            
            # Open the file for writing
            # newline='' is used to prevent extra blank lines between rows in the CSV
            with open(full_path_to_file, 'w', newline='', encoding='utf-8') as csv_output_file:
                # Create a helper object that knows how to write dictionaries to CSV
                csv_file_writer = csv.DictWriter(csv_output_file, fieldnames=column_header_names)
                
                # Write the first row which contains the names of the columns
                csv_file_writer.writeheader()
                
                # Write all the data rows from our list into the file
                csv_file_writer.writerows(data_list_to_save)
        # If something goes wrong, print the error
        except Exception as error:
            # Show the error message to the user
            print("An error occurred while saving CSV file: " + str(error))

# This is the main function that coordinates saving data to the local computer
def save_data_to_local_file(data_content, folder_category, name_prefix, file_format='json'):
    # Get the correct folder path for this category
    # This ensures files are organized into folders like 'attendance' or 'batches'
    target_folder_path = create_directory_path(folder_category)
    
    # Create a unique filename for the new data
    # This prevents us from accidentally deleting old data by using the same name
    unique_file_name = generate_unique_filename(name_prefix, file_format)
    
    # Combine the folder path and the filename to get the complete path
    full_destination_path = os.path.join(target_folder_path, unique_file_name)
    
    # Check if the user wants to save the data in JSON format
    if file_format == 'json':
        # Call our JSON saving function
        save_data_as_json(data_content, full_destination_path)
    # Check if the user wants to save the data in CSV format
    elif file_format == 'csv':
        # Call our CSV saving function
        save_data_as_csv(data_content, full_destination_path)
    
    # Print a message so the user knows where the file was saved
    print("[Local Storage] Saved data to: " + full_destination_path)
    
    # Return the full path so the rest of the program knows where the file is
    return full_destination_path
