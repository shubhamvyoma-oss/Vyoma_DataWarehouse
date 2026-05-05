# Import the subprocess tool to run other Python scripts
import subprocess
# Import the sys tool to find the current Python program path
import sys
# Import the time tool to measure how long the scripts take to run
import time
# Import the os tool to handle file and folder paths
import os

# This function runs a single Python script and waits for it to finish
def run_pipeline_step(script_file_name, step_label_name):
    # Print a message to show which step is starting
    print("--- Starting Pipeline Step: " + step_label_name + " ---")
    
    # Get the folder where this current script is located
    current_script_folder = os.path.dirname(os.path.abspath(__file__))
    
    # Combine the folder path with the script name to get the full path
    # This makes sure we find the script correctly regardless of where we start
    full_script_path = os.path.join(current_script_folder, script_file_name)
    
    # Run the script using the same Python program that is running this one
    # subprocess.run waits until the script finishes before continuing
    result_of_run = subprocess.run([sys.executable, full_script_path])
    
    # Check the return code to see if the script finished successfully
    # A return code of 0 means everything went fine
    if result_of_run.returncode == 0:
        # Print a success message
        print("Completed Successfully: " + step_label_name)
        # Return True to show it worked
        return True
    # If the return code is not 0, something went wrong
    else:
        # Print a failure message
        print("FAILED: " + step_label_name)
        # Return False to show it failed
        return False

# This function prints a final summary of all the pipeline steps
def print_pipeline_summary(catalogue_status, batches_status, duration_seconds):
    # Print a header for the summary section
    print("\n================================")
    print("      PIPELINE FINAL SUMMARY     ")
    print("================================")
    
    # Print the status of the Catalogue step
    if catalogue_status == True:
        print("Catalogue Fetch: SUCCESS")
    else:
        print("Catalogue Fetch: FAILED")
        
    # Print the status of the Batches step
    if batches_status == True:
        print("Batches Fetch:   SUCCESS")
    else:
        print("Batches Fetch:   FAILED")
        
    # Round the total time to one decimal place and print it
    rounded_time = round(duration_seconds, 1)
    print("Total Time Taken: " + str(rounded_time) + " seconds")
    print("================================\n")

# This is the main function that coordinates the whole process
def main():
    # Record the exact time the pipeline started
    start_time_stamp = time.time()
    
    # Print a message to show the whole process has started
    print("COURSE DATA PIPELINE STARTED")
    
    # Step 1: Run the script that fetches the course catalogue
    # We save the result (True or False) in a variable
    is_catalogue_ok = run_pipeline_step("fetch_course_catalogue.py", "Course Catalogue")
    
    # Step 2: Run the script that fetches the course batches
    # This step will run after the first one finishes
    is_batches_ok = run_pipeline_step("fetch_course_batches.py", "Course Batches")
    
    # Calculate how many seconds have passed since we started
    total_duration = time.time() - start_time_stamp
    
    # Call the function to print the final report
    print_pipeline_summary(is_catalogue_ok, is_batches_ok, total_duration)

# Check if this file is being run directly by the user
if __name__ == "__main__":
    # Start the main process
    main()
