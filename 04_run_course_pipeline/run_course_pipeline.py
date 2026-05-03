# ============================================================
# 04 — RUN COURSE PIPELINE
# ============================================================
# What it does: Runs scripts 02 and 03 in sequence to keep
#               all course data current in one command.
#               Order: Catalogue (02) → Batches + Master (03)
#
# Why we need it: Power BI reads silver.course_master which
#                 needs BOTH catalogue data (script 02) AND
#                 batch data (script 03) to be current first.
#                 This script runs them together automatically.
#
# How to run:
#   python 04_run_course_pipeline/run_course_pipeline.py
#
# Schedule: Run this daily at 7:00 AM IST.
#
# What to check after:
#   - Both steps should say SUCCESS
#   - silver.course_master should have rows
#   - Power BI should show fresh data after refresh
# ============================================================

import subprocess
import sys
import time


def run_script(script_path, script_name):
    # Run a Python script as a separate process using the same Python interpreter
    print("--- Running: " + script_name + " ---")
    print("")

    # subprocess.run() runs the script and waits for it to finish
    # sys.executable gives us the full path to the current Python interpreter
    result = subprocess.run(
        [sys.executable, script_path],
        # capture_output=False means the script's print statements appear in our terminal
        capture_output=False,
    )

    print("")
    # returncode of 0 means success; anything else means an error occurred
    if result.returncode == 0:
        print("  " + script_name + " completed successfully.")
    else:
        print("  " + script_name + " FAILED (exit code " + str(result.returncode) + ").")

    return result.returncode == 0


def main():
    # Record the start time so we can report how long the whole pipeline took
    start_time = time.time()

    print("=" * 45)
    print("COURSE DATA PIPELINE")
    print("=" * 45)
    print("")

    # Step 1: Fetch the course catalogue (what courses exist)
    catalogue_ok = run_script(
        "02_fetch_course_catalogue/fetch_course_catalogue.py",
        "Course Catalogue"
    )

    # Step 2: Fetch course batches and rebuild the master table
    batches_ok = run_script(
        "03_fetch_course_batches/fetch_course_batches.py",
        "Course Batches + Master Table"
    )

    # Calculate how many seconds the pipeline took
    elapsed_seconds = time.time() - start_time

    print("")
    print("  COURSE PIPELINE COMPLETE")
    print("  " + "-" * 41)
    if catalogue_ok:
        print("  Catalogue API     : SUCCESS")
    else:
        print("  Catalogue API     : FAILED")
    if batches_ok:
        print("  Batches API       : SUCCESS")
    else:
        print("  Batches API       : FAILED")
    if catalogue_ok and batches_ok:
        print("  Power BI ready    : YES")
    else:
        print("  Power BI ready    : NO — check errors above")
    print("  Run time          : " + str(round(elapsed_seconds, 1)) + "s")
    print("  " + "-" * 41)
    print("  Next run: schedule daily at 7:00 AM IST")
    print("")


if __name__ == "__main__":
    main()
