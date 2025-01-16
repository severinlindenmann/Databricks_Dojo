# Databricks notebook source
# MAGIC %md
# MAGIC # get_total_folder_size_recursive
# MAGIC This is a draft and have to be improved

# COMMAND ----------

folder_path = "/Volumes/hub_dev/raw/sap_s4_testing"

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum

def get_total_folder_size_recursive(folder_path):
    """
    Recursively get the total file size (in bytes) of all files in a Databricks folder and its subfolders.

    Parameters:
    folder_path (str): The path to the folder in DBFS (e.g., "dbfs:/path/to/folder")

    Returns:
    float: Total file size in megabytes (MB).
    """
    total_size = 0

    # List files and folders
    files_and_folders = dbutils.fs.ls(folder_path)

    for item in files_and_folders:
        if item.isFile():  # If it's a file, add its size
            total_size += item.size
        else:  # If it's a folder, recurse into it
            total_size += get_total_folder_size_recursive(item.path)

    return total_size

def print_total_folder_size(folder_path):
    """
    Print the total folder size in a human-readable format.
    """
    total_size_bytes = get_total_folder_size_recursive(folder_path)
    total_size_mb = total_size_bytes / (1024 * 1024)  # Convert bytes to MB
    print(f"Total size of folder '{folder_path}' (including subfolders): {total_size_mb:.2f} MB")


print_total_folder_size(folder_path)

# COMMAND ----------

