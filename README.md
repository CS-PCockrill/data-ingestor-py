# **Data Processor Utility**

## **Overview**

This Python project provides a robust set of tools for handling **JSON**, **XML**, and **Excel** file processing. Its key functionalities include:

### JSON/XML Processing
- Export records from JSON or XML files into an **Excel** file.
- Flatten nested structures in JSON/XML and **insert the processed records into a PostgreSQL or Oracle database**.

### Excel File Processing
- Handle Excel files with a schema defined on the **third row** and data records starting from the **fourth row**.
- Export the processed records into a **CSV file** for seamless integration with SQL Loader or database ingestion workflows.


## Makefile Commands

### `offline-package`

### Purpose
Prepares all Python dependencies for offline installation. Run this in a connected environment.

### Steps:
1. Freezes the current Python environment's dependencies into `requirements.txt`.
2. Downloads the dependencies specified in `requirements.txt` into a `vendor` directory.
3. Compresses the `vendor` directory into a `vendor.tar.gz` archive for portability.

---

### `install-offline`

### **Purpose**:  
Installs dependencies in a disconnected environment using the `vendor.tar.gz` archive.

### **Pre-requisites**:
- The `vendor.tar.gz` file and `requirements.txt` must be available in the working directory.  
  - Run `make offline-package` in a connected environment to generate these artifacts.

### **Steps**:
1. Decompresses the `vendor.tar.gz` archive to recreate the `vendor` directory.
2. Installs dependencies from the `vendor` directory without connecting to the internet.

### **Usage**:
```bash
make install-offline
```

Dependencies installed locally, enabling the utility to execute in a disconnected environment.

## Example Workflow

### 1. Connected Environment

Prepare the dependencies for offline installation:

```make offline-package```

Transfer the ```vendor.tar.gz``` and ```requirements.txt``` files to the disconnected environment.

### 2. Disconnected Environment

The ```vendor.tar.gz``` and ```requirements.txt``` files should be in the working directory.

### 3. Install the dependencies

```make install-offline```

### Cleanup (Optional)

To remove generated files in the connected environment:

```make clean-vendor```

# Notes

Ensure make is installed on your system to run these commands.
Python and pip must be installed in both connected and disconnected environments.
Use the same Python version in both environments to ensure compatibility.
The offline-package command prepares dependencies specific to the current environment. If requirements change, re-run the command.