# **Data Processor Utility**

## **Overview**

This Python project provides a modular, extensible framework for processing **JSON**, **XML**, and **Excel** files with support for database ingestion, configurable schemas, and interchangeable components.

### Key Features

#### General Features
- Modular design with Factories for processors, loggers, and connections.
- Prometheus integration for real-time metrics and observability.
- Robust fallback logging and retry mechanisms for resilience.
- Highly extensible to support new file types, databases, or workflows.

#### JSON/XML Processing
- Export records from JSON or XML files into an **Excel** or **CSV** file.
- Flatten nested structures in JSON/XML and **insert processed records into PostgreSQL or Oracle databases**.

#### Excel File Processing
- Process Excel files with a schema defined on the **third row** and data starting from the **fourth row**.
- Export processed records into a **CSV file** for integration with SQL Loader or other ingestion workflows.

#### Customization with Producers/Consumers
- **Producers**: Sources for generating or fetching data (e.g., APIs, file systems).
- **Consumers**: Sinks for processed data (e.g., SQL, message queues).

---

## **Configuration Schema**

The project uses configuration schemas to map file keys to database columns, ensuring seamless data integration.

### Example Configuration Schemas

#### **JSON Schema**
```json
{
  "jsonSchema": {
    "user": "USER",
    "dt_created": "DT_CREATED",
    "dt_submitted": "DT_SUBMITTED",
    "ast_name": "AST_NAME",
    "location": "LOCATION",
    "status": "STATUS",
    "json_hash": "JSON_HASH",
    "local_id": "LOCAL_ID",
    "filename": "FILENAME",
    "fnumber": "FNUMBER",
    "scan_time": "SCAN_TIME"
  }
}
```

#### **XML Schema**
```json
{
  "xmlSchema": {
    "user": "USER",
    "dt_created": "DT_CREATED",
    "dt_submitted": "DT_SUBMITTED",
    "ast_name": "AST_NAME",
    "location": "LOCATION",
    "status": "STATUS",
    "json_hash": "JSON_HASH",
    "local_id": "LOCAL_ID",
    "filename": "FILENAME",
    "fnumber": "FNUMBER",
    "scan_time": "SCAN_TIME"
  }
}
```

### Key-Value Mapping

- **Key**: Represents the field in the JSON or XML file.
- **Value**: Specifies the corresponding column name in the database.

### How It Works

#### **JSON Processing**
- The `jsonSchema` defines the mapping of JSON fields to database columns.
- **Example**: `"user"` in JSON maps to `"USER"` in the database.

#### **XML Processing**
- The `xmlSchema` defines the mapping of XML fields to database columns.
- **Example**: `"location"` in XML maps to `"LOCATION"` in the database.

### Customization
You can customize the schema by modifying the `jsonSchema` or `xmlSchema` in the configuration file to align with your specific database structure.


## Makefile Commands

### `make offline-package`

### Purpose
Prepares all Python dependencies for offline installation. Run this in a connected environment.

### Steps
1. Freezes the current Python environment's dependencies into `requirements.txt`.
2. Downloads the dependencies specified in `requirements.txt` into a `vendor` directory.
3. Compresses the `vendor` directory into a `vendor.tar.gz` archive for portability.

---

### `make install-offline`

### **Purpose**
Installs dependencies in a disconnected environment using the `vendor.tar.gz` archive.

### **Pre-requisites**
- The `vendor.tar.gz` file and `requirements.txt` must be available in the working directory.  
  - Run `make offline-package` in a connected environment to generate these artifacts.

### **Steps**
1. Decompresses the `vendor.tar.gz` archive to recreate the `vendor` directory.
2. Installs dependencies from the `vendor` directory without connecting to the internet.


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

To remove generated dependency files in the environment:

```make clean-vendor```

# Notes

Ensure make is installed on your system to run these commands.
Python and pip must be installed in both connected and disconnected environments.
Use the same Python version in both environments to ensure compatibility.
The offline-package command prepares dependencies specific to the current environment. If requirements change, re-run the command.

# Setup

sudo yum install -y python3 python3-pip python3-virtualenv