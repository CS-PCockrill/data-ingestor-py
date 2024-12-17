# **Data Processor Utility**

## **Overview**

This Python project provides tools for processing **JSON**, **XML**, and **Excel** files. It includes functionality to:

1. **Feature 1**: Process JSON/XML files to:
   - Export records into an **Excel** file.
   - Flatten nested structures in JSON/XML and **insert the records into a PostgreSQL/Oracle database**.

2. **Feature 2**: Process an **Excel file** with:
   - Schema defined on the **third row**.
   - Data records starting from the fourth row.
   - Export the records into a **CSV file** for integration with SQL Loader or database ingestion.

## **Dependencies**

The project requires Python 3.8+ and the following libraries:

### Core Libraries:
- **pandas**: For Excel and CSV file processing.
- **psycopg2**: For connecting to PostgreSQL.
- **cx_Oracle**: For connecting to Oracle databases in production or testing environments.
- **openpyxl**: For reading and writing Excel files.
- **xml.etree**: For processing XML files.
- **json**: For handling JSON files.

### Installation:
Use `pip` to install the required libraries.

```bash
# Connected Environment
pip install pandas psycopg2-binary cx_Oracle openpyxl
```


