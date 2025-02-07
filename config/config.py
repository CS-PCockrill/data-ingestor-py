from prometheus_client import Counter, Histogram, Summary

# Special marker for file separation used for the start of a new file so there's no side effects with table names between files
FILE_DELIMITER = "__NEW_FILE__"

# Prometheus metrics definitions
METRICS = {
    "records_read": Counter(
        "file_processor_records_read",
        "Total number of records read from files."
    ),
    "records_processed": Counter(
        "file_processor_records_processed",
        "Total number of records successfully inserted into the database."
    ),
    "errors": Counter(
        "file_processor_errors",
        "Total number of errors encountered during processing."
    ),
    "file_processing_time": Summary(
        "file_processor_processing_time_seconds",
        "Time taken to process a file, including all worker operations."
    ),
    "batch_insert_time": Histogram(
        "file_processor_batch_insert_time_seconds",
        "Time taken to perform batch inserts into the database."
    )
}
