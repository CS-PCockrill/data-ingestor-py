from prometheus_client import Counter, Histogram, Summary

INTERFACE_IDS = {
    key: "interfaces/mist-ams/control-file.json"
    for key in ("mist-ams", "mist")
} | {
    key: "interfaces/dms/control-file.json"
    for key in ("dms", "dms-test")
}

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
