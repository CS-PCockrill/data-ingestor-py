from fileprocesser.dms_processor import DMSProcessor
from fileprocesser.file_processor import FileProcessor
from logger.sqllogger import SQLLogger
from msgbroker.excel_consumer import ExcelConsumer
from msgbroker.excel_producer import ExcelProducer
from msgbroker.file_producer import FileProducer
from msgbroker.sql_consumer import SQLConsumer

INTERFACES = [
    {
        "interface_ids": {"mist", "mist-ams"},  # Unique IDs for this interface
        "control_file_path": "interfaces/mist-ams/control-file.json",  # Path to control/configuration file
        "processor_class": FileProcessor,  # Processor responsible for handling files for this interface
        "file_extensions": [".json", ".xml"],  # Supported file extensions for ingestion
        "logger_class": SQLLogger,  # Logger to track operations and log metadata
        "producer_class": FileProducer,
        "consumer_class": SQLConsumer,
    },
    {
        "interface_ids": {"dms", "dms-test"},  # Unique IDs for another interface
        "control_file_path": "interfaces/dms/control-file.json",  # Path to control/configuration file
        "processor_class": DMSProcessor,  # Processor for handling DMS-specific files
        "file_extensions": [".xlsx", ".xls"],  # Supported Excel file types
        "logger_class": SQLLogger,  # Logger for DMS
        "producer_class": ExcelProducer,
        "consumer_class": ExcelConsumer,
    },
]
