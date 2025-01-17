import logging
import pandas as pd
from msgbroker.producer_consumer import Consumer


class ExcelConsumer(Consumer):
    def __init__(self, producer, output_file):
        """
        Initialize the ExcelConsumer.

        Args:
            producer (ExcelProducer): The producer instance providing records.
            output_file (str): Path to the output CSV file.
        """
        super().__init__(producer)
        self.output_file = output_file
        self.consumed_records = []

    def consume(self):
        """
        Consume all records produced by the producer.
        """
        if self.producer.records is None:
            logging.error("No records to consume. Ensure the producer has produced records.")
            raise ValueError("Producer has not produced records.")
        for record in self.producer.records:
            self.process_record(record)

    def process_record(self, record):
        """
        Process an individual record by adding it to the consumed records list.
        """
        self.consumed_records.append(record)

    def finalize(self):
        """
        Finalize the consumption by writing records to the output CSV file.
        """
        try:
            pd.DataFrame(self.consumed_records).to_csv(self.output_file, index=False, sep="|")
            logging.info(f"Data successfully written to CSV: {self.output_file}")
        except Exception as e:
            logging.error(f"Error writing to CSV file {self.output_file}: {e}")
            raise
        finally:
            self.consumed_records = []  # Clear records after writing
