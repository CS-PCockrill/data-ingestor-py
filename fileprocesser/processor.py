from config.config import METRICS
from threading import Thread, Event
import logging

class Processor:
    def __init__(self, logger=None):
        self.logger = logger

    @METRICS["file_processing_time"].time()
    def process(self, producer, consumer, key_column_mapping=None):
        """
        Processes records using producer and consumer components.

        Args:
            producer (Producer): The producer instance for generating records.
            consumer (Consumer): The consumer instance for processing records.
            key_column_mapping (dict): Optional mapping of JSON keys to database column names.

        Behavior:
            - Uses the provided producer to generate records.
            - Passes records from the producer to the consumer for processing.
        """
        all_workers_done = Event()

        try:
            # Start the producer task
            def producer_task():
                try:
                    producer.produce_from_source()
                except Exception as e:
                    logging.error(f"Producer encountered an error: {e}")
                    METRICS["errors"].inc()
                    raise
                finally:
                    producer.signal_done()
                    all_workers_done.set()

            producer_thread = Thread(target=producer_task)
            producer_thread.start()

            # Start the consumer task
            def consumer_task():
                try:
                    consumer.consume()
                except Exception as e:
                    logging.error(f"Consumer encountered an error: {e}")
                    METRICS["errors"].inc()
                    raise
                finally:
                    consumer.finalize()

            consumer_thread = Thread(target=consumer_task)
            consumer_thread.start()

            # Wait for producer and consumer to finish
            producer_thread.join()
            all_workers_done.wait()
            consumer_thread.join()

            logging.info("Processing completed successfully.")
        except Exception as e:
            logging.error(f"Failed to process records: {e}")
            METRICS["errors"].inc()
            raise
        finally:
            producer.close()

    def process_files(self, files, producer=None):
        pass