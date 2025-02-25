import polars as pl
import sys
import os

sys.path.append(os.getcwd())

from utils.kafka_utils import KafkaProducer, load_config


class UserEventProducer:
    def __init__(self, config_path="config.yaml"):
        config = load_config(config_path)
        self.file_path = config["data"]["dataset_path"]
        self.original_df = self._load_data()
        self.df = self.original_df.clone()
        self.batch_size = config["data"]["batch_size"]

        self.producer = KafkaProducer("user_events", config["kafka"]["user_events"])

    def _load_data(self) -> pl.DataFrame:
        df = pl.read_csv(self.file_path)
        return df.fill_null(strategy="forward").drop("Customer ID", "Purchase Date")

    def _resample_data(self):
        self.df = self.original_df.sample(n=self.original_df.height, with_replacement=True)

    def run(self):
        while True:
            if self.df.is_empty():
                self._resample_data()

            batch = self.df.head(self.batch_size)
            self.df = self.df.slice(self.batch_size, len(self.df) - self.batch_size)

            churn_data = batch.to_dicts()
            self.producer.send_message(churn_data)

            
if __name__ == "__main__":
    p = UserEventProducer()
    p.run()
