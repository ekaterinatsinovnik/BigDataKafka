import sys
import os

sys.path.append(os.getcwd())

import polars as pl
import numpy as np
from catboost import CatBoostClassifier
from sklearn.metrics import f1_score, balanced_accuracy_score
from utils.kafka_utils import KafkaConsumer, KafkaProducer, load_config


class ChurnPredictor:
    def __init__(self, config_path="config.yaml"):
        config = load_config(config_path)
        self.consumer = KafkaConsumer("user_events", config["kafka"]["user_events"], "churn_group")
        self.producer = KafkaProducer("churn_metrics", config["kafka"]["churn_metrics"])

        self.start_shape_train = config["ml"]["start_shape_train"]
        self.step_shape_train = config["ml"]["step_shape_train"]

        self.model_path = config["ml"]["churn_model_path"]
        self.model = CatBoostClassifier(iterations=1000)

        self.X = np.empty((0, 1))
        self.y = np.empty((0,))
    
    def _update_data(self, new_X, new_y):
        self.X = pl.concat([self.X, new_X]) if self.X.shape[0] > 0 else new_X
        self.y = pl.concat([self.y, new_y]) if self.y.shape[0] > 0 else new_y

    def _train(self):
        cat_features = self.X.select(pl.col(pl.Utf8)).columns
        self.model.fit(self.X.to_pandas(), self.y.to_pandas(), 
                        cat_features=cat_features)
        self.model.save_model(self.model_path)

    def _calc_metrics(self, target, prediction):
        return {
            'f1_score': f1_score(target, prediction),
            'accuracy': balanced_accuracy_score(target, prediction),
            'total_purchase_amount': self.X.limit(50)["Total Purchase Amount"].mean()	
                }

    def _predict(self, event):
        user_data = pl.from_dicts(event)

        y = user_data.select("Churn")
        X = user_data.drop("Churn")

        self._update_data(X, y)

        if len(self.X) < self.start_shape_train:
            return None
        else:
            if len(self.X) % self.step_shape_train == 0:
                self._train()

            prediction = self.model.predict(X.to_pandas())
            metrics = self._calc_metrics(y, prediction)
            return metrics


    def run(self):
        while True:
            event = self.consumer.consume_message()
            if event:
                metrics = self._predict(event)
                self.producer.send_message(metrics)


if __name__ == "__main__":
    p = ChurnPredictor()
    p.run()
