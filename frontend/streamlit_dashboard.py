import streamlit as st
import pandas as pd
import sys
import os

sys.path.append(os.getcwd())

from utils.kafka_utils import KafkaConsumer, load_config


class RetentionDashboard:
    def __init__(self, config_path="config.yaml"):
        self.config = load_config(config_path)
        self.setup_kafka_consumers()

    def setup_kafka_consumers(self):
        self.churn_consumer = KafkaConsumer("churn_metrics", self.config["kafka"]["churn_metrics"], group_id="dashboard_group")

    def fetch_churn_data(self):
        st.session_state["f1"] = []
        st.session_state["accuracy"] = []
        st.session_state["total_amount"] = []

        st.subheader("F1 score")
        f1 = st.empty()
        st.subheader("Accuracy score")
        accuracy = st.empty()
        st.subheader("Total purchase amount")
        total_amount = st.empty()

        while True:
            metric = self.churn_consumer.consume_message()
            if metric:
                st.session_state['f1'].append(metric["f1_score"])
                st.session_state['accuracy'].append(metric["accuracy"])
                st.session_state['total_amount'].append(metric["total_purchase_amount"])

                f1.line_chart(pd.DataFrame.from_dict(st.session_state['f1']))
                accuracy.line_chart(pd.DataFrame.from_dict(st.session_state['accuracy']))
                total_amount.line_chart(pd.DataFrame.from_dict(st.session_state['total_amount']))


    def run(self):
        st.set_page_config(page_title="Churn Dashboard", layout="wide")
        st.title("📊 Retention & Churn Prediction Dashboard")

        self.fetch_churn_data()

if __name__ == "__main__":
    dashboard = RetentionDashboard()
    dashboard.run()
