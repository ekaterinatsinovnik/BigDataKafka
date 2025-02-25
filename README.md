# E-Commerce Churn prediction


## Dataset
The dataset is available on **Kaggle**:  
[🔗 E-Commerce Customer Behavior Dataset](https://www.kaggle.com/datasets/shriyashjagtap/e-commerce-customer-for-behavior-analysis/data)


## How to Run the Project:

#### **Step 1**
Clone repo, create virtual environment, download data and run docker-compose.
```sh
git clone git@github.com:ekaterinatsinovnik/BigDataKafka.git
cd ./BigDataKafka
bash setup.sh
```

#### **Step 2**
Run parts of project in different terminals:
```python
python backend/data_producer.py
```
```python
python backend/churn_trainer.py
```
```python
streamlit run frontend/streamlit_dashboard.py
```
