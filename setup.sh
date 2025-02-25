#!/usr/bin/env bash
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
kaggle datasets download -d shriyashjagtap/e-commerce-customer-for-behavior-analysis -p data --unzip
docker compose up -d