name: Python Script Workflow

on:
  push:
    branches:
      - main
  workflow_dispatch:

jobs:
  run-script:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout repository
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.10'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt

    - name: Create dummy data.csv
      run: |
        echo "T,P,B,A,M,O,C,I,S" > data.csv
        echo "AAPL,,,,,," >> data.csv
        echo "GOOGL,,,,,," >> data.csv
        echo "MSFT,,123.45,,,,,," >> data.csv
        echo "INVALIDTICKER,,,,,," >> data.csv


    - name: Run python script
      run: python test.py

    - name: Upload artifacts
      uses: actions/upload-artifact@v4
      with:
        name: output-files
        path: |
          yahooClean.csv
          unClean.csv
