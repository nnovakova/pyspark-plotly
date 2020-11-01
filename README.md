# PySpark SQL, Plotly example

An example of PySpark API to join two datasets via SQL. Visualization is done via Plotly.

## Run

Pre-requisites:
- Install Java 8 (for Spark)

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
python3 loan-vs-price.py
```

It will generate Plotly graph as HTML file `plot.html`

## Datasets:

- https://fred.stlouisfed.org/series/QDEN628BIS
- https://data.europa.eu/euodp/en/data/dataset/bank-interest-rates-loans-households
