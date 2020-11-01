import os
import pyspark
import plotly.graph_objs as go

from pandas.core.series import Series
from datetime import datetime
from plotly.offline import plot
from pyspark.sql import SparkSession, SQLContext, DataFrame, Column
from pyspark.sql.types import FloatType, DateType, StructType, StructField
from pyspark.sql.functions import to_date, col, date_format


def normalize(df: DataFrame, column: str) -> Series:
    result = df.copy()
    max_value = df[column].max()
    min_value = df[column].min()
    result[column] = (df[column] - min_value) / (max_value - min_value)
    return result[column]


# version = 'python3'
# os.environ['PYSPARK_PYTHON'] = version
# os.environ['PYSPARK_DRIVER_PYTHON'] = version

spark = SparkSession.builder.appName('LoanVsPrices').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

loanSchema = StructType([
    StructField("quarterDate", DateType(), True),
    StructField("percent", FloatType(), True)])

loanDf = spark.sparkContext \
    .textFile("data.csv") \
    .zipWithIndex() \
    .filter(lambda x: x[1] > 5) \
    .map(lambda x: x[0].split(',')) \
    .map(lambda x: (datetime.strptime(x[0], '%Y%b'), float(x[4]))) \
    .toDF(loanSchema)
loanDf.show()
loanDf.printSchema()

priceSchema = StructType([
    StructField("quarterDate", DateType(), True),
    StructField("index2010", FloatType(), True)])
priceDf = spark.read.format("csv").option("header", True) \
    .schema(priceSchema) \
    .load("QDEN628BIS.csv") \
    .select(to_date(col("quarterDate")).alias("quarterDate"), col("index2010"))
priceDf.show()
priceDf.printSchema()

priceDf.createOrReplaceTempView("price")
loanDf.createOrReplaceTempView("loan")
joined = spark.sql(
    "select p.quarterDate, l.percent, p.index2010 from price p inner join loan l on p.quarterDate = l.quarterDate order by p.quarterDate") \
    .toPandas()
print(joined)

percentValues = normalize(joined, "percent")
indexValues = normalize(joined, "index2010")

data = [
    go.Scatter(x=joined.quarterDate, y=percentValues,
               name="% Loan for House Purchase", text=joined.percent),
    go.Scatter(x=joined.quarterDate, y=indexValues,
               name="Residential Property Price (quarterly)", text=joined.index2010)
]
fig = go.Figure(data, layout_title_text="Loan vs. Property Price")
plot(fig, filename='plot.html')
