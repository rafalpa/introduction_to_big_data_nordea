from __future__ import print_function

# $example on$
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

import os
spark_location='/home/rafalpa/spark/spark/' # Set your own
java8_location='/home/rafalpa/.sdkman/candidates/java/current' # Set your own
os.environ['JAVA_HOME'] = java8_location

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PCAExample")\
        .getOrCreate()

    # $example on$
    data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
            (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
            (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
    df = spark.createDataFrame(data, ["features"])

    pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(df)

    result = model.transform(df).select("pcaFeatures")
    result.show(truncate=False)
    # $example off$

    spark.stop()
