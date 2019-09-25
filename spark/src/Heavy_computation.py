# import findspark
import os
import time
spark_location='/home/rafalpa/spark/spark/' # Set your own
java8_location='/home/rafalpa/.sdkman/candidates/java/current' # Set your own
os.environ['JAVA_HOME'] = java8_location
# findspark.init(spark_home=spark_location)

from pyspark import SparkContext

# a very heavy computation
def transform_word(word):
    count = 0
    for i in range(2000):
        count += i
    if len(word)>3:
        return word.lower().replace(' ', 'a').replace('Romeo', 'Julia').replace('b', 'c')
    else:
        return "veryLongWord"

start = time.time()

sc = SparkContext("local[*]", "word_count2")

lines_org = sc.textFile("file:/home/rafalpa/BIGDATA/examples/spark/notebooks/data/shakespeare.txt")

lines = lines_org.repartition(2)
print(lines.getNumPartitions())

words = lines.flatMap(lambda line: line.split())

words_transformed = words.map(lambda x: transform_word(x))

print(words_transformed.getNumPartitions())

# word_dict = words_transformed.countByValue()
# sorted = sorted(word_dict.items(),key=lambda i: i[1],reverse=True)
word_dict = words_transformed.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False).take(10)

print(word_dict)

end = time.time()
print(end - start)