from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName("Streaming").setMaster("local")
sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 1) ## Con intervalo de un segundo
lines = ssc.socketTextStream("localhost", 4444)
lines.pprint()

ssc.start()
ssc.awaitTermination()
