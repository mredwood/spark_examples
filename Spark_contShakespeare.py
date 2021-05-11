from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setAppName("ContadorShakespeare").setMaster("local")
sc = SparkContext(conf=conf)

sc.setLogLevel("WARN")

shakespeareWords = sc.textFile("file:///home/marcos/BIT/data/shakespeare").flatMap(lambda line: line.split(" ")).map(lambda word: word.lower())

stopWords = sc.textFile("file:///home/marcos/BIT/data/stop-word-list.csv").flatMap(lambda line: line.split(",")).map(lambda word: word.strip())


wordsFiltered = shakespeareWords.subtract(stopWords).filter(lambda s: re.search("^[a-zA-Z]+$", s))

shakespeareWordsCounted = wordsFiltered.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)

shakespeareWordsCountedSorted = shakespeareWordsCounted.map(lambda word: (word[1], word[0])).sortByKey(ascending=False).map(lambda x: (x[1], x[0]))

top100 = shakespeareWordsCountedSorted.take(100)

print("_Palabra_\t_Ocurrencias")
for i in top100:
    if len(i[0]) >=8:
            print(str(i[0]) + "\t" + str(i[1]))
    else:
        print(str(i[0]) + "\t\t" + str(i[1]))
