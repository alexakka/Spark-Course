from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parsedLine(line):
    fields = line.split(",")
    station = fields[0]
    tempFlag = fields[2]
    temp = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32

    return (station, tempFlag, temp)


lines = sc.textFile("C:/SparkCourse/Spark-Course/1800.csv")
parsedLines = lines.map(parsedLine)
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stations = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stations.reduceByKey(lambda x, y: max(x, y))

result = minTemps.collect()

for temp in result:
    print(f"{temp[0]}\t{temp[1]}")
