from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("totalOrder")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    orderAmount = float(fields[2])
    return (customerID, orderAmount)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
totalOrders = parsedLines.reduceByKey(lambda x, y: x+y)

results = totalOrders.collect()
for result in results:
    print result
