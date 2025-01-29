from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("TotalAmountByCustomer")
sc = SparkContext(conf=conf)


def parse_line(line: str) -> tuple[int, float]:
    fields = line.split(",")
    customer_id = fields[0]
    amount = fields[2]
    return (int(customer_id), float(amount))


input = sc.textFile("./customer-orders.csv")
parsed_lines = input.map(parse_line)
total_by_customer = parsed_lines.reduceByKey(lambda x, y: x + y)

flipped = total_by_customer.map(lambda x: (x[1], x[0]))
total_by_customer_sorted = flipped.sortByKey(ascending=False)

results = total_by_customer_sorted.collect()

for result in results:
    print(result)

