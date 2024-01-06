import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col,split, explode, count, lower
spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header','true').load('ex1.csv')

# #Câu1
print("Câu 1:")
total_transactions = data.select("InvoiceNo").distinct().count()
print("Tổng số giao dịch là:", total_transactions)
total_products = data.select("StockCode").distinct().count()
print("Tổng số sản phẩm là:", total_products)
total_customers = data.select("CustomerID").distinct().count()
print("Tổng số khách hàng là:", total_customers)


# #câu 2
print ("câu 2:")
missing_customers = data.filter(col("CustomerID").isNull()).count()
total_customers = data.count()
missing_ratio = (missing_customers / total_customers) * 100
print(f'Tỉ lệ khách hàng không có thông tin là {missing_ratio:.2f}%')


# #Câu3
print("câu 3")
# Nhóm dữ liệu theo "Country" và tính tổng "Quantity"
country_orders = data.groupBy("Country") \
    .agg(sum("InvoiceNo").alias("TotalQuantity")) \
    .sort(col("TotalQuantity").desc())

# Lấy nước có số lượng đơn hàng thứ ba
third_highest_country = country_orders.collect()[2]
print(f"Nước có số lượng đơn hàng nhiều thứ 3 là: {third_highest_country['Country']}, với số lượng: {third_highest_country['TotalQuantity']}")


# Câu 4
print("câu 4:")
# Tách từ trong cột "Description" và chuyển đổi thành dòng riêng biệt
words = data.select(explode(split(lower(col("Description")), r'\W+')).alias("word"))
# Đếm số lần xuất hiện của mỗi từ
word_counts = words.groupBy("word").agg(count("word").alias("count"))
# Tìm số lần xuất hiện ít nhất
min_count = word_counts.agg({"count": "min"}).collect()[0][0]
# Lấy tất cả các từ có số lần xuất hiện ít nhất
least_occurred_words = word_counts.filter(col("count") == min_count).select("word", "count")
# In kết quả
print("Các từ xuất hiện ít nhất trong phần 'Description':")
least_occurred_words.show()
print(least_occurred_words.count())

# Câu 5
print("câu 5")
# Lọc dữ liệu cho United Kingdom
uk_data = data.filter(col("Country") == "United Kingdom")

# Nhóm dữ liệu theo "StockCode" và tính tổng "Quantity"
product_sales = uk_data.groupBy("StockCode") \
    .agg(sum("Quantity").alias("TotalQuantity")) \
    .sort(col("TotalQuantity").desc())

# Lấy sản phẩm bán chạy nhất
best_selling_product = product_sales.first()
print(f"Sản phẩm bán được số lượng lớn nhất ở United Kingdom là: {best_selling_product['StockCode']}, với số lượng: {best_selling_product['TotalQuantity']}")

# Đóng Spark session    
spark.stop()