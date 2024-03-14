from pyspark.sql.types import StructType, StructField, IntegerType, StringType


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

products_data = [
    (1, "product_1"),    #пример продуктов, категорий и их связей 
    (2, "product_2"),
    (3, "product_3")
    ]
df_products = ['product_id', 'name_product']
products = spark.createDataFrame(products_data, df_products)

categories_data = [
    (1, "category_1"),
    (2, "category_2"),
    (3, "category_3")
    ]
df_categories = ['category_id', 'name_category']
categories = spark.createDataFrame(categories_data, df_categories)

links_data = [(1, 1, 1),
         (2, 1, 2),
         (3, 2, 2)
         ]
df_links = ['link_id', 'category_id', 'product_id']
links = spark.createDataFrame(links_data, df_links)

df = products.join(links, "product_id", 'left')       #сам джоин
df_rez = df.join(categories, 'category_id', 'left')
df_rez.select("name_product", "name_category").show()

#Вывод получается вот такой

# +------------+-------------+                                                    
# |name_product|name_category|
# +------------+-------------+
# |   product_1|   category_1|
# |   product_2|   category_2|
# |   product_2|   category_1|
# |   product_3|         NULL|
# +------------+-------------+
