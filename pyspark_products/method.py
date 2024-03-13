from pyspark.sql.types import StructType, StructField, IntegerType, StringType

products_data = [
    (1, "product_1"),
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

links_data = [(1, "category_1", "product_1"),
         (2, 1, 2),
         (3, 2, 2)
         ]
df_links = ['link_id', 'category_id', 'product_id']
links = spark.createDataFrame(links_data, df_links)

df = products.join(categories, "product_id", 'left')
df_rez = df.join(links, 'category_id', 'left')
df_rez.select("name_product", "name_category").show()
