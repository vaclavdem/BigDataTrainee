from pyspark.sql.functions import count, sum, col, rank, coalesce, lit
from pyspark.sql.window import Window
from data_io.jdcb_connection import jdbc_input_function
from data_io.json_export import saving_into_json

actor_df = jdbc_input_function("public.actor")
category_df = jdbc_input_function("public.category")
film_category_df = jdbc_input_function("public.film_category")
inventory_df = jdbc_input_function("public.inventory")
film_actor_df = jdbc_input_function("public.film_actor")
rental_df = jdbc_input_function("public.rental")
payment_df = jdbc_input_function("public.payment")
film_df = jdbc_input_function("public.film")
customer_df = jdbc_input_function("public.customer")
address_df = jdbc_input_function("public.address")
city_df = jdbc_input_function("public.city")


#first query
first_result = film_category_df.join(
    category_df,
    "category_id",
    "inner"
)
first_result = first_result.groupBy("name") \
    .agg(count("film_id")) \
    .orderBy("name")


#second query
second_result = rental_df.join(
    inventory_df,
    "inventory_id",
    "inner"
).join(
    film_actor_df,
    "film_id",
    "inner"
).join(
    actor_df,
    "actor_id",
    "inner"
)
second_result = second_result.groupBy("actor_id", "first_name", "last_name") \
    .agg(count("rental_id").alias("rented_times")) \
    .orderBy(col("rented_times").desc())


#third query
third_result = payment_df.join(
    rental_df,
    "rental_id",
    "inner"
).join(
    inventory_df,
    "inventory_id",
    "inner"
).join(
    film_category_df,
    "film_id",
    "inner"
).join(
    category_df,
    "category_id",
    "inner"
)
third_result = third_result.groupBy("name") \
    .agg(sum("amount").alias("amount")) \
    .orderBy(col("amount").desc()) \
    .limit(1)


#fourth query
fourth_result = film_df.join(
    inventory_df,
    "film_id",
    "left"
)
fourth_result = fourth_result.filter(col("inventory_id").isNull())
fourth_result = fourth_result.select("title")


#fifth query
fifth_result = film_df.join(
    film_category_df,
    "film_id",
    "inner"
).join(
    category_df,
    "category_id",
    "inner"
).join(
    film_actor_df,
    "film_id",
    "inner"
).join(
    actor_df,
    "actor_id",
    "inner"
)
fifth_result = fifth_result.filter(col("name") == "Children")
fifth_result = fifth_result.groupBy("actor_id", "first_name", "last_name") \
    .agg(count("film_id").alias("appeared_times"))
fifth_result = fifth_result.withColumn(
    "rnk",
    rank().over(Window.orderBy(col("appeared_times").desc()))
)
fifth_result = fifth_result.filter(col("rnk") <= 3)
fifth_result = fifth_result.select(
    "actor_id", "first_name", "last_name", "appeared_times"
).orderBy(col("appeared_times").desc())


#sixth query
active_cust = customer_df.join(
    address_df,
    "address_id",
    "inner"
).join(
    city_df,
    "city_id",
    "inner"
)
active_cust = active_cust.filter(col("active") == 1)
active_cust = active_cust.groupBy("city") \
    .agg(count("customer_id").alias("active_customers"))
inactive_cust = customer_df.join(
    address_df,
    "address_id",
    "inner"
).join(
    city_df,
    "city_id",
    "inner"
)
inactive_cust = inactive_cust.filter(col("active") == 0)
inactive_cust = inactive_cust.groupBy("city") \
    .agg(count("customer_id").alias("inactive_customers"))
sixth_result = active_cust.join(
    inactive_cust,
    "city",
    "full_outer"
)
sixth_result = sixth_result.select(
    "city",
    coalesce(col("active_customers"), lit(0)).alias("active_customers"),
    coalesce(col("inactive_customers"), lit(0)).alias("inactive_customers")
).orderBy(col("inactive_customers").desc())


#seventh query
seventh_result = payment_df.join(
    customer_df,
    "customer_id",
    "inner"
).join(
    address_df,
    "address_id",
    "inner"
).join(
    city_df,
    "city_id",
    "inner"
).join(
    rental_df,
    "rental_id",
    "inner"
).join(
    inventory_df,
    "inventory_id",
    "inner"
).join(
    film_df,
    "film_id",
    "inner"
).join(
    film_category_df,
    "film_id",
    "inner"
).join(
    category_df,
    "category_id",
    "inner"
)
seventh_result = seventh_result.filter(
    col("name").ilike("a%") & col("city").like("%-%")
)
seventh_result = seventh_result.groupBy(
    col("city"),
    col("name").alias("category")
).agg(
    sum("rental_duration").alias("hours_watched")
)
window_spec = Window.partitionBy("city").orderBy(col("hours_watched").desc())
seventh_result = seventh_result.withColumn(
    "rnk",
    rank().over(window_spec)
)
seventh_result = seventh_result.filter(col("rnk") == 1)
seventh_result = seventh_result.select(
    col("city"),
    col("category"),
    col("hours_watched")
).orderBy("city")


saving_into_json(first_result, "first_result")
saving_into_json(second_result, "second_result")
saving_into_json(third_result, "third_result")
saving_into_json(fourth_result, "fourth_result")
saving_into_json(fifth_result, "fifth_result")
saving_into_json(sixth_result, "sixth_result")
saving_into_json(seventh_result, "seventh_result")