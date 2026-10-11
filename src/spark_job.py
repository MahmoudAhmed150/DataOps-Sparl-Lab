from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def clean_customers(df):
    return df.filter(col("age") >= 18)


def clean_orders(df):
    return df.filter(
        (col("status") == "Completed") &
        (col("quantity") >= 2)
    )


def calculate_revenue(df):
    return df.withColumn(
        "revenue",
        col("quantity") * col("price")
    )


def aggregate_revenue(orders_df, customers_df):
    result = orders_df.join(
        customers_df,
        "customer_id",
        "inner"
    )

    return result.groupBy("country") \
        .agg(sum("revenue").alias("total_revenue")) \
        .orderBy(col("total_revenue").desc())


def validate_schema(df, required_columns):
    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    return df


def main():
    spark = SparkSession.builder \
        .appName("Customer Revenue Pipeline") \
        .getOrCreate()

    customers = spark.read.csv(
        "data/customers.csv",
        header=True,
        inferSchema=True
    )

    orders = spark.read.csv(
        "data/orders.csv",
        header=True,
        inferSchema=True
    )

    customers = validate_schema(
    customers,
    ["customer_id", "name", "age", "country"]
)

    orders = validate_schema(
        orders,
        ["order_id", "customer_id", "status", "quantity", "price"]
    )

    customers = clean_customers(customers)
    orders = clean_orders(orders)
    orders = calculate_revenue(orders)

    result = aggregate_revenue(orders, customers)

    result.select(
        "country",
        "total_revenue"
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()