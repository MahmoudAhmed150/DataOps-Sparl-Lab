import pytest
from pyspark.sql import SparkSession

from src.spark_job import (
    clean_customers,
    clean_orders,
    calculate_revenue,
    aggregate_revenue,
    validate_schema
)


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("TestSparkJob") \
        .getOrCreate()

    yield spark

    spark.stop()


def test_clean_customers(spark):
    customers = spark.createDataFrame([
        (1, "Ahmed", 25, "Egypt"),
        (2, "Mohamed", 17, "Egypt"),
        (3, "John", 30, "USA")
    ], ["customer_id", "name", "age", "country"])

    result = clean_customers(customers)

    assert result.count() == 2
    assert result.filter("age < 18").count() == 0


def test_clean_orders(spark):
    orders = spark.createDataFrame([
        (101, 1, "Completed", 3, 100),
        (102, 1, "Pending", 4, 100),
        (103, 1, "Completed", 1, 100),
        (104, 1, "Completed", 5, 50)
    ], ["order_id", "customer_id", "status", "quantity", "price"])

    result = clean_orders(orders)

    assert result.count() == 2
    assert result.filter("status != 'Completed'").count() == 0
    assert result.filter("quantity < 2").count() == 0


def test_calculate_revenue(spark):
    orders = spark.createDataFrame([
        (101, 1, "Completed", 3, 100),
        (102, 1, "Completed", 2, 150)
    ], ["order_id", "customer_id", "status", "quantity", "price"])

    result = calculate_revenue(orders)

    revenues = {
        row["order_id"]: row["revenue"]
        for row in result.collect()
    }

    assert revenues == {
        101: 300,
        102: 300
    }


def test_aggregate_revenue(spark):
    customers = spark.createDataFrame([
        (1, "Ahmed", 25, "Egypt"),
        (3, "John", 30, "USA"),
        (4, "Sara", 22, "UK")
    ], ["customer_id", "name", "age", "country"])

    orders = spark.createDataFrame([
        (101, 1, "Completed", 3, 100),
        (104, 3, "Completed", 2, 150),
        (106, 4, "Completed", 3, 120)
    ], ["order_id", "customer_id", "status", "quantity", "price"])

    orders = calculate_revenue(orders)

    result = aggregate_revenue(orders, customers)

    actual = {
        row["country"]: row["total_revenue"]
        for row in result.collect()
    }

    expected = {
        "Egypt": 300,
        "USA": 300,
        "UK": 360
    }

    assert actual == expected

def test_validate_schema(spark):
    customers = spark.createDataFrame([
        (1, "Ahmed", 25, "Egypt")
    ], ["customer_id", "name", "age", "country"])

    result = validate_schema(
        customers,
        ["customer_id", "name", "age", "country"]
    )

    assert result is customers


def test_validate_schema_missing_column(spark):
    customers = spark.createDataFrame([
        (1, "Ahmed", 25)
    ], ["customer_id", "name", "age"])

    with pytest.raises(ValueError):
        validate_schema(
            customers,
            ["customer_id", "name", "age", "country"]
        )