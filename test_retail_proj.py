import pytest
from lib.DataReader import read_customers , read_orders
from lib.DataManipulation import filter_closed_orders , count_orders_state , filter_orders_generic
from lib.ConfigReader import get_app_config

## TEST CASE 1

@pytest.mark.skip("work in progress")
def test_read_customers(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

## TEST CASE 2

@pytest.mark.skip("work in progress")
def test_read_orders(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

## TEST CASE 3

@pytest.mark.skip("work in progress")
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

## TEST CASE 4

@pytest.mark.skip("work in progress")
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

## TEST CASE 5 

@pytest.mark.skip("work in progress")
def test_count_orders_state(spark,expected_result):
    customers_df = read_customers(spark,"LOCAL")
    actual_result = count_orders_state(customers_df)
    assert actual_result.collect() == expected_result.collect()

## TEST CASE 6

@pytest.mark.parametrize(
 "status,count",
 [("CLOSED", 7556),
 ("PENDING_PAYMENT", 15030),
 ("COMPLETE", 22900)])

def test_check_count_df(spark,status,count):
 orders_df = read_orders(spark, "LOCAL")
 filtered_count = filter_orders_generic(orders_df,status).count()
 assert filtered_count == count
