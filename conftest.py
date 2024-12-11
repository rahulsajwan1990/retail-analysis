from lib.Utils import get_spark_session
import pytest


@pytest.fixture
def spark():
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_result(spark):
    "gives the expected results"
    result_schema = "state string , count int"
    return spark.read \
       .format("csv") \
       .schema(result_schema) \
       .load("data/test_result/customers_aggregate.csv")
