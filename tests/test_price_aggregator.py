from src.aggregation import PriceAggregator


def test_empty_aggregator_has_zero_average_and_no_bounds():
    agg = PriceAggregator()
    assert agg.count == 0
    assert agg.running_average == 0.0
    assert agg.min_price is None and agg.max_price is None


def test_running_average_and_bounds():
    agg = PriceAggregator()
    assert agg.add(10.0) == 10.0
    assert agg.add(20.0) == 15.0
    assert agg.add(30.0) == 20.0
    assert agg.count == 3
    assert agg.min_price == 10.0
    assert agg.max_price == 30.0


def test_per_product_average():
    agg = PriceAggregator()
    agg.add(100.0, "Laptop")
    agg.add(200.0, "Laptop")
    agg.add(50.0, "Mouse")
    assert agg.per_product_average() == {"Laptop": 150.0, "Mouse": 50.0}


def test_snapshot_is_plain_data():
    agg = PriceAggregator()
    agg.add(10.0, "Mouse")
    agg.add(15.0, "Mouse")
    snap = agg.snapshot()
    assert snap["count"] == 2
    assert snap["running_average"] == 12.5
    assert snap["min_price"] == 10.0
    assert snap["max_price"] == 15.0
    assert snap["per_product_average"] == {"Mouse": 12.5}
