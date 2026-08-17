def test_list_sales_performance_all(client):
    response = client.get("/api/v1/sales-performance")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert data[0]["sales_rep_id"] == "rep_101"
    assert data[0]["win_rate"] == 0.75


def test_list_sales_performance_filter_rep(client):
    response = client.get("/api/v1/sales-performance?sales_rep_id=rep_101")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["sales_rep_name"] == "John Doe"


def test_list_sales_performance_filter_month(client):
    response = client.get("/api/v1/sales-performance?month=2026-08")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["won_opportunities"] == 3


def test_list_sales_performance_no_match(client):
    response = client.get("/api/v1/sales-performance?sales_rep_id=rep_unknown")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0