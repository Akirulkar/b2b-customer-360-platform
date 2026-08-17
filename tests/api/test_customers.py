def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_list_customers(client):
    response = client.get("/api/v1/customers")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert data[0]["account_id"] == "acc_001"


def test_get_customer_by_id_success(client):
    response = client.get("/api/v1/customers/acc_001")
    assert response.status_code == 200
    assert response.json()["account_name"] == "Acme Corp"


def test_get_customer_by_id_not_found(client):
    response = client.get("/api/v1/customers/non_existent")
    assert response.status_code == 404