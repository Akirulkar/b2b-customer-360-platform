def test_list_intent_engagement(client):
    response = client.get("/api/v1/intent")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert data[0]["account_id"] == "acc_001"
    assert data[0]["intent_score"] == 45.0
    assert data[0]["website_visits"] == 5


def test_get_intent_by_account_id_success(client):
    response = client.get("/api/v1/intent/acc_001")
    assert response.status_code == 200
    data = response.json()
    assert data["account_id"] == "acc_001"
    assert data["demo_requests"] == 1


def test_get_intent_by_account_id_not_found(client):
    response = client.get("/api/v1/intent/non_existent_acc")
    assert response.status_code == 404