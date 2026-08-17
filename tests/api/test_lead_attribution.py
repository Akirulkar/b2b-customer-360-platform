def test_list_lead_attribution_all(client):
    response = client.get("/api/v1/lead-attribution")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert data[0]["lead_id"] == "lead_99"
    assert data[0]["is_converted"] is True


def test_list_lead_attribution_filter_source(client):
    response = client.get("/api/v1/lead-attribution?lead_source=Web")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["lead_source"] == "Web"


def test_list_lead_attribution_filter_conversion(client):
    response = client.get("/api/v1/lead-attribution?is_converted=false")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0