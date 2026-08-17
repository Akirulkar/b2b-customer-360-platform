def test_list_prioritization(client):
    response = client.get("/api/v1/prioritization?priority_band=Tier 1")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["priority_band"] == "Tier 1"
    assert data[0]["priority_score"] == 88.5