"""Unit tests verifying CRM canonical transformations, hash computation, and phone/email normalization."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.silver.salesforce.account import transform_account
from spark.silver.salesforce.contact import transform_contact
from spark.silver.salesforce.lead import transform_lead
from spark.silver.salesforce.opportunity import transform_opportunity
from spark.silver.salesforce.opportunity_contact_role import transform_opportunity_contact_role
from spark.silver.salesforce.sales_rep import transform_sales_rep


def test_contact_transformation_and_normalization(spark):
    sample_contact = [
        {
            "Id": "003XX001",
            "AccountId": "001XX001",
            "FirstName": " John ",
            "LastName": "Doe ",
            "Email": "  John.Doe@ACME.COM ",
            "Phone": "+1 (555) 019-2834",
            "Title": "Director",
            "Department": "IT",
            "OwnerId": "005XX001",
            "CreatedDate": "2026-01-10T10:00:00Z",
            "SystemModstamp": "2026-01-12T12:00:00Z",
        }
    ]
    bronze_df = spark.createDataFrame(sample_contact)
    silver_df = transform_contact(bronze_df)
    row = silver_df.first()

    assert row["contact_id"] == "cnt_003XX001"
    assert row["account_id"] == "acc_001XX001"
    assert row["first_name"] == "John"
    assert row["last_name"] == "Doe"
    assert row["email"] == "john.doe@acme.com"
    assert row["phone"] == "+15550192834"
    assert row["owner_id"] == "srep_005XX001"
    assert row["source_system"] == "salesforce"
    assert row["record_hash"] is not None


def test_account_transformation_and_domain_cleanup(spark):
    sample_account = [
        {
            "Id": "001XX001",
            "Name": " Acme Corporation ",
            "Type": "Customer - Direct",
            "Phone": "(555) 019-0000",
            "Website": "https://www.acme.com/about",
            "Industry": "Technology",
            "AnnualRevenue": 5000000.00,
            "NumberOfEmployees": 250,
            "OwnerId": "005XX001",
            "CreatedDate": "2026-01-01T08:00:00Z",
            "SystemModstamp": "2026-01-05T09:00:00Z",
        }
    ]
    bronze_df = spark.createDataFrame(sample_account)
    silver_df = transform_account(bronze_df)
    row = silver_df.first()

    assert row["account_id"] == "acc_001XX001"
    assert row["account_name"] == "Acme Corporation"
    assert row["website"] == "acme.com"
    assert row["owner_id"] == "srep_005XX001"
    assert row["record_hash"] is not None


def test_lead_conversion_relationships(spark):
    sample_lead = [
        {
            "Id": "00QXX001",
            "FirstName": "Jane",
            "LastName": "Smith",
            "Company": "Smith Logistics",
            "Email": "jane@smithlogistics.com",
            "LeadSource": "Web",
            "Status": "Closed - Converted",
            "Rating": "Hot",
            "OwnerId": "005XX001",
            "IsConverted": True,
            "ConvertedAccountId": "001XX002",
            "ConvertedContactId": "003XX002",
            "ConvertedOpportunityId": "006XX001",
            "CreatedDate": "2026-01-02T10:00:00Z",
            "SystemModstamp": "2026-01-06T10:00:00Z",
        }
    ]
    bronze_df = spark.createDataFrame(sample_lead)
    silver_df = transform_lead(bronze_df)
    row = silver_df.first()

    assert row["lead_id"] == "led_00QXX001"
    assert row["is_converted"] is True
    assert row["converted_account_id"] == "acc_001XX002"
    assert row["converted_contact_id"] == "cnt_003XX002"
    assert row["converted_opportunity_id"] == "opp_006XX001"


def test_opportunity_transformation(spark):
    sample_opp = [
        {
            "Id": "006XX001",
            "AccountId": "001XX001",
            "Name": "Acme - Expansion 2026",
            "StageName": "Negotiation/Review",
            "Amount": 120000.00,
            "Probability": 80.0,
            "CloseDate": "2026-04-30",
            "Type": "New Customer",
            "LeadSource": "Web",
            "IsClosed": False,
            "IsWon": False,
            "OwnerId": "005XX001",
            "CreatedDate": "2026-01-10T10:00:00Z",
            "SystemModstamp": "2026-01-15T12:00:00Z",
        }
    ]
    bronze_df = spark.createDataFrame(sample_opp)
    silver_df = transform_opportunity(bronze_df)
    row = silver_df.first()

    assert row["opportunity_id"] == "opp_006XX001"
    assert row["account_id"] == "acc_001XX001"
    assert row["stage"] == "Negotiation/Review"
    assert row["is_closed"] is False
    assert row["is_won"] is False