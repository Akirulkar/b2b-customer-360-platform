import json
from pathlib import Path
import pandas as pd


def load_selected_fields_summary() -> str:
    """Reads selected_salesforce_fields.csv and builds a markdown list of required fields."""
    csv_path = Path("docs/requirements/selected_salesforce_fields.csv")
    if not csv_path.exists():
        return (
            "_Field selection CSV not found. Run 03_parse_field_selection.py first._\n"
        )

    df = pd.read_csv(csv_path)
    required_df = df[df["Selected for Ingestion"] == "Required"]

    output_lines = []
    for obj_name, group in required_df.groupby("Object"):
        field_list = ", ".join([f"`{field}`" for field in group["Field API Name"]])
        output_lines.append(f"- **{obj_name}**: {field_list}")

    return "\n".join(output_lines)


def generate_phase1_md():
    fields_summary_md = load_selected_fields_summary()

    content = rf"""# Phase 1 — Source Discovery & Requirements Freeze
    ## 1. Project Overview & Objectives
    The **B2B Customer 360 & Real-Time Sales Analytics Platform** integrates fragmented B2B customer data across three primary layers:
    1. **Salesforce CRM**: Account, Contact, Lead, Opportunity, User, and OpportunityContactRole records.
    2. **Real-Time Digital Events**: High-frequency streaming events (page views, demo requests, content downloads).
    3. **PostgreSQL Reference Data**: Product catalogs, product categories, and pricing tiers.

    ---

    ## 2. Business Question Matrix

    | Business Question | Source Systems Required | Key Canonical Entities | Target Analytics Mart |
    | :--- | :--- | :--- | :--- |
    | **Who is the customer?** | Salesforce (Account, Contact) | Account, Contact | Customer 360 |
    | **What is our relationship?** | Salesforce (Opportunity) | Opportunity | Pipeline Analytics |
    | **What is the customer doing?** | Real-Time Digital Events | Customer Event | Intent & Engagement Score |
    | **Which accounts need focus?** | SFDC + Digital Events | Account + Intent Aggregates | Sales Prioritization Mart |
    | **How is the pipeline performing?** | Salesforce (Opportunity, User) | Opportunity, Rep | Sales Performance Mart |
    | **Are leads converting?** | Salesforce (Lead, Opp) | Lead Conversion | Lead Attribution Mart |

    ---

    ## 3. Salesforce Source Scope (Freeze)

    ### Selected Core Objects & Selected Fields

    {fields_summary_md}

    ### Delta Loading Strategy
    All six core objects contain the standard Salesforce audit timestamp `SystemModstamp`. This field will be used as the high-water mark for incremental CDC ingestion during Bronze layer extraction.

    ---

    ## 4. Real-Time Event Scope

    Digital events will be ingested via JSON format with a normalized envelope:

    ```json
    {{
    "event_id": "evt_1020304050",
    "event_type": "pricing_page_view",
    "timestamp": "2026-08-11T17:30:00Z",
    "user_identity": {{
        "email": "stakeholder@acme.com",
        "domain": "acme.com",
        "sfdc_contact_id": "0038c00002A1xBCAAZ"
    }},
    "attributes": {{
        "url": "/pricing",
        "plan_interest": "enterprise",
        "duration_seconds": 45
    }}
    }}"""

    output_dir = Path("docs/requirements")
    output_dir.mkdir(parents=True, exist_ok=True)

    doc_path = output_dir / "phase-1-source-discovery.md"
    doc_path.write_text(content, encoding="utf-8")

    print(f"✅ Phase 1 Freeze Document successfully generated at: {doc_path}")

if __name__ == "__main__":
    generate_phase1_md()


