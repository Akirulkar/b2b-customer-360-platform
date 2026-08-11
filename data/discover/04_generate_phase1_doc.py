from pathlib import Path
import pandas as pd


def load_selected_fields_summary() -> str:
    """Reads selected_salesforce_fields.csv and builds a clean markdown list of required fields."""
    csv_path = Path("docs/requirements/selected_salesforce_fields.csv")
    if not csv_path.exists():
        return "_Field selection CSV not found. Run 03_parse_field_selection.py first._\n"

    df = pd.read_csv(csv_path)
    required_df = df[df["Selected for Ingestion"] == "Required"]

    output_lines = []
    for obj_name, group in required_df.groupby("Object"):
        direct_fields = group[group["Field Type"] == "Direct"][
            "Field API Name"
        ].tolist()
        rel_fields = group[group["Field Type"] == "Relationship Traversal"][
            "Field API Name"
        ].tolist()

        formatted_direct = ", ".join([f"`{f}`" for f in direct_fields])
        line = f"- **{obj_name}** ({len(group)} total): {formatted_direct}"

        if rel_fields:
            formatted_rel = ", ".join([f"`{f}`" for f in rel_fields])
            line += (
                f" | *SOQL Relationship Traversal for Enrichment:* {formatted_rel}"
            )

        output_lines.append(line)

    return "\n".join(output_lines)


def generate_phase1_md():
    fields_summary_md = load_selected_fields_summary()

    content = rf"""# Phase 1 — Source Discovery & Requirements Freeze

    ## 1. Project Overview & Objectives
    The **B2B Customer 360 & Real-Time Sales Analytics Platform** integrates fragmented B2B customer data across three primary layers:
    1. **Salesforce CRM**: Core entity extraction (Account, Contact, Lead, Opportunity, User, OpportunityContactRole).
    2. **Real-Time Digital Events**: Streaming interactions (page views, demo requests, content downloads).
    3. **PostgreSQL Reference Data**: Reference and lookup enrichment for downstream analytics.

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

    > **Field Modeling Note:** Direct fields represent native object attributes stored in Salesforce. `UserRole.Name` is categorized as a SOQL relationship traversal field used for downstream sales representative role enrichment during Bronze-to-Silver transformation.

    ### Explicit Relationship Topology
    To support Phase 2 canonical data modeling, the exact foreign key relationships among the six core objects are defined as follows:

    ```text
    Account (Id)
    ├── 1:N ──> Contact (AccountId)
    └── 1:N ──> Opportunity (AccountId)

    Opportunity (Id)
    └── 1:N ──> OpportunityContactRole (OpportunityId)

    Contact (Id)
    └── 1:N ──> OpportunityContactRole (ContactId)

    User (Id)
    ├── 1:N ──> Account (OwnerId)
    └── 1:N ──> Opportunity (OwnerId)

    Lead (Conversion Keys — Nullable)
    ├── 0..1 ──> Account     via ConvertedAccountId
    ├── 0..1 ──> Contact     via ConvertedContactId
    └── 0..1 ──> Opportunity via ConvertedOpportunityId

    ```json
    {{
    "event_id": "evt_1020304050",
    "event_type": "pricing_page_view",
    "timestamp": "2026-08-11T17:30:00Z",
    "identity_resolution": {{
        "sfdc_contact_id": "0038c00002A1xBCAAZ",
        "sfdc_account_id": "0018c00002A1xACAAZ",
        "anonymous_id": "anon_usr_998877",
        "email": "stakeholder@acme.com",
        "domain": "acme.com"
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

    print(f"✅ Revised Phase 1 Freeze Document saved at: {doc_path}")

if __name__ == "__main__":
    generate_phase1_md()


