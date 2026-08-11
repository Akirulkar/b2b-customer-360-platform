import glob
import json
import os
import pandas as pd


def analyze_extracted_schemas():
    json_files = glob.glob("metadata_raw/*.json")

    if not json_files:
        print("❌ No JSON files found in metadata_raw/. Run step 2 first.")
        return

    summary_list = []
    field_inventory = []

    # Business priority fields to highlight for Phase 1E
    priority_fields = {
        "Account": [
            "Id",
            "Name",
            "Type",
            "Industry",
            "AnnualRevenue",
            "NumberOfEmployees",
            "Phone",
            "Website",
            "OwnerId",
            "CreatedDate",
            "SystemModstamp",
        ],
        "Contact": [
            "Id",
            "AccountId",
            "FirstName",
            "LastName",
            "Email",
            "Phone",
            "Title",
            "Department",
            "OwnerId",
            "CreatedDate",
            "SystemModstamp",
        ],
        "Lead": [
            "Id",
            "FirstName",
            "LastName",
            "Company",
            "Email",
            "Status",
            "LeadSource",
            "Rating",
            "IsConverted",
            "ConvertedAccountId",
            "ConvertedContactId",
            "ConvertedOpportunityId",
            "OwnerId",
            "CreatedDate",
            "SystemModstamp",
        ],
        "Opportunity": [
            "Id",
            "AccountId",
            "Name",
            "StageName",
            "Amount",
            "Probability",
            "CloseDate",
            "Type",
            "LeadSource",
            "IsWon",
            "IsClosed",
            "OwnerId",
            "CreatedDate",
            "SystemModstamp",
        ],
        "User": [
            "Id",
            "Username",
            "Name",
            "Email",
            "IsActive",
            "UserRole.Name",
            "CreatedDate",
            "SystemModstamp",
        ],
        "OpportunityContactRole": [
            "Id",
            "OpportunityId",
            "ContactId",
            "Role",
            "IsPrimary",
            "CreatedDate",
            "SystemModstamp",
        ],
    }

    print(f"📊 Analyzing {len(json_files)} metadata files...")

    for file_path in json_files:
        obj_name = os.path.basename(file_path).replace(".json", "")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        fields = data.get("fields", [])
        relationships = data.get("childRelationships", [])

        # Detect CDC / Incremental loading anchor
        has_system_modstamp = any(
            f["name"] == "SystemModstamp" for f in fields
        )

        summary_list.append(
            {
                "Object Name": obj_name,
                "Label": data.get("label", ""),
                "Total Fields": len(fields),
                "Child Relationships": len(relationships),
                "CDC Ready (SystemModstamp)": (
                    "Yes" if has_system_modstamp else "No"
                ),
            }
        )

        # Parse detailed fields for this object
        for field in fields:
            field_name = field["name"]
            is_priority = (
                obj_name in priority_fields
                and field_name in priority_fields[obj_name]
            )

            field_inventory.append(
                {
                    "Object": obj_name,
                    "Field API Name": field_name,
                    "Label": field["label"],
                    "Data Type": field["type"],
                    "Nillable": field["nillable"],
                    "Selected for Ingestion": (
                        "Required"
                        if is_priority
                        else "Optional / Evaluate"
                    ),
                }
            )

    # 1. Save overall summary
    df_summary = pd.DataFrame(summary_list)
    print("\n--- Object Metadata Summary ---")
    print(df_summary.to_string(index=False))

    # 2. Save complete field inventory CSV
    df_fields = pd.DataFrame(field_inventory)
    os.makedirs("docs/requirements", exist_ok=True)
    field_csv_path = "docs/requirements/selected_salesforce_fields.csv"
    df_fields.to_csv(field_csv_path, index=False)

    print(f"\n✅ Field inventory saved to: {field_csv_path}")

    # Display recommended count per object
    selected_counts = df_fields[
        df_fields["Selected for Ingestion"] == "Required"
    ]["Object"].value_counts()
    print("\n--- Core Recommended Fields per Object ---")
    print(selected_counts.to_string())


if __name__ == "__main__":
    analyze_analyzed_schemas = analyze_extracted_schemas()