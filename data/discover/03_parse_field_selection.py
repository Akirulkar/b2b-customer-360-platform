import glob
import json
import os
import pandas as pd


def analyze_extracted_schemas():
    json_files = glob.glob("metadata_raw/*.json")

    if not json_files:
        print("❌ No JSON files found in metadata_raw/. Run step 2 first.")
        return

    field_inventory = []

    # Explicit priority map distinguishing Direct vs Relationship fields
    priority_fields = {
        "Account": {
            "direct": [
                "Id",
                "Name",
                "Type",
                "Phone",
                "Website",
                "Industry",
                "AnnualRevenue",
                "NumberOfEmployees",
                "OwnerId",
                "CreatedDate",
                "SystemModstamp",
            ],
            "relationship": [],
        },
        "Contact": {
            "direct": [
                "Id",
                "AccountId",
                "FirstName",
                "LastName",
                "Phone",
                "Email",
                "Title",
                "Department",
                "OwnerId",
                "CreatedDate",
                "SystemModstamp",
            ],
            "relationship": [],
        },
        "Lead": {
            "direct": [
                "Id",
                "FirstName",
                "LastName",
                "Company",
                "Email",
                "LeadSource",
                "Status",
                "Rating",
                "OwnerId",
                "IsConverted",
                "ConvertedAccountId",
                "ConvertedContactId",
                "ConvertedOpportunityId",
                "CreatedDate",
                "SystemModstamp",
            ],
            "relationship": [],
        },
        "Opportunity": {
            "direct": [
                "Id",
                "AccountId",
                "Name",
                "StageName",
                "Amount",
                "Probability",
                "CloseDate",
                "Type",
                "LeadSource",
                "IsClosed",
                "IsWon",
                "OwnerId",
                "CreatedDate",
                "SystemModstamp",
            ],
            "relationship": [],
        },
        "OpportunityContactRole": {
            "direct": [
                "Id",
                "OpportunityId",
                "ContactId",
                "Role",
                "IsPrimary",
                "CreatedDate",
                "SystemModstamp",
            ],
            "relationship": [],
        },
        "User": {
            "direct": [
                "Id",
                "Username",
                "Name",
                "Email",
                "IsActive",
                "CreatedDate",
                "SystemModstamp",
            ],
            "relationship": ["UserRole.Name"],
        },
    }

    print(f"📊 Analyzing {len(json_files)} metadata files...")

    for file_path in json_files:
        obj_name = os.path.basename(file_path).replace(".json", "")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        fields = data.get("fields", [])

        # Process standard direct fields
        for field in fields:
            field_name = field["name"]
            is_direct = (
                obj_name in priority_fields
                and field_name in priority_fields[obj_name]["direct"]
            )

            field_inventory.append(
                {
                    "Object": obj_name,
                    "Field API Name": field_name,
                    "Field Type": "Direct",
                    "Label": field["label"],
                    "Data Type": field["type"],
                    "Nillable": field["nillable"],
                    "Selected for Ingestion": (
                        "Required" if is_direct else "Optional / Evaluate"
                    ),
                }
            )

        # Append relationship fields manually if defined
        if obj_name in priority_fields:
            for rel_field in priority_fields[obj_name]["relationship"]:
                field_inventory.append(
                    {
                        "Object": obj_name,
                        "Field API Name": rel_field,
                        "Field Type": "Relationship Traversal",
                        "Label": f"{rel_field} (SOQL Relationship Traversal)",
                        "Data Type": "reference_field",
                        "Nillable": True,
                        "Selected for Ingestion": "Required",
                    }
                )

    df_fields = pd.DataFrame(field_inventory)
    os.makedirs("docs/requirements", exist_ok=True)
    field_csv_path = "docs/requirements/selected_salesforce_fields.csv"
    df_fields.to_csv(field_csv_path, index=False)

    print(f"\n✅ Field inventory saved to: {field_csv_path}")

    # Explicit summary verification
    required_df = df_fields[df_fields["Selected for Ingestion"] == "Required"]
    print("\n--- Field Counts per Object ---")
    summary = (
        required_df.groupby(["Object", "Field Type"])
        .size()
        .unstack(fill_value=0)
    )
    summary["Total Required"] = summary.sum(axis=1)
    print(summary)
    print(f"\nTotal Selected Items: {required_df.shape[0]}")


if __name__ == "__main__":
    analyze_extracted_schemas()