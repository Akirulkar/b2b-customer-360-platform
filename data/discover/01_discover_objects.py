import json
import os
import subprocess
import pandas as pd


def discover_all_objects(alias="b2b-dev-org"):
    print("🔑 Fetching global sobject metadata using Salesforce CLI...")

    # Query the REST describe endpoint directly via sf CLI
    cmd = f"sf api request rest /services/data/v60.0/sobjects --target-org {alias}"
    
    # Set environment variable to suppress beta warnings from sf CLI output
    env = os.environ.copy()
    env["SF_SUPPRESS_WARNINGS"] = "true"

    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, env=env
    )

    if result.returncode != 0:
        raise Exception(
            f"CLI request failed (exit code {result.returncode}):\n{result.stderr or result.stdout}"
        )

    stdout_clean = result.stdout.strip()
    if not stdout_clean:
        raise Exception("CLI request succeeded but returned empty output.")

    # Find where the JSON payload starts in case any CLI header/warning text crept in
    json_start = stdout_clean.find("{")
    if json_start != -1:
        stdout_clean = stdout_clean[json_start:]

    try:
        sobjects_data = json.loads(stdout_clean)
    except json.JSONDecodeError as e:
        raise Exception(f"Failed to parse JSON output: {e}\nRaw stdout: {result.stdout}")

    sobjects = sobjects_data.get("sobjects", [])
    if not sobjects:
        raise Exception("No sobjects returned from Salesforce API call.")

    object_list = []
    for obj in sobjects:
        name = obj["name"]
        if name.endswith("__c"):
            obj_type = "Custom Object"
        elif name.endswith("__change_event"):
            obj_type = "CDC Event Object"
        elif name.endswith("__Share") or name.endswith("__History"):
            obj_type = "System / Security"
        else:
            obj_type = "Standard Object"

        object_list.append(
            {
                "API Name": name,
                "Label": obj["label"],
                "Type": obj_type,
                "Queryable": obj["queryable"],
                "Retrieveable": obj["retrieveable"],
                "Createable": obj["createable"],
                "Updateable": obj["updateable"],
                "Custom": obj["custom"],
            }
        )

    df = pd.DataFrame(object_list)

    # Ensure output directory exists
    os.makedirs("docs/requirements", exist_ok=True)

    csv_path = "docs/requirements/discovered_salesforce_objects.csv"
    df.to_csv(csv_path, index=False)
    print(f"✅ Discovered {len(df)} total objects.")
    print(f"📁 Full object inventory saved to: {csv_path}\n")

    print("--- Object Breakdown ---")
    print(df["Type"].value_counts().to_string())

    custom_df = df[df["Custom"] == True]
    if not custom_df.empty:
        print("\n--- Custom Objects Found in Org ---")
        print(custom_df[["API Name", "Label"]].to_string(index=False))


if __name__ == "__main__":
    discover_all_objects()