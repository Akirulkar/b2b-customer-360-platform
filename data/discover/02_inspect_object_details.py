import json
import os
import subprocess


def dump_object_schemas(objects_to_inspect, alias="b2b-dev-org"):
    os.makedirs("metadata_raw", exist_ok=True)

    for obj in objects_to_inspect:
        print(f"📦 Extracting schema for: {obj}...")
        cmd = f"sf sobject describe --sobject {obj} --target-org {alias} --json"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        try:
            payload = json.loads(result.stdout)
            if payload.get("status") == 0:
                with open(f"metadata_raw/{obj}.json", "w") as f:
                    json.dump(payload["result"], f, indent=2)
                print(f"   ✓ Saved metadata_raw/{obj}.json")
            else:
                print(f"   ⚠️ Could not describe {obj}: {payload.get('message')}")
        except Exception as e:
            print(f"   ❌ Error parsing response for {obj}: {e}")


if __name__ == "__main__":
    # Candidate list after reviewing discovered objects
    target_objects = [
        "Account",
        "Contact",
        "Lead",
        "Opportunity",
        "User",
        "OpportunityContactRole",
        "Pricebook2",
        "OpportunityLineItem",
        "Campaign",
        "CampaignMember",
        "Task",
        "Event",
    ]

    dump_object_schemas(target_objects)