import os
import logging
from typing import Dict, List, Any
import requests
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from ingestion.utils.logger import logger

load_dotenv()




class SalesforceClient:
    """Authenticates via OAuth 2.0 and executes SOQL queries."""

    def __init__(self):
        self.consumer_key = os.getenv("SF_CONSUMER_KEY")
        self.consumer_secret = os.getenv("SF_CONSUMER_SECRET")
        self.my_domain_url = os.getenv("SF_MY_DOMAIN_URL", "").rstrip("/")
        self.username = os.getenv("SF_USERNAME")
        self.password = os.getenv("SF_PASSWORD")
        self.sf: Salesforce = None
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticates with Salesforce via OAuth2 token endpoint."""
        if not self.my_domain_url:
            raise ValueError("SF_MY_DOMAIN_URL is not set in .env")

        token_url = f"{self.my_domain_url}/services/oauth2/token"

        # Try client_credentials first
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.consumer_key,
            "client_secret": self.consumer_secret
        }

        # Fallback to password credentials if username is provided and needed
        if self.username and self.password:
            # If client_credentials fails or if using Password/Credentials flow
            pass

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        try:
            logger.info(f"Authenticating via OAuth2 endpoint: {token_url}")
            response = requests.post(token_url, data=payload, headers=headers)
            
            # If client_credentials isn't enabled on ECA, try password flow automatically
            if response.status_code == 400 and "unsupported_grant_type" in response.text and self.username:
                logger.info("Attempting credentials flow with username/password...")
                payload = {
                    "grant_type": "password",
                    "client_id": self.consumer_key,
                    "client_secret": self.consumer_secret,
                    "username": self.username,
                    "password": self.password
                }
                response = requests.post(token_url, data=payload, headers=headers)

            response.raise_for_status()
            
            auth_data = response.json()
            access_token = auth_data["access_token"]
            instance_url = auth_data.get("instance_url", self.my_domain_url)
            
            self.sf = Salesforce(
                instance_url=instance_url,
                session_id=access_token
            )
            logger.info("Salesforce authentication successful.")
        except requests.exceptions.RequestException as e:
            logger.error(f"OAuth token request failed: {str(e)}")
            if response is not None and response.text:
                logger.error(f"Response details: {response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected connection error: {str(e)}")
            raise

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Executes a SOQL query and fetches all records."""
        if not self.sf:
            self._authenticate()

        logger.info("Executing SOQL Query...")
        try:
            results = self.sf.query_all(query)
            records = results.get("records", [])
            logger.info(f"Retrieved {len(records)} records from Salesforce.")

            for record in records:
                record.pop("attributes", None)

            return records
        except Exception as e:
            logger.error(f"Error executing SOQL query: {str(e)}")
            raise


if __name__ == "__main__":
    client = SalesforceClient()
    test_data = client.execute_query("SELECT Id, Name, SystemModstamp FROM Account LIMIT 5")
    print(f"Smoke test successful! Retrieved {len(test_data)} records.")
    if test_data:
        print("Sample Account:", test_data[0])