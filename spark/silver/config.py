"""Configuration constants for Phase 5 Silver Layer."""

from typing import Set

BRONZE_BASE_PATH: str = "data/bronze"
SILVER_BASE_PATH: str = "data/silver"

# Excluded free/public email providers for domain matching waterfall Tier 4
PUBLIC_EMAIL_DOMAINS: Set[str] = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "aol.com",
    "protonmail.com",
    "zoho.com",
    "mail.com",
}

# Standardized semantic weights for digital interactions
INTERACTION_WEIGHTS = {
    "website_visit": "low",
    "product_page_view": "medium",
    "documentation_view": "medium",
    "brochure_download": "medium_high",
    "pricing_page_view": "high",
    "demo_request": "very_high",
}