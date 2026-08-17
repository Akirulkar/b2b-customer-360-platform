"""
Frozen Phase 1 SOQL Query definitions.
Supports both FULL and INCREMENTAL extractions via string format injection.
"""

ACCOUNT_QUERY = """
SELECT
    Id,
    Name,
    Type,
    Phone,
    Website,
    Industry,
    AnnualRevenue,
    NumberOfEmployees,
    OwnerId,
    CreatedDate,
    SystemModstamp
FROM Account
{where_clause}
"""

CONTACT_QUERY = """
SELECT
    Id,
    AccountId,
    LastName,
    FirstName,
    Phone,
    Email,
    Title,
    Department,
    OwnerId,
    CreatedDate,
    SystemModstamp
FROM Contact
{where_clause}
"""

LEAD_QUERY = """
SELECT
    Id,
    LastName,
    FirstName,
    Company,
    Email,
    LeadSource,
    Status,
    Rating,
    OwnerId,
    IsConverted,
    ConvertedAccountId,
    ConvertedContactId,
    ConvertedOpportunityId,
    CreatedDate,
    SystemModstamp
FROM Lead
{where_clause}
"""

OPPORTUNITY_QUERY = """
SELECT
    Id,
    AccountId,
    Name,
    StageName,
    Amount,
    Probability,
    CloseDate,
    Type,
    LeadSource,
    IsClosed,
    IsWon,
    OwnerId,
    CreatedDate,
    SystemModstamp
FROM Opportunity
{where_clause}
"""

OPPORTUNITY_CONTACT_ROLE_QUERY = """
SELECT
    Id,
    OpportunityId,
    ContactId,
    Role,
    IsPrimary,
    CreatedDate,
    SystemModstamp
FROM OpportunityContactRole
{where_clause}
"""

USER_QUERY = """
SELECT
    Id,
    Username,
    Name,
    Email,
    IsActive,
    CreatedDate,
    SystemModstamp,
    UserRole.Name
FROM User
{where_clause}
"""

SOQL_REGISTRY = {
    "account": ACCOUNT_QUERY,
    "contact": CONTACT_QUERY,
    "lead": LEAD_QUERY,
    "opportunity": OPPORTUNITY_QUERY,
    "opportunity_contact_role": OPPORTUNITY_CONTACT_ROLE_QUERY,
    "user": USER_QUERY
}


def build_soql(object_name: str, watermark_timestamp: str = None) -> str:
    """
    Constructs a SOQL query string for the target object.
    Appends SystemModstamp filtering if a watermark_timestamp is supplied.
    """
    if object_name not in SOQL_REGISTRY:
        raise ValueError(f"Unknown object_name '{object_name}'. Supported: {list(SOQL_REGISTRY.keys())}")

    base_query = SOQL_REGISTRY[object_name]
    where_clause = ""
    
    if watermark_timestamp:
        # ISO-8601 UTC timestamp format for Salesforce SOQL
        where_clause = f"WHERE SystemModstamp > {watermark_timestamp}"

    return base_query.format(where_clause=where_clause).strip()