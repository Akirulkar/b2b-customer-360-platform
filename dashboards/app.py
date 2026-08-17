import streamlit as st
import pandas as pd
import requests
import plotly.express as px

API_BASE_URL = "http://127.0.0.1:8000/api/v1"

st.set_page_config(
    page_title="B2B Customer 360 & Analytics",
    page_icon="📊",
    layout="wide",
)

st.title("🎯 B2B Customer 360 & Sales Intelligence Platform")


@st.cache_data(ttl=15)
def fetch_api_data(endpoint: str, params: dict | None = None):
    try:
        response = requests.get(f"{API_BASE_URL}/{endpoint}", params=params, timeout=5)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            # Expected when an entity or telemetry does not exist
            return None
        st.error(f"API Error ({endpoint} - {response.status_code}): {response.text}")
        return []
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to connect to FastAPI at {API_BASE_URL}: {e}")
        return []


# Sidebar Navigation
tab_selection = st.sidebar.radio(
    "Select Business View",
    [
        "Executive Overview",
        "Customer 360",
        "Sales Prioritization",
        "Sales Performance",
        "Lead Attribution",
    ],
)

# -------------------------------------------------------------
# 1. Executive Overview
# -------------------------------------------------------------
if tab_selection == "Executive Overview":
    st.header("Executive Summary")
    
    cust_data = fetch_api_data("customers", {"limit": 500})
    prio_data = fetch_api_data("prioritization", {"limit": 500})
    perf_data = fetch_api_data("sales-performance", {"limit": 500})
    
    df_cust = pd.DataFrame(cust_data)
    df_prio = pd.DataFrame(prio_data)
    df_perf = pd.DataFrame(perf_data)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_accounts = len(df_cust) if not df_cust.empty else 0
        st.metric("Total Accounts", total_accounts)
    with col2:
        open_pipeline = df_cust["open_pipeline_value"].sum() if not df_cust.empty else 0.0
        st.metric("Total Open Pipeline", f"${open_pipeline:,.0f}")
    with col3:
        won_revenue = df_cust["won_pipeline_value"].sum() if not df_cust.empty else 0.0
        st.metric("Total Won Revenue", f"${won_revenue:,.0f}")
    with col4:
        tier_1_count = len(df_prio[df_prio["priority_band"] == "Tier 1"]) if not df_prio.empty else 0
        st.metric("Tier 1 Target Accounts", tier_1_count)

    st.markdown("---")
    
    c_left, c_right = st.columns(2)
    with c_left:
        if not df_prio.empty:
            fig = px.pie(
                df_prio,
                names="priority_band",
                title="Account Priority Distribution",
                hole=0.4,
            )
            st.plotly_chart(fig, use_container_width=True)
    with c_right:
        if not df_perf.empty:
            fig = px.bar(
                df_perf,
                x="sales_rep_name",
                y=["won_pipeline_value", "open_pipeline_value"],
                title="Rep Revenue Breakdown",
                barmode="group",
            )
            st.plotly_chart(fig, use_container_width=True)

# -------------------------------------------------------------
# 2. Customer 360
# -------------------------------------------------------------
elif tab_selection == "Customer 360":
    st.header("Customer 360 Deep-Dive")
    cust_data = fetch_api_data("customers", {"limit": 500})
    
    if cust_data:
        df_cust = pd.DataFrame(cust_data)
        account_names = df_cust["account_name"].dropna().unique().tolist()
        selected_account_name = st.selectbox("Select Account", account_names)

        account_row = df_cust[df_cust["account_name"] == selected_account_name].iloc[0]
        account_id = account_row["account_id"]

        # Fetch intent details for this specific account
        # Fetch intent details for this specific account
        intent_info = fetch_api_data(f"intent/{account_id}")

        st.subheader("Digital Intent & Engagement Activity")
        if intent_info and isinstance(intent_info, dict) and "intent_score" in intent_info:
            ic1, ic2, ic3, ic4, ic5, ic6 = st.columns(6)
            ic1.metric("Intent Score", f"{intent_info.get('intent_score', 0):.1f}")
            ic2.metric("Site Visits", intent_info.get("website_visits", 0))
            ic3.metric("Product Views", intent_info.get("product_views", 0))
            ic4.metric("Pricing Views", intent_info.get("pricing_views", 0))
            ic5.metric("Doc Views", intent_info.get("documentation_views", 0))
            ic6.metric("Demo Requests", intent_info.get("demo_requests", 0))
        else:
            st.info("ℹ️ No digital intent or web telemetry recorded for this account.")
    else:
        st.warning("No customer records found.")

# -------------------------------------------------------------
# 3. Sales Prioritization
# -------------------------------------------------------------
elif tab_selection == "Sales Prioritization":
    st.header("Sales Target Prioritization")
    tier_filter = st.selectbox("Filter Tier", ["All", "Tier 1", "Tier 2", "Tier 3"])
    
    params = {"limit": 500}
    if tier_filter != "All":
        params["priority_band"] = tier_filter
        
    prio_data = fetch_api_data("prioritization", params)
    
    if prio_data:
        df_prio = pd.DataFrame(prio_data)
        st.dataframe(
            df_prio[[
                "account_name",
                "priority_band",
                "priority_score",
                "intent_score",
                "open_pipeline_value",
                "sales_rep_name",
            ]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No prioritization records found matching filter.")

# -------------------------------------------------------------
# 4. Sales Performance
# -------------------------------------------------------------
elif tab_selection == "Sales Performance":
    st.header("Sales Rep Performance Metrics")
    perf_data = fetch_api_data("sales-performance", {"limit": 500})
    
    if perf_data:
        df_perf = pd.DataFrame(perf_data)
        st.dataframe(
            df_perf[[
                "sales_rep_name",
                "month",
                "total_opportunities",
                "won_opportunities",
                "win_rate",
                "won_pipeline_value",
                "open_pipeline_value",
                "avg_deal_size",
            ]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No sales performance records found.")

# -------------------------------------------------------------
# 5. Lead Attribution
# -------------------------------------------------------------
elif tab_selection == "Lead Attribution":
    st.header("Lead Attribution & Conversion")
    lead_data = fetch_api_data("lead-attribution", {"limit": 500})
    
    if lead_data:
        df_lead = pd.DataFrame(lead_data)
        
        c1, c2 = st.columns(2)
        with c1:
            source_summary = (
                df_lead.groupby("lead_source")
                .agg(
                    total_leads=("lead_id", "count"),
                    converted_leads=("is_converted", "sum"),
                    won_deals=("is_won", "sum"),
                    won_revenue=("opportunity_amount", "sum"),
                )
                .reset_index()
            )
            st.dataframe(source_summary, use_container_width=True, hide_index=True)
        with c2:
            fig = px.bar(
                source_summary,
                x="lead_source",
                y="won_revenue",
                title="Revenue by Lead Source",
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No lead attribution records found.")