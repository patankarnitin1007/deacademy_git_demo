import streamlit as st
import plotly.express as px
from utils import query

st.set_page_config(page_title="Customer Segmentation", layout="wide")
st.title("Customer Segmentation")
st.caption(
    "What distinct customer segments emerge when grouping customers by "
    "total spend, frequency, recency, and loyalty status?"
)

df = query(
    """
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY snapshot_date DESC) AS rn
        FROM fact_customer_daily
    )
    SELECT user_id, clv_to_date, clv_tier, frequency_trailing, monetary_trailing,
           recency_score, frequency_score, rfm_segment, days_since_last_order
    FROM latest WHERE rn = 1
    """
)

col1, col2, col3 = st.columns(3)
col1.metric("Customers", f"{df['user_id'].nunique():,}")
col2.metric("VIP customers", f"{(df['rfm_segment'] == 'VIP').sum():,}")
col3.metric("Avg CLV", f"${df['clv_to_date'].mean():,.2f}")

seg_counts = df["rfm_segment"].value_counts().reset_index()
seg_counts.columns = ["segment", "customers"]
st.plotly_chart(
    px.bar(seg_counts, x="segment", y="customers", title="Customers by RFM Segment"),
    use_container_width=True,
)

tier_counts = df["clv_tier"].value_counts().reset_index()
tier_counts.columns = ["tier", "customers"]
st.plotly_chart(
    px.pie(tier_counts, names="tier", values="customers", title="CLV Tier Distribution (High / Medium / Low)"),
    use_container_width=True,
)

st.plotly_chart(
    px.scatter(
        df,
        x="frequency_trailing",
        y="monetary_trailing",
        color="rfm_segment",
        hover_data=["user_id", "clv_to_date"],
        title="Frequency vs. Monetary (trailing window), colored by segment",
    ),
    use_container_width=True,
)

st.subheader("Customer detail")
st.dataframe(df.sort_values("clv_to_date", ascending=False), use_container_width=True)
