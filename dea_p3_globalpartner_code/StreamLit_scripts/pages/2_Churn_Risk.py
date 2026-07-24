import streamlit as st
import plotly.express as px
from utils import query

st.set_page_config(page_title="Churn Risk Indicators", layout="wide")
st.title("Churn Risk Indicators")
st.caption(
    "Which metrics - days since last order, order interval, spend trend - "
    "correlate with higher churn risk, and who needs re-engagement now?"
)

df = query(
    """
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY snapshot_date DESC) AS rn
        FROM fact_customer_daily
    )
    SELECT user_id, clv_to_date, days_since_last_order, frequency_trailing, monetary_trailing,
           spend_change_pct, churn_risk, rfm_segment
    FROM latest WHERE rn = 1
    """
)

at_risk = df[df["churn_risk"]]

col1, col2, col3 = st.columns(3)
col1.metric("Customers at risk (>45 days idle)", f"{len(at_risk):,}")
col2.metric("Share of customer base", f"{len(at_risk) / len(df) * 100:.1f}%")
col3.metric("CLV at risk", f"${at_risk['clv_to_date'].sum():,.2f}")

fig1 = px.histogram(
    df,
    x="days_since_last_order",
    color="churn_risk",
    nbins=40,
    title="Days Since Last Order (dashed line = 45-day churn threshold)",
)
fig1.add_vline(x=45, line_dash="dash", line_color="red")
st.plotly_chart(fig1, use_container_width=True)

st.plotly_chart(
    px.scatter(
        df,
        x="days_since_last_order",
        y="spend_change_pct",
        color="churn_risk",
        hover_data=["user_id"],
        title="Spend Change % vs. Recency",
    ),
    use_container_width=True,
)

st.subheader("Highest-value customers at risk - prioritize outreach here")
st.dataframe(
    at_risk.sort_values("clv_to_date", ascending=False)[
        ["user_id", "clv_to_date", "days_since_last_order", "spend_change_pct"]
    ],
    use_container_width=True,
)
