import pandas as pd
import streamlit as st
import plotly.express as px
from utils import query

st.set_page_config(page_title="Pricing & Discount Effectiveness", layout="wide")
st.title("Pricing & Discount Effectiveness")
st.caption("How are discounts and promotions affecting sales volume and net revenue?")

df = query("SELECT * FROM discount_effectiveness_agg")
df["segment"] = df["has_discount"].map({True: "Discounted", False: "Full Price"})

discounted_row = df[df["has_discount"]]
full_price_row = df[~df["has_discount"]]

col1, col2, col3 = st.columns(3)
col1.metric("Total discount given", f"${df['discount_amount'].sum():,.2f}")
col2.metric(
    "Discounted orders",
    f"{int(discounted_row['order_count'].iloc[0]):,}" if not discounted_row.empty else "0",
)
col3.metric(
    "Full-price orders",
    f"{int(full_price_row['order_count'].iloc[0]):,}" if not full_price_row.empty else "0",
)

revenue_long = df.melt(
    id_vars="segment", value_vars=["gross_revenue", "net_revenue"], var_name="metric", value_name="amount"
)
st.plotly_chart(
    px.bar(revenue_long, x="segment", y="amount", color="metric", barmode="group", title="Gross vs. Net Revenue"),
    use_container_width=True,
)

st.plotly_chart(
    px.bar(df, x="segment", y="avg_order_value", title="Average Order Value"),
    use_container_width=True,
)

st.subheader("Detail")
st.dataframe(df.drop(columns=["has_discount"]), use_container_width=True)
