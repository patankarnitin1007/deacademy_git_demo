import streamlit as st
import plotly.express as px
from utils import query

st.set_page_config(page_title="Location Performance", layout="wide")
st.title("Location Performance")
st.caption(
    "Which restaurant locations generate the most revenue, and what "
    "operational metrics distinguish top performers?"
)

df = query("SELECT * FROM location_performance_agg ORDER BY revenue_rank")

st.plotly_chart(
    px.bar(df, x="restaurant_id", y="total_net_revenue", title="Revenue by Location (ranked)"),
    use_container_width=True,
)

st.plotly_chart(
    px.scatter(
        df,
        x="avg_orders_per_day",
        y="avg_order_value",
        size="total_net_revenue",
        hover_data=["restaurant_id"],
        title="Order Volume vs. Order Size (bubble size = total revenue)",
    ),
    use_container_width=True,
)

st.subheader("Full ranking")
st.dataframe(df, use_container_width=True)
