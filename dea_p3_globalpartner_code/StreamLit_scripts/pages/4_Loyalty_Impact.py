import streamlit as st
import plotly.express as px
from utils import query

st.set_page_config(page_title="Loyalty Program Impact", layout="wide")
st.title("Loyalty Program Impact")
st.caption("How does loyalty membership affect customer spending and repeat order rates?")

df = query("SELECT * FROM loyalty_impact_agg")
df["segment"] = df["is_loyalty"].map({True: "Loyalty Member", False: "Non-Member"})

cols = st.columns(len(df))
for col, (_, row) in zip(cols, df.iterrows()):
    col.metric(row["segment"], f"${row['avg_order_value']:.2f} AOV", f"{row['repeat_customer_rate']:.1f}% repeat rate")

st.plotly_chart(
    px.bar(df, x="segment", y="avg_order_value", title="Average Order Value"),
    use_container_width=True,
)
st.plotly_chart(
    px.bar(df, x="segment", y="repeat_customer_rate", title="Repeat Customer Rate (%)"),
    use_container_width=True,
)
st.plotly_chart(
    px.bar(df, x="segment", y="avg_orders_per_customer", title="Average Orders per Customer (proxy for CLV)"),
    use_container_width=True,
)

st.subheader("Detail")
st.dataframe(df.drop(columns=["is_loyalty"]), use_container_width=True)
