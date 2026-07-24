import streamlit as st
import plotly.express as px
from utils import query

st.set_page_config(page_title="Sales Trends & Seasonality", layout="wide")
st.title("Sales Trends & Seasonality")
st.caption(
    "Monthly and seasonal sales trends, by product category and location, "
    "to support inventory and staffing decisions."
)

df = query(
    """
    SELECT order_date, year, month, week, is_weekend, is_holiday, restaurant_id,
           item_category, net_revenue, items_sold, order_count
    FROM sales_trends_agg
    """
)

monthly = df.groupby(["year", "month"], as_index=False)["net_revenue"].sum()
st.plotly_chart(
    px.line(monthly, x="month", y="net_revenue", color="year", markers=True, title="Monthly Revenue Trend"),
    use_container_width=True,
)

cat = (
    df.groupby("item_category", as_index=False)["net_revenue"]
    .sum()
    .sort_values("net_revenue", ascending=False)
)
st.plotly_chart(
    px.bar(cat, x="item_category", y="net_revenue", title="Revenue by Menu Category"),
    use_container_width=True,
)

daily = df.groupby("order_date", as_index=False).agg(
    net_revenue=("net_revenue", "sum"), is_holiday=("is_holiday", "max")
)
fig_daily = px.line(daily, x="order_date", y="net_revenue", title="Daily Revenue (holidays marked)")
holidays = daily[daily["is_holiday"]]
if not holidays.empty:
    fig_daily.add_scatter(
        x=holidays["order_date"],
        y=holidays["net_revenue"],
        mode="markers",
        name="Holiday",
        marker=dict(color="red", size=8),
    )
st.plotly_chart(fig_daily, use_container_width=True)

weekend_comp = df.groupby("is_weekend", as_index=False)["net_revenue"].sum()
weekend_comp["label"] = weekend_comp["is_weekend"].map({True: "Weekend", False: "Weekday"})
st.plotly_chart(
    px.bar(weekend_comp, x="label", y="net_revenue", title="Weekday vs. Weekend Revenue"),
    use_container_width=True,
)
