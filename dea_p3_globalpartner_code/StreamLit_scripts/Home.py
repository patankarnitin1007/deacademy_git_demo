import streamlit as st

st.set_page_config(page_title="Restaurant Insights Platform", layout="wide")

st.title("Restaurant Insights Platform")
st.write(
    """
    Use the sidebar to open a dashboard. Each page queries Amazon Athena
    directly against the Gold layer, which the Glue pipeline
    (Bronze -> Silver -> Gold) refreshes on its schedule.

    - **Customer Segmentation** - RFM-based groups for targeted marketing
    - **Churn Risk Indicators** - who's going quiet, and how much CLV is at stake
    - **Sales Trends & Seasonality** - revenue by month, category, and location
    - **Loyalty Program Impact** - members vs. non-members
    - **Location Performance** - revenue ranking across restaurants
    - **Pricing & Discount Effectiveness** - discounted vs. full-price orders
    """
)
