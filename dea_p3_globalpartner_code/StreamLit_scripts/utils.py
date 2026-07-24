import awswrangler as wr
import streamlit as st

# Update these two for your environment. AWS credentials are picked up the
# normal boto3 way: local ~/.aws/credentials during development, or the
# ECS task role once this is deployed (see the CI/CD steps discussed
# earlier) - nothing AWS-specific needs to live in this file.
ATHENA_DATABASE = "dea_p3_globalpartner"
ATHENA_S3_STAGING_DIR = "s3://xxdea-p3-globalpartners-data-bucket/athena-results/"


@st.cache_data(ttl=600)
def query(sql: str):
    """Run a query against Athena and return a pandas DataFrame.

    Cached for 10 minutes so navigating between dashboards doesn't
    re-run Athena queries on every click - the data only changes once
    a day when the pipeline runs.

    ctas_approach=False: the default CTAS mode creates and then deletes a
    temporary Glue table per query, which needs glue:CreateTable and
    glue:DeleteTable on top of read access. This app only needs to read,
    so the plain query path keeps the IAM policy read-only.
    """
    return wr.athena.read_sql_query(
        sql, database=ATHENA_DATABASE, s3_output=ATHENA_S3_STAGING_DIR, ctas_approach=False
    )
