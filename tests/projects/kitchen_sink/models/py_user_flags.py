def model(dbt, session):
    # the same through pandas: read, transform in Python, hand a DataFrame back
    dbt.config(materialized="table")
    users = dbt.ref("dim_users").to_pandas()
    users["HAS_EMAIL"] = users["EMAIL"].notna()
    return users[["USER_ID", "HAS_EMAIL"]]
