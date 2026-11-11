import snowflake.snowpark.functions as F


def model(dbt, session):
    # a dbt Python model: Snowpark turns this into Snowflake SQL, which runs on the emulator
    dbt.config(materialized="table")
    users = dbt.ref("dim_users")
    return users.group_by("PLAN").agg(F.count("USER_ID").alias("USERS"))
