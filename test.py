from bq import get_client

client = get_client()

query = """
SELECT 1 as test
"""

df = client.query(query).to_dataframe()

print(df)