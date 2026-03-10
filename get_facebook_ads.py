# get_facebook_ads.py
from bq import get_client

def get_facebook_ads():

    client = get_client()

    query = """
    SELECT
        adset_name,
        Edition_ID,
        Territory,
        Targeting_type,
        Targeting,
        Age_range,
        DATE(date_start) AS date_start,
        SUM(spend) AS spend,
        SUM(clicks) AS clicks,
        AVG(cpc) AS cpc,
        AVG(ctr) AS ctr,
        SUM(impressions) AS impressions
    FROM `marketing-489109.facebook_ads.facebook_ads`
    WHERE DATE(date_start) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND Territory NOT IN ('USGB', 'GBUS')
    GROUP BY
        Edition_ID,
        Territory,
        Targeting_type,
        Targeting,
        Age_range,
        adset_name,
        date_start
    """

    df = client.query(query).to_dataframe()

    return df