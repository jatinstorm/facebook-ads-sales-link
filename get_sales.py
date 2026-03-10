# get_sales.py
from bq import get_client

def get_sales(asins, isbns):

    client = get_client()

    asin_list = ",".join([f"'{a}'" for a in asins])
    isbn_list = ",".join([f"'{i}'" for i in isbns])

    ebook_query = f"""
    SELECT
        ASIN,
        Royalty_Date AS sale_date,
        CASE WHEN Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS Territory,
        SUM(Net_Units_Sold) AS ebook_units,
        SUM(Royalty_GBP) AS ebook_revenue
    FROM `storm-pub-amazon-sales.daily_sales.daily_sales_ebook_agg`
    WHERE Royalty_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND ASIN IN ({asin_list})
    GROUP BY ASIN, sale_date , Territory
    """

    paperback_query = f"""
    SELECT
        CAST(ISBN AS STRING) AS paperback_isbn,
        Royalty_Date AS sale_date,
        CASE WHEN Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS Territory,
        SUM(Net_Units_Sold) AS paperback_units,
        SUM(Royalty_GBP) AS paperback_revenue
    FROM `storm-pub-amazon-sales.daily_sales.daily_sales_paperback_agg`
    WHERE Royalty_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND CAST(ISBN AS STRING) IN ({isbn_list})
    GROUP BY ISBN, sale_date , Territory
    """

    kenp_query = f"""
    SELECT
        ASIN,
        Date AS sale_date,
        CASE WHEN Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS Territory,
        SUM(KENP) AS kenp
    FROM `storm-pub-amazon-sales.daily_sales.daily_sales_kenp_agg`
    WHERE Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND ASIN IN ({asin_list})
    GROUP BY ASIN, sale_date , Territory
    """

    

    ebook_df = client.query(ebook_query).to_dataframe()
    kenp_df = client.query(kenp_query).to_dataframe()
    paperback_df = client.query(paperback_query).to_dataframe()

    return ebook_df, kenp_df, paperback_df