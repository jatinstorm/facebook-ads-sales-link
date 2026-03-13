# get_asin.py
from bq import get_client

def get_asin(edition_ids):

    client = get_client()

    edition_ids_string = ",".join([str(i) for i in edition_ids])

    
    query = f"""
        SELECT
            e.ID AS Edition_ID,
            e.Title,
            MIN(eb.ASIN) AS ASIN,
            MIN(p.ISBN) AS paperback_isbn,
            MIN(e.Genre) AS Genre,
            MIN(e.Genre_Subgenre) AS Genre_Subgenre
        FROM `storm-pub-amazon-sales.airtable.awe_editions` e
        LEFT JOIN `storm-pub-amazon-sales.airtable.awe_editions` eb
            ON e.Title = eb.Title
            AND eb.Format = 'Ebook'
        LEFT JOIN `storm-pub-amazon-sales.airtable.awe_editions` p
            ON e.Title = p.Title
            AND p.Format = 'POD'
        WHERE e.ID IN ({edition_ids_string})
        GROUP BY e.ID, e.Title
        """

    df = client.query(query).to_dataframe()

    return df