"""
Simple Data Pipeline
"""
import sys
from pathlib import Path
import logging
import sqlite3
import pandas as pd


def create_connection(db_file:str) -> sqlite3.Connection:
    """
    Utility function - creates connection to SQLite database
    :param db_file: database file path
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as error:
        logging.error(error)

    return conn


def configure_logging():
    """
    Utility function - configures the data pipeline logging
    """
    # configure the logging
    log_file_path = Path('pipeline_log.txt')
    logging.basicConfig(filename=log_file_path,
                        format='%(asctime)s -- [%(levelname)s]: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def get_sales_by_month_sql(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Get total sales by month using SQL to aggregate the data
    :param: conn: connection to database
    :return: DataFrame with sales by month (Month, Quantity, TotalSales)
    """
    sql_query = """
        SELECT ROUND(SUM(ii.Quantity), 2) as Quantity,
        ROUND(SUM(ii.UnitPrice), 2) as TotalSales,
        CAST(strftime('%Y', i.InvoiceDate) as text) || "-" || 
            CAST(strftime('%m', i.InvoiceDate) as text) as Month
        FROM invoice_items ii
        INNER JOIN invoices i ON i.InvoiceId = ii.InvoiceId
		GROUP BY Month;
    """
    monthly_sales_df = pd.read_sql_query(sql_query, conn)
    return monthly_sales_df


def get_sales_by_month_pd(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Get total sales by month using Pandas to aggregate the data
    :param: conn: connection to database
    :return: DataFrame with sales by month (Month, Quantity, TotalSales)
    """
    sql_query = """
        SELECT ii.Quantity as Quantity,
        ii.UnitPrice as UnitPrice,
        i.InvoiceDate as InvoiceDate
        FROM invoice_items ii
        INNER JOIN invoices i ON i.InvoiceId = ii.InvoiceId;
    """
    monthly_sales_df = pd.read_sql_query(sql_query, conn)
    
    # add the month column in YYYY-MM format
    monthly_sales_df['Month'] = pd.to_datetime(monthly_sales_df['InvoiceDate']).dt.to_period('M')
    
    # drop the raw date column
    monthly_sales_df.drop('InvoiceDate', axis=1)

    #group by month and sum the Price and Quantity
    monthly_sales_df = monthly_sales_df.groupby('Month').agg(
        TotalSales=('UnitPrice', sum),
        Quantity=('Quantity', sum),
        Month=('Month','first'))

    return monthly_sales_df


def get_top_artists_by_sales(num_results:int, conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Get the top N artists by total sales
    :param: num_results: the number of results to return
    :param: conn: connection to database
    :return: DataFrame with sales by Artist (Artist, Quantity, TotalSales)
    """
    sql_query = """
        SELECT ROUND(SUM(ii.Quantity), 2) as Quantity,
        ROUND(SUM(ii.UnitPrice), 2) as TotalSales,
        ar.Name as ArtistName
        FROM invoice_items ii
        INNER JOIN invoices i ON i.InvoiceId = ii.InvoiceId
        INNER JOIN tracks t ON t.TrackId = ii.TrackId
        INNER JOIN albums al ON al.AlbumId = t.AlbumId
        INNER JOIN artists ar ON ar.ArtistId = al.ArtistId
        GROUP BY ArtistName
        ORDER BY TotalSales DESC
        LIMIT ?
    """
    artist_sales_df = pd.read_sql_query(sql_query, conn, params=(num_results, ))
    return artist_sales_df


def main():
    """
    Data Pipeline Process Control
    """
    configure_logging()
    logging.info("Starting data pipeline process")

    # create a database connection
    conn = create_connection(Path("data/chinook.db"))

    # run the data pipeline steps
    with conn:

        logging.info("Extracting sales data from database")

        # there are two example approaches for this function
        # Approach 1: format the month and calculate the total sales in SQL
        # monthly_sales_df = get_sales_by_month_sql(conn)
        # Approach 2: format the month and calculate the total sales in Pandas
        monthly_sales_df = get_sales_by_month_pd(conn)

        logging.info("Saving the sales data as CSV")
        monthly_sales_df.to_csv(Path('data/sales.csv'), index=False)

        logging.info("Extracting top 10 Artists by TotalSales")
        sales_by_artist_df = get_top_artists_by_sales(10, conn)

        logging.info("Saving sales by artist data as CSV")
        sales_by_artist_df.to_csv(Path('data/sales_by_artist.csv'), index=False)

    conn.close()

    logging.info("Finishing data pipeline process")


if __name__ == "__main__":
    main()
