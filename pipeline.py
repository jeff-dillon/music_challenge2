from multiprocessing.connection import Connection
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
    except sqlite3.Error as e:
        logging.error(e)

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


def get_sales_by_month(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Get total sales by month
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
    df = pd.read_sql_query(sql_query, conn)
    
    return df
    

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
    df = pd.read_sql_query(sql_query, conn, params=(num_results, ))
    return df


def save_sales(sales_df:pd.DataFrame):
    """
    Save the sales data to CSV file
    :param: sales_df: DataFrame with sales data
    """
    file_path = Path('data/sales.csv')
    sales_df.to_csv(file_path, index=False)


def save_sales_by_artist(sales_by_artist_df:pd.DataFrame):
    """
    Save the sales by artist data to CSV file
    :param: sales_by_artist_df: DataFrame with sales by artist data
    """
    file_path = Path('data/sales_by_artist.csv')
    sales_by_artist_df.to_csv(file_path, index=False)


def main():
    configure_logging()    
    logging.info("Starting data pipeline process")

    # create a database connection
    database_file_path = Path("data/chinook.db")
    conn = create_connection(database_file_path)
    if not conn:
        logging.ERROR("error connecting to database")
        quit()

    # run the data pipeline steps
    with conn:
        
        logging.info("Extracting sales data from database.")
        sales_df = get_sales_by_month(conn)

        logging.info("Saving the sales data as CSV.")
        save_sales(sales_df)

        logging.info("Extracting top 10 Artists by TotalSales")
        sales_by_artist_df = get_top_artists_by_sales(10, conn)

        logging.info("Saving sales by artist")
        save_sales_by_artist(sales_by_artist_df)
    
    conn.close()

    logging.info("Finishing data pipeline process")


if __name__ == "__main__":
    main()