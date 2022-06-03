import logging
import sqlite3
import pandas as pd
from pathlib import Path

def create_connection(db_file:str):
    """
    utility function - creates connection to sqlite database
    :param db_file: database file path
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logging.error(e)

    return conn


def get_sales(conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Get the list of Track Sales from the database
    :param: conn: connection to database
    :return: all genres
    """

    sql_query = """
        SELECT ROUND(SUM(ii.Quantity), 2) as Quantity,
                ROUND(SUM(ii.UnitPrice), 2) as TotalSales,
				CAST(strftime('%Y', i.InvoiceDate) as text) || "-" || CAST(strftime('%m', i.InvoiceDate) as text) as Month
        FROM invoice_items ii
        INNER JOIN invoices i ON i.InvoiceId = ii.InvoiceId
		GROUP BY Month;
    """
    df = pd.read_sql_query(sql_query, conn)
    
    return df
    

def get_sales_by_artist(artist_id:int, conn:sqlite3.Connection) -> pd.DataFrame:
    """
    Get the list of tracks by Artist
    :param: artist_id: the ID of the artist
    :param: conn: connection to database
    :return: all tracks in the specified genre
    """
    sql_query = """
        SELECT ROUND(SUM(ii.Quantity), 2) as Quantity,
        ROUND(SUM(ii.UnitPrice), 2) as TotalSales,
        CAST(strftime('%Y', i.InvoiceDate) as text) || "-" || CAST(strftime('%m', i.InvoiceDate) as text) as Month
        FROM invoice_items ii
        INNER JOIN invoices i ON i.InvoiceId = ii.InvoiceId
        INNER JOIN tracks t ON t.TrackId = ii.TrackId
        INNER JOIN albums al ON al.AlbumId = t.AlbumId
        GROUP BY Month
        HAVING al.ArtistId = ?;
    """
    df = pd.read_sql_query(sql_query, conn, params=(artist_id, ))
    return df


def save_sales(sales_df:pd.DataFrame):
    """
    Save the sales data to CSV file
    :param: sales_df: DataFrame with sales data
    """
    file_path = Path('sales.csv')
    sales_df.to_csv(file_path, index=False)


def save_sales_by_artist(sales_by_artist_df:pd.DataFrame):
    """
    Save the sales by artist data to CSV file
    :param: sales_by_artist_df: DataFrame with sales by artist data
    """
    file_path = Path('sales_by_artist.csv')
    sales_by_artist_df.to_csv(file_path, index=False)


def main():
    """
    Runs the data pipeline.
    """

    logging.basicConfig(filename='pipeline_log.txt', level=logging.INFO)
    
    database = "chinook.db"

    # create a database connection
    conn = create_connection(database)
    if not conn:
        logging.ERROR("error connecting to database.")
        quit()

    with conn:
        
        logging.info("Extracting sales data from database.")
        sales_df = get_sales(conn)

        logging.info("Saving the sales data as CSV.")
        save_sales(sales_df)

        logging.info("Extracting Sales by Artist '22'")
        sales_by_artist_df = get_sales_by_artist(22, conn)

        logging.info("Saving sales by artist")
        save_sales_by_artist(sales_by_artist_df)


if __name__ == "__main__":
    main()