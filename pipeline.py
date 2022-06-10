"""
Simple Data Pipeline
"""
import sys
import time
from pathlib import Path
import logging
import sqlite3
import yaml
import pandas as pd



def create_connection(cfg: dict) -> sqlite3.Connection:
    """
    Utility function - creates connection to SQLite database
    :return: Connection object or None
    """
    conn = None
    database_file_path = Path(cfg['db']['file_path'])
    if not database_file_path.exists():
        logging.error('database file not found')
        sys.exit()
    else:
        try:
            conn = sqlite3.connect(database_file_path)
        except sqlite3.Error as error:
            logging.error(error)
            sys.exit()

    return conn


def configure_logging(cfg: dict):
    """
    Utility function - configures the data pipeline logging
    """
    # configure the logging
    log_file_path = Path(cfg['logging']['file_path'])
    logging.basicConfig(filename=log_file_path,
                        format='%(asctime)s -- [%(levelname)s]: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def get_config() -> dict:
    """
    Utility function returns Dict with config parameters
    :return: Dict with config parameters
    """
    with open("config.yaml", "r", encoding='utf-8') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    return cfg


def get_sales_by_month_sql(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Get total sales by month using SQL to aggregate the data
    :param: conn: connection to database
    :return: DataFrame with sales by month (Month, Quantity, TotalSales)
    """
    sql_query = """
        SELECT ROUND(SUM(ii.Quantity), 2) as Quantity,
        ROUND(SUM(ii.UnitPrice * ii.Quantity), 2) as TotalSales,
        CAST(strftime('%Y', i.InvoiceDate) as text) || "-" || 
            CAST(strftime('%m', i.InvoiceDate) as text) as Month
        FROM invoice_items ii
        INNER JOIN invoices i ON i.InvoiceId = ii.InvoiceId
		GROUP BY Month;
    """
    monthly_sales_df = pd.read_sql_query(sql_query, conn)
    return monthly_sales_df


def get_sales_by_month_pd(conn: sqlite3.Connection) -> pd.DataFrame:
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
    monthly_sales_df['Month'] = pd.to_datetime(
        monthly_sales_df['InvoiceDate']).dt.to_period('M')

    # drop the raw date column
    monthly_sales_df.drop('InvoiceDate', axis=1)

    monthly_sales_df['TotalPrice'] = monthly_sales_df['UnitPrice'] * \
        monthly_sales_df['Quantity']

    # group by month and sum the Price and Quantity
    monthly_sales_df = monthly_sales_df.groupby('Month').agg(
        TotalSales=('TotalPrice', sum),
        Quantity=('Quantity', sum),
        Month=('Month', 'first'))

    return monthly_sales_df


def get_top_artists_by_sales(num_results: int, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Get the top N artists by total sales
    :param: num_results: the number of results to return
    :param: conn: connection to database
    :return: DataFrame with sales by Artist (Artist, Quantity, TotalSales)
    """
    sql_query = """
        SELECT ROUND(SUM(ii.Quantity), 2) as Quantity,
        ROUND(SUM(ii.UnitPrice * ii.Quantity), 2) as TotalSales,
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
    artist_sales_df = pd.read_sql_query(
        sql_query, conn, params=(num_results, ))
    return artist_sales_df


def get_tracks_by_genre(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    get the number of Tracks by Genre
    :param: conn: connection to database
    :return: DataFrame with NumberOfTracks, Genre
    """
    sql_query = """
        SELECT COUNT(t.TrackId) AS NumberOfTracks, g.Name AS GenreName
		FROM tracks t
		INNER JOIN genres g ON t.GenreID = g.GenreId
		GROUP BY g.Name;
        """

    tracks_by_genre_df = pd.read_sql_query(sql_query, conn)
    return tracks_by_genre_df


def get_annual_sales_by_month(year: str, conn: sqlite3.Connection) -> pd.DataFrame:
    """ 
    get the annual sales by month for a specified year
    :param: conn: connection to database
    :param: conn: year: the specified year
    :return: DataFrame with Month, Quantity, Total Sales """

    sql_query = """
        SELECT 
        strftime('%m', i.InvoiceDate) as Month,
        sum(ii.Quantity) as Quantity,
        ROUND(SUM(ii.UnitPrice * ii.Quantity), 2) as MonthlySales
        FROM invoices i
        INNER JOIN invoice_items ii ON i.InvoiceId =  ii.InvoiceId
        WHERE strftime('%Y', i.InvoiceDate) = ?
        GROUP BY Month
        ;
        """

    annual_sales_by_month_df = pd.read_sql_query(
        sql_query, conn, params=(year, ))
    return annual_sales_by_month_df


def get_sales_by_quarter(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    get the total sales by quarter for all years
    :param: conn: connection to database
    :return: DataFrame with Quarter, Quantity, Total Sales` """

    sql_query = """
    SELECT 
    strftime('%Y', i.InvoiceDate) ||
    CASE 
        when strftime('%m', i.InvoiceDate) = '01' OR strftime('%m', i.InvoiceDate) = '02' OR strftime('%m', i.InvoiceDate) = '03'
        then 'Q1'
        when strftime('%m', i.InvoiceDate) = '04' OR strftime('%m', i.InvoiceDate) = '05' OR strftime('%m', i.InvoiceDate) ='06'
        then 'Q2'
        when strftime('%m', i.InvoiceDate) = '07' OR strftime('%m', i.InvoiceDate) = '08' OR strftime('%m', i.InvoiceDate) = '09'
        then 'Q3'
        else 'Q4'
    END AS Quarter,
    sum(ii.Quantity) as Quantity,
    ROUND(SUM(ii.UnitPrice * ii.Quantity), 2) as TotalSales
    FROM invoices i
    INNER JOIN invoice_items ii ON i.InvoiceId =  ii.InvoiceId
    GROUP BY Quarter
        ;"""
    sales_by_quarter_df = pd.read_sql_query(sql_query, conn)
    return sales_by_quarter_df


def get_top_artists_by_sales_pd(num_results: int, conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Get the top N artists by total sales
    :param: num_results: the number of results to return
    :param: conn: connection to database
    :return: DataFrame with sales by Artist (Artist, Quantity, TotalSales)
    """
    sql_query = """
        SELECT ii.Quantity as Quantity,
        ii.UnitPrice,
        i.InvoiceDate as Date, 
        ar.Name as ArtistName
        FROM invoice_items ii
        INNER JOIN invoices i ON i.InvoiceId = ii.InvoiceId
        INNER JOIN tracks t ON t.TrackId = ii.TrackId
        INNER JOIN albums al ON al.AlbumId = t.AlbumId
        INNER JOIN artists ar ON ar.ArtistId = al.ArtistId
        """
    total_sales_by_artist_df = pd.read_sql_query(sql_query, conn)

    # Extract the Month from the Invoice Date and drop the Date Column
    total_sales_by_artist_df['Month'] = pd.to_datetime(
        total_sales_by_artist_df['Date']).dt.month

    # total_sales_by_artist_df.drop('Date', axis=1, inplace=True)

    # Calculate the total for each track sold
    total_sales_by_artist_df['TotalSales'] = total_sales_by_artist_df['Quantity'] * \
        total_sales_by_artist_df['UnitPrice']

    # Group by artist
    # Aggregate by sum of totals for each sale
    # Reset the index so Artist is diplayed as a column in the results
    # Sort in descending order
    # Keep the top n artists where n is the num_results param
    total_sales_by_artist_df = total_sales_by_artist_df.groupby(['ArtistName']).agg(
        {'Month': 'first', 'Quantity': 'sum', 'TotalSales': 'sum'}).reset_index().sort_values('TotalSales', ascending=False).head(num_results)

    return total_sales_by_artist_df


def main():
    """
    Data Pipeline Process Control
    """
    start_time = time.time()
    cfg = get_config()
    configure_logging(cfg)
    logging.info("Starting data pipeline process")
    conn = create_connection(cfg)
    year = '2010'
    num_results = 10

    # run the data pipeline steps
    with conn:

        logging.info("Extracting sales data from database")

        # there are two example approaches for this function
        # Approach 1: format the month and calculate the total sales in SQL
        # monthly_sales_df = get_sales_by_month_sql(conn)
        # Approach 2: format the month and calculate the total sales in Pandas
        monthly_sales_df = get_sales_by_month_pd(conn)

        logging.info("Saving the sales data as CSV")
        monthly_sales_df.to_csv(
            Path(cfg['extract_files']['sales_by_month_file_path']), index=False)

        logging.info(f"Extracting top {num_results} Artists by TotalSales")
        sales_by_artist_df = get_top_artists_by_sales(num_results, conn)

        logging.info("Saving sales by artist data as CSV")
        sales_by_artist_df.to_csv(
            Path(cfg['extract_files']['sales_by_artist_file_path']), index=False)

        logging.info("Extracting number of tracks by genre from database")
        tracks_by_genre_df = get_tracks_by_genre(conn)

        logging.info("Saving tracks by genre as CSV")
        tracks_by_genre_df.to_csv(
            Path(cfg['extract_files']['tracks_by_genre_file_path']), index=False)

        logging.info(
            f"Extracting monthly sales by year for {year} from database")
        annual_sales_by_month_df = get_annual_sales_by_month(year, conn)

        logging.info(f"Saving monthly sales by year for {year}as CSV")
        annual_sales_by_month_df.to_csv(str(Path(
            cfg['extract_files']['annual_sales_by_month_file_path'])) + year + '.csv', index=False)

        logging.info("Extracting total sales by quarter from database")
        sales_by_quarter_df = get_sales_by_quarter(conn)

        logging.info("Saving total sales by quarter as CSV")
        sales_by_quarter_df.to_csv(
            Path(cfg['extract_files']['sales_by_quarter_file_path']), index=False)

        logging.info(f"Extracting top {num_results} artists as CSV")
        total_sales_by_artist_df = get_top_artists_by_sales_pd(
            num_results, conn)

        logging.info("Saving top artists as CSV")
        total_sales_by_artist_df.to_csv(
            Path(cfg['extract_files']['top_artist_sales_file_path']), index=False)

    conn.close()

    logging.info("Pipeline completed in %2.2f seconds" %
                 (time.time() - start_time))
    logging.info("Finishing data pipeline process")


if __name__ == "__main__":
    main()
