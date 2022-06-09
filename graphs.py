"""
Example graphs to test pipeline CSV output
"""
from pathlib import Path
import pandas as pd
import plotly.express as px

def load_sales_data() -> pd.DataFrame:
    file_path = Path('data/sales_by_month.csv')
    df = pd.read_csv(file_path)
    return df

def plot_sales(df:pd.DataFrame) -> None:
    fig = px.line(df, x='Month', y="TotalSales", title='Sales by Month')
    fig.show()
    return None

def load_sales_by_artist_data() -> pd.DataFrame:
    file_path = Path('data/sales_by_artist.csv')
    df = pd.read_csv(file_path)
    return df

def plot_sales_by_artist(df:pd.DataFrame):
    fig = px.bar(df, x='ArtistName', y="TotalSales", title='Sales by Artist')
    fig.show()
    return None

def load_sales_by_quarter_data() -> pd.DataFrame:
    file_path = Path('data/sales_by_quarter.csv')
    df = pd.read_csv(file_path)
    return df

def plot_sales_by_quarter(df:pd.DataFrame) -> None:
    fig = px.line(df, x='Quarter', y="TotalSales", title='Sales by Quarter')
    fig.show()
    return

def main():
    # sales_df = load_sales_data()
    # plot_sales(sales_df)

    # sales_by_artist_df = load_sales_by_artist_data()
    # plot_sales_by_artist(sales_by_artist_df)

    sales_by_quarter_df = load_sales_by_quarter_data()
    plot_sales_by_quarter(sales_by_quarter_df)

if __name__ == "__main__":
    main()