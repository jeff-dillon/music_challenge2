import pandas as pd
import plotly.express as px
from pathlib import Path

def load_sales_data() -> pd.DataFrame:
    file_path = Path('sales.csv')
    df = pd.read_csv(file_path)
    return df

def plot_sales(df:pd.DataFrame):
    fig = px.line(df, x='Month', y="Quantity", title='Sales by Month')
    fig.show()

def load_sales_by_artist_data() ->  pd.DataFrame:
    file_path = Path('sales_by_artist.csv')
    df = pd.read_csv(file_path)
    return df

def plot_sales_by_artist(df:pd.DataFrame):
    fig = px.line(df, x='Month', y="Quantity", title='Artist 22 Sales by Month')
    fig.show()

def main():
    sales_df = load_sales_data()
    plot_sales(sales_df)

    sales_by_artist_df = load_sales_by_artist_data()
    plot_sales_by_artist(sales_by_artist_df)

if __name__ == "__main__":
    main()