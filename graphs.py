import pandas as pd
import plotly.express as px
from pathlib import Path

def load_sales_data() -> pd.DataFrame:
    """
    Load the sales data from CSV file.
    """
    file_path = Path('sales.csv')
    df = pd.read_csv(file_path)
    return df

def plot_sales(df:pd.DataFrame):
    """
    Graph the sales data.
    :param: df: DataFrame with sales data grouped by Month
    """
    fig = px.bar(df, x='Month', y="TotalSales", title='Sales by Month')
    fig.show()

def main():
    """
    Loads data from CSV files and displays graphs.
    """
    sales_df = load_sales_data()
    plot_sales(sales_df)

if __name__ == "__main__":
    main()