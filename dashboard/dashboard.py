import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
import datetime
import os 

# 🛠️ LocalStack S3 Configuration
S3_BUCKET = "curated"
LOCALSTACK_ENDPOINT_URL = "localstack:4566"  

# Set environment variables for LocalStack authentication
os.environ["AWS_ACCESS_KEY_ID"] = "root"
os.environ["AWS_SECRET_ACCESS_KEY"] = "root"
os.environ["AWS_REGION"] = "us-east-1"

@st.cache_data
def load_data_from_s3(ticker, start_date, end_date):
    file_path = f"s3://{S3_BUCKET}/{ticker}.parquet"

    # Establish a DuckDB in-memory connection
    con = duckdb.connect(database=':memory:')

    # Configure DuckDB for S3 access
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET s3_endpoint='localstack:4566';")
    con.execute("SET s3_access_key_id='root';")
    con.execute("SET s3_secret_access_key='root';")
    con.execute("SET s3_url_style='path';") 
    con.execute("SET s3_use_ssl=false;") 

    # SQL query to fetch data from CSV in S3
    print(file_path)
    query = f"""
        SELECT Date, Close, Volume, SMA_10, SMA_50, RSI_14, Upper_Band, Lower_Band
        FROM read_parquet('{file_path}')
        WHERE Date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY Date
    """

    try:
        df = con.execute(query).fetchdf()
        print("✅ Data Loaded Successfully!")
    except Exception as e:
        print("❌ DuckDB Error:", e)
        df = None
    finally:
        con.close()  # Ensure the connection is always closed

    # Check if data is empty
    if df is None or df.empty:
        return None

    df["Date"] = pd.to_datetime(df["Date"])
    return df


# 🎨 Streamlit Page Config
st.set_page_config(page_title="📈 Stock Dashboard", layout="wide")

# 🎯 Sidebar: Stock Selection & Date Range
st.sidebar.title("📊 Sélectionnez une Action")
tickers = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "DIS", "GE", "NVDA", "XOM"]
selected_tickers = st.sidebar.multiselect("📌 Sélectionnez un ou plusieurs tickers", tickers, default=["AAPL"])

st.sidebar.title("📆 Sélectionnez une Période")
start_date = st.sidebar.date_input("📅 Date de début", datetime.date(2023, 1, 1))
end_date = st.sidebar.date_input("📅 Date de fin", datetime.date(2024, 1, 1))

# 🎯 Load Data for Selected Stocks
dataframes = {ticker: load_data_from_s3(ticker, start_date, end_date) for ticker in selected_tickers}

# 📌 **Stock Price Comparison**
st.subheader("📈 Comparaison des Prix de Clôture")
fig = px.line()

for ticker, df in dataframes.items():
    if df is not None:
        fig.add_scatter(x=df['Date'], y=df['Close'], mode='lines', name=ticker)

st.plotly_chart(fig, use_container_width=True, key="close_prices_chart")

# 📌 **Stock Performance Metrics**
st.subheader("📊 Statistiques de Performance")
performance_data = []

for ticker, df in dataframes.items():
    if df is not None:
        perf = {
            "Ticker": ticker,
            "Dernier Prix": df['Close'].iloc[-1],
            "Max Prix": df['Close'].max(),
            "Min Prix": df['Close'].min(),
            "Performance (%)": ((df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0]) * 100
        }
        performance_data.append(perf)

if performance_data:
    df_perf = pd.DataFrame(performance_data)
    st.dataframe(df_perf, use_container_width=True)

# 📌 **Moving Averages Visualization**
st.subheader("📊 Moyennes Mobiles")
fig_sma = px.line()

for ticker, df in dataframes.items():
    if df is not None:
        fig_sma.add_scatter(x=df['Date'], y=df['Close'], mode='lines', name=f"{ticker} - Close")
        fig_sma.add_scatter(x=df['Date'], y=df['SMA_10'], mode='lines', name=f"{ticker} - SMA 10")
        fig_sma.add_scatter(x=df['Date'], y=df['SMA_50'], mode='lines', name=f"{ticker} - SMA 50")

st.plotly_chart(fig_sma, use_container_width=True, key="sma_chart")

# 📌 **Trading Volume Chart**
st.subheader("📊 Volume des Transactions")
fig_volume = px.bar()

for ticker, df in dataframes.items():
    if df is not None:
        fig_volume.add_bar(x=df['Date'], y=df['Volume'], name=ticker)

st.plotly_chart(fig_volume, use_container_width=True, key="volume_chart")

# 📌 **Heatmap of Daily Price Changes**
st.subheader("🔥 Heatmap des Variations Journalières")

for ticker, df in dataframes.items():
    if df is not None:
        df['Daily Change'] = df['Close'].pct_change()
        df['Month'] = df['Date'].dt.strftime('%Y-%m')
        df['Day'] = df['Date'].dt.day

        heatmap_data = df.pivot(index="Day", columns="Month", values="Daily Change")

        fig = px.imshow(
            heatmap_data,
            color_continuous_scale="RdYlGn",
            title=f"📊 Daily Stock Price Change Heatmap for {ticker}",
            labels={"color": "Daily % Change"},
        )

        fig.update_layout(
            autosize=True,
            xaxis_title="Month",
            yaxis_title="Day",
            height=500,
            width=900,
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=False)
        )

        st.plotly_chart(fig, use_container_width=True, key=f"heatmap_{ticker}")

# 📌 **Technical Indicators: RSI & Bollinger Bands**
st.subheader("📌 Indicateurs Techniques")
for ticker, df in dataframes.items():
    if df is not None:
        fig_indicators = px.line(df, x="Date", y=["Close", "SMA_10", "Upper_Band", "Lower_Band"], title=f"{ticker} - Bollinger Bands & SMA")
        st.plotly_chart(fig_indicators, use_container_width=True, key=f"indicators_{ticker}")

# 📌 **Download Data Option**
st.subheader("📥 Télécharger les Données")

for ticker, df in dataframes.items():
    if df is not None:
        st.download_button(
            label=f"📥 Télécharger {ticker} en CSV",
            data=df.to_csv(index=False),
            file_name=f"{ticker}_data.csv",
            mime="text/csv"
        )