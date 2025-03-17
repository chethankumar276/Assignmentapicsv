from fastapi import FastAPI, HTTPException
import pandas as pd

app = FastAPI(title="CSV Data API", description="API to fetch and filter CSV data", version="1.0")

# Load CSV file
CSV_FILE = "data.csv"

def load_csv():
    try:
        return pd.read_csv(CSV_FILE)
    except FileNotFoundError:
        return pd.DataFrame()  # Return empty DataFrame if file not found

df = load_csv()

@app.get("/")
def home():
    return {"message": "Welcome to the CSV Data API"}

@app.get("/data")
def get_data():
    """Fetch all data from the CSV."""
    if df.empty:
        raise HTTPException(status_code=404, detail="CSV file is empty or missing")
    return df.to_dict(orient="records")

@app.get("/data/columns")
def get_columns():
    """Fetch available column names."""
    if df.empty:
        raise HTTPException(status_code=404, detail="CSV file is empty or missing")
    return {"columns": df.columns.tolist()}

@app.get("/data/{column}/{value}")
def filter_data(column: str, value: str):
    """Fetch filtered data based on column and value."""
    if df.empty:
        raise HTTPException(status_code=404, detail="CSV file is empty or missing")
    if column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Invalid column name: {column}")
    
    filtered = df[df[column].astype(str) == value]
    if filtered.empty:
        raise HTTPException(status_code=404, detail=f"No matching records found for {value} in {column}")
    
    return filtered.to_dict(orient="records")

@app.get("/data/row/{index}")
def get_row(index: int):
    """Fetch a specific row by index."""
    if df.empty:
        raise HTTPException(status_code=404, detail="CSV file is empty or missing")
    if index < 0 or index >= len(df):
        raise HTTPException(status_code=400, detail="Invalid row index")
    
    return df.iloc[index].to_dict()

@app.get("/data/summary")
def get_summary():
    """Fetch basic statistics of numeric columns."""
    if df.empty:
        raise HTTPException(status_code=404, detail="CSV file is empty or missing")
    
    return df.describe().to_dict()