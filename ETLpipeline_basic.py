import pandas as pd

def extract(file_path):
    """Extract data from CSV file."""
    return pd.read_csv(file_path)

def transform(df):
    """Transform data: clean and modify as needed."""
    # Example: Drop rows with missing values and convert column names to lowercase
    df = df.dropna()
    df.columns = [col.lower() for col in df.columns]
    return df

def load(df, output_path):
    """Load data to a new CSV file."""
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    # Example usage
    input_file = "input_data.csv"
    output_file = "output_data.csv"

    data = extract(input_file)
    transformed_data = transform(data)
    load(transformed_data, output_file)
