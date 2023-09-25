# Import necessary modules and functions for extraction and transformation
from src.extraction.extraction import extract_tables
# from src.transformation import transform_table1, transform_table2
# from src.loading import load_data

def main():
    """Function to orchestration""" 
    # Extract data from Tables
    tables_data = extract_tables()
    print(tables_data)

if __name__ == "__main__":
    main()
