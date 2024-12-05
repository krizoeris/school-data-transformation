from helpers import extract_data, transform_data, load_data

# Here we configure the API and school year and is only required in California state
# Since this is a public API, I can just declare it in a constant
API = "https://educationdata.urban.org/api/v1/schools/ccd/directory/"
SCHOOL_YEARS = ["2018", "2019", "2020"] 

# This is an ETL Pipeline function
def run():
    # Extract data from the source
    extracted_data = [extract_data(API, year) for year in SCHOOL_YEARS]

    # Transform data
    transformed_data = transform_data(extracted_data)

    # Load data to CSV
    load_data(transformed_data)

# Execute the ETL pipeline
if __name__ == "__main__":
    run()