import requests
import pandas as pd

# This function extracts the data from 'educationdata.urban.org'
# Returns a DataFrame
def extract_data(api, year):
    results = []  
    next_url = f"{api}{year}/?state=CA"
    page = 1

    while next_url:
        response = requests.get(url=next_url)
        print(f"Fetching data for school year {year} page {page}...")
        
        if response.status_code != 200:
            print(f"Failed to fetch data for {year}: {response.status_code}")
            break
        
        data = response.json()
        results.extend(data.get('results', []))

        if data.get('next', None):
            next_url = data.get('next') 
            page += 1
        else:
            next_url = None

    return pd.DataFrame(results)

# This function will transform and reshape data from long to wide format
def transform_data(data):
    print(f"Transforming the data...")

    # 1 Combine all DataFrames
    df = pd.concat(data, ignore_index=True)

    # 2 Select and rename columns to school_id, school_name, year, students, teachers
    df = df[['school_id', 'school_name', 'year', 'enrollment', 'teachers_fte']]
    df.columns = ['school_id', 'school_name', 'year', 'students', 'teachers']

    # 3 Unpivot 'students' and 'teachers' column
    df = pd.melt(df,
            id_vars=['school_id', 'school_name', 'year'], 
            value_vars=['students', 'teachers'], 
            var_name='metric', 
            value_name='value'
        )
    
    # 4 Reorganize columns
    df = pd.pivot_table(df,
            index=['school_id', 'school_name'], 
            columns=['metric', 'year'], 
            values='value'
        )

    # 5 Flatten Multindex columns
    df.columns = [f"{metric}_{year}" for metric, year in df.columns]

    # 6 Handle NaN values and remove decimals
    df_num_col = df.select_dtypes(include=["float", 'int']).columns
    df[df_num_col] = df[df_num_col].fillna(0).astype(int)

    # 7 Use the default index
    df.reset_index(inplace=True)

    # 8 Reorder columns sort by 'students' first, then 'teachers'
    df_student_col = [col for col in df.columns if 'students' in col]
    df_teacher_col = [col for col in df.columns if 'teachers' in col]
    
    df_col_order = ['school_id', 'school_name'] + df_student_col + df_teacher_col
    df = df[df_col_order]

    return df

# This function will export the data into CSV File
def load_data(df, output_file="school_data_wide_format.csv"):
    df.to_csv(output_file, index=False)
    print(f"Data successfully transformed and saved to ./{output_file}")