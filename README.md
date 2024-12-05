# School Data Transformation

This is a data process that retrieves school data from the Common Core of Data (CCD) Schools API and Transform the data from long format (multiple rows per school) to wide format (one row per school)

**Expected Wide Format**

| school_id | school_name        | students_2018 | students_2019 | students_2020 | teachers_2018 | teachers_2019 | teachers_2020 |
| --------- | ------------------ | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| 12345     | Lincoln Elementary | 450.00        | 475.00        | 460.00        | 25.00         | 26.00         | 25.00         |
| 67890     | Washington Middle  | 685.00        | 692.00        | 678.00        | 35.00         | 36.00         | 35.00         |

## Requirements

In order to run the pipeline, you'd need a few tools on your local machine.

- Python3.8 & PIP

## Getting Started

Install the required packages

```
pip install -r requirements.txt
```

Run the pipeline

```
python etl_runner.py
```

## How it works

We will follow a simple ETL pipeline process, starting with constant configuration, **extract data**, **transform data**, and **load data**.

#### Constant Configuration

We start by configuring the API endpoint and School Year

```
API = "https://educationdata.urban.org/api/v1/schools/ccd/directory/"
SCHOOL_YEARS = ["2018", "2019", "2020"]
```

#### Extract Data

Since the data source is an API, we will call /GET request to extract the data, the function arguments we define here will utilize values we declared from the constant.

```
def extract_data(api, year):
    response = requests.get(url=f"{api}{year}/?state=CA")

    if response.status_code != 200:
        return pd.DataFrame()

    results = response.json().get('results', [])
    return result
```

#### Transform Data

To reshape the format of our source data, we will use a package called `pandas`, this is included from the `requirements.txt`.

In this function we will transform the data into 7 steps, you can refer to the comments from the example code below.

```
import pandas as pd

...

# 1 Combine all DataFrames
df = pd.concat(data, ignore_index=True)

# 2 Select and rename columns to school_id, school_name, year, total_students, teachers
df = df[['school_id', 'school_name', 'year', 'enrollment', 'teachers_fte']]
df.columns = ['school_id', 'school_name', 'year', 'total_students', 'teachers']

# 3 Unpivot 'total_students' and 'teachers' column
df = pd.melt(df,
        id_vars=['school_id', 'school_name', 'year'],
        value_vars=['total_students', 'teachers'],
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
```

#### Load Data

To export the CSV, you just have to the method `to_csv` available from `pandas` package

```
df.to_csv(output_file, index=False)
```

#### ETL Pipeline

The `run()` function orchestrates the Extract, Transform, and Load (ETL) process and is executed when the script is run directly
