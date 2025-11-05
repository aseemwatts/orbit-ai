import pandas as pd
from sqlalchemy import create_engine, inspect, text
DB_URL = "postgresql+psycopg2://postgres:Aseem%40152123@localhost:5433/orbit"
engine = create_engine(DB_URL)

#---------------------------------
#step 1: Auto-detect & harmonize tables

#---------------------------------
#get all tables name starting with expenses 
inspector = inspect(engine)
all_tables = inspector.get_table_names()
expense_tables = [t for t in all_tables if t.startswith("expenses_") and t != "expenses_clean"]

dfs = []
segments = ["B&M Retail","B&M RETAIL"]
for tbl in expense_tables:
    seg_list = "','".join(segments)
    query = f"SELECT * FROM {tbl} WHERE \"revised-segment\" IN ('{seg_list}')"
    df = pd.read_sql(query, engine)
    df["source_table"] = tbl

    dfs.append(df)

# unify columns across all the tables

all_columns = sorted({c for df in dfs for c in df.columns})
for i, df in enumerate(dfs):
    # add missing columns wiht NaN so structure matches
    missing = [c for c in all_columns if c not in df.columns]
    for c in missing:
        df[c] =  pd.NA
    # reoder columsn for consistency
        df[i] =  df[all_columns]
    # combine 
expenses_all =  pd.concat(dfs, ignore_index = True)
expenses_all.to_sql("expenses_clean",engine, index=False, if_exists="replace")

#export to csv
expenses_all.to_csv("expenses_cleaned.csv",index =False,encoding ="utf-8")
print("cleaned data exported to expenses_cleaned.csv")

print(f" Consolidated {len(expenses_all)} rows from {len(expense_tables)} tables ({len(all_columns)} columns)")