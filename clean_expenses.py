import pandas as pd
from sqlalchemy import create_engine,text
#-- database connection --
engine = create_engine("postgresql://postgres:Aseem%40152123@localhost:5433/orbit")
# --  step1: Read raw Data from postgres or csv --
# option A: Read directly from DB
df_raw = pd.read_sql("SELECT * FROM expenses",engine)

#-- step2: Filter for Retail B&M --
segments = ["B&M Retail","B&M RETAIL"]
df = df_raw[df_raw["revised-segment"].isin(segments)].copy()

#-- step3: Select only required co  lumns --
keep_cols = [
   "posting-date","cost-element","cost-element-name","business-area", "val-in-rep-cur-"]

df = df[keep_cols]

#-- step4 = clean up data--

df["business-area"] = pd.to_numeric(df["business-area"],errors = "coerce").fillna(0)
df["val-in-rep-cur-"] = pd.to_numeric(df["val-in-rep-cur-"],errors = "coerce").fillna(0)
if pd.api.types.is_numeric_dtype(df["posting-date"]):
    df["posting-date"] = pd.to_datetime("1899-12-30") + pd.to_timedelta(df["posting-date"],unit="D")
else:
    df["posting-date"] = pd.to_datetime(df["posting-date"],errors="coerce", dayfirst=True)
#-- step5 = Write cleaned data to Postgres--
table_name = "expenses_cleaned"

#drop existing table befor rewriting (optional)

with engine.begin() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

df.to_sql(table_name,engine, index=False, if_exists="replace")

print(f"Cleaned data written to postgres table '{table_name}")
print(f"Rows: {len(df)} | columns: {len(df.columns)}")

#-- step 6 : export cleaned data to CSV----

df.to_csv("expenses_cleaned.csv",index =False,encoding ="utf-8")
print("cleaned data exported to expenses_cleaned.csv")