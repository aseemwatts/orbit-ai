import pandas as pd
from sqlalchemy import create_engine, text
#-- DB Connect ----
engine = create_engine("postgresql+psycopg2://postgres:Aseem%40152123@localhost:5433/orbit")
mapping = pd.read_csv("/Users/aseemwatts/orbit_mvp/store_mapping.csv")
print("in progress creating table")
#---step1 load sales data----
sfs = pd.read_sql("SELECT * FROM sales_consol LIMIT 300000;",engine)

#---step2 clean dates---

if pd.api.types.is_numeric_dtype(sfs["invoice-date"]):
    sfs["bill_date"] = pd.to_datetime("1899-12-30") + pd.to_timedelta(sfs["invoice-date"], unit="D")

else:
    sfs["bill_date"] = pd.to_datetime(sfs["invoice-date"],errors = "coerce")


sfs["month"] = sfs["bill_date"].dt.to_period("M").astype(str)

print("in progress creating tableq")
summary = (
    sfs.groupby(["month","actor-config-mapping"])
    .agg(
        num_orders= ("inv-count","nunique"),
        total_mrp = ("gmv-mrp-rs-","sum"),
        total_discounts = ("discount-amount-rs-","sum"),
        total_nmv = ("invoice-amount-without-tax-rs-","sum"),
        total_cogs = ("cogs-wo-tax","sum"),
        total_gross_margins = ("gross-margin-rs-","sum")
    )
    .reset_index()
)


# ##----expenses_tables_starts_here----

exps = pd.read_sql("SELECT * FROM expenses_summary;",engine)

mapping["store"] = mapping["store"].astype(str)
exps["store"] = (
    pd.to_numeric(exps["store"], errors='coerce')
    .astype(pd.Int64Dtype())
    .astype(str)
)

exps = exps.merge(mapping,left_on = "store",right_on ="store",how = "left")


print(f"in progress creating table with {len(exps)}")

exp_summary = (
    exps.groupby(["month","store_id"])
    .agg(
        total_expenses=("val-in-rep-cur-", "sum")
    )
    .reset_index()
)

ebitda = summary.merge(exp_summary,left_on = ["month","actor-config-mapping"],right_on =["month","store_id"],how = "left")

table_name = "ebitda"

with engine.begin() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

ebitda.to_sql(table_name,engine, index = False)


print(f"sales summary P&L created successfully: {len(ebitda)} rows.")

