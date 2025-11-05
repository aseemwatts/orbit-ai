import pandas as pd
from sqlalchemy import create_engine, Date
from pathlib import Path
class BaseIngestor:
    def __init__(self,file_path,db_url,table):
        self.file_path = file_path
        self.db_url = db_url
        self.table = table
        self.db = None
        self.engine = create_engine(db_url)

    def load_excel(self,sheet_name = None):
        ext = Path(self.file_path).suffix.lower()
        engine_type = "pyxlsb" if ext == ".xlsb" else "openpyxl"
    

        df =pd.read_excel(self.file_path,sheet_name=sheet_name,engine=engine_type)
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(r"[^\w]+","-",regex=True)
        )
        self.df =df
        print(f"Loaded {len(df)} rows from {self.file_path.name}")
        return self 
    
    # def save(self, if_exists='append'):
    #     self.df.to_sql(self.table,self.engine,if_exists=if_exists,index = False)
    #     dtype_map = {
    #         "posting-date" : Date() # for timestamp
    #     }
    #     print(f"Saved {len(self.df)} rows into{self.table}")
    #     return self
    
class ExpenseIngestor(BaseIngestor):
    def __init__(self,file_path, db_url ="postgresql+psycopg2://user:pass@localhost:5433/orbit", table = None):
        # if table not provided, create it from file name
        table_name = table or Path(file_path).stem.lower().replace(" ","_")
        super().__init__(file_path,db_url,table_name)

    def clean(self):
        #standardize date
        #self.df["posting-date"] = pd.to_datetime(self.df.get("posting-date"),errors = "coerce",dayfirst = True)

        # # clean amounts
        # self.df["amounts"] =(
        #     self.df["amounts"].astype(str)
        #     .str.replace(",","", regex=False)
        #     .str.replace(r"\((.*)\)",r"-\1",regex=True)
        #     .astype(float)
        # )

        # self.df["description"] = self.df.get("description","").astype(str)

        return self

    def categorize(self):
        # def map_cat(desc:str) ->str:
        #     d = desc.lower()
        #     if "rent" in d: return "rental"
        #     if any(x in d for x in ["salary","wage","payroll"]): return "manpower"
        #     return "general"
        
        # self.df["category"] =self.df["description"].map(map_cat)
        return self
    
# ---usage---   
if __name__ == "__main__":
    FOLDER = Path("/Users/aseemwatts/orbit_mvp/ingest/")
    FOLDER2 = Path("/Users/aseemwatts/orbit_mvp/")
    DB_URL = "postgresql+psycopg2://postgres:Aseem%40152123@localhost:5433/orbit"
    engine = create_engine(DB_URL)
    excel_files = list(FOLDER.glob("*.xlsb")) + list(FOLDER.glob("*.xlsx"))
    print(f"Folder found {len(excel_files)} Excel Files")

    dfs = []
    segments = ["B&M Retail","B&M RETAIL"]

    for file in excel_files:
        print(f" Processing {file.name}")
        ingestor = ExpenseIngestor(file,DB_URL)
        ingestor.load_excel(sheet_name=2).clean().categorize()
        df = ingestor.df

        #apply filter if revised-segment exists
        if "revised-segment" in df.columns:
            df = df[df["revised-segment"].isin(segments)].copy()
            print(f" --> filtered {len(df)} rows for segment criteria")
        else:
            print(f" skipped filtering - no revised segment")
        
        df["source_file"] = file.name
        dfs.append(df)

    if dfs:
        all_columns = sorted({c for df in dfs for c in df.columns})
        for i, df in enumerate(dfs):
            missing = [c for c in all_columns if c not in df.columns]
            for c in missing:
                df[c] = pd.NA
            dfs[i] = df[all_columns]

        expenses_all = pd.concat(dfs, ignore_index= True)

        #create a table 
        expenses_all.to_sql("expenses_clean",engine, index=False, if_exists="replace")

        #export to csv
        out_path = FOLDER2/"expenses_cleaned.csv"
        expenses_all.to_csv(out_path, index=False, encoding="utf-8")

        print(f"consolidated {len(expenses_all)} rows from {len(excel_files)} files")
        print(f" Exported to {out_path}")
        print(f"Table 'expenses_clean' created in the database")
        
        # --- sync updated local mapping CSV to DB---
        map_out_path = FOLDER2/"expenses_mapping.csv"
        if map_out_path.exists():
            try:
                csv_map = pd.read_csv(map_out_path, encoding = "utf-8", on_bad_lines="skip")
                if "expense-category" in csv_map.columns:
                    csv_map.to_sql("expense_mapping", engine, index=False, if_exists="replace")
                    print(f"synced updated local mapping csv ({len(csv_map)} rows) to DB")

            except UnicodeDecodeError:
                print("UTF-8 decode failed, tyring fallback encoding (latin1)")
                csv_map = pd.read_csv(map_out_path,encoding="latin1")
                if "expense-category" in csv_map.columns:
                    csv_map.to_sql("expense_mapping", engine, index=False, if_exists="replace")
                    print(f"synced update local mapping csv ({len(csv_map)} rows) to DB (via fallback)")


            except Exception as e:
                print(f" could not sync mapping csv : {e}")

        print(f" about to give mapping table for your reference")

        # --- Derive business-type (store vs business central)

        if "cost-element" in expenses_all.columns and "business-area" in expenses_all.columns:
            #derive business type once per row
            expenses_all["business-type"] = expenses_all["business-area"].astype(str).apply(
                lambda x: "store" if x.strip().startswith("3") else "Businsess Central")
        else:
            expenses_all["business-type"] = "Unknown"


            # Create mapping on cost-element and business-type
        if "cost-element" in expenses_all.columns and "business-type" in expenses_all.columns:
            new_mapping = (
                expenses_all[["business-type","cost-element"]]
                .dropna()
                .drop_duplicates()
                .sort_values(["business-type","cost-element"])
                .reset_index(drop=True)
            )

            new_mapping["expense-category"] = pd.NA #placeholder for manual tagging
                
            #save mapping to DB and CSV
            try:
                existing_mapping = pd.read_sql("SELECT * FROM expense_mapping", engine)
                print(f"Found existing mapping with {len(existing_mapping)} rows")

                #merge to find new combinations
                merged_mapping = (
                    pd.concat([existing_mapping,new_mapping])
                    .drop_duplicates(subset=["business-type","cost-element"],keep='first')
                    .reset_index(drop=True)
                )

                print(f"added {len(merged_mapping)- len(existing_mapping)} new cost elements to mapping")

            except Exception as e:
                print(f"No existing mapping found, creating a new one: {e}")
                merged_mapping = new_mapping.copy()


           
            merged_mapping.to_sql("expense_mapping",engine, index=False, if_exists = "replace")
            map_out_path = FOLDER2/"expenses_mapping.csv"
            merged_mapping.to_csv(map_out_path,index=False,encoding="utf-8")

            print(f"Mapping table created with {len(merged_mapping)} unique combinations")
            print(f"Exported to {map_out_path}")
            print(f"Table expneses mapping created")

        else:
            print("mapping table skipped")
        # --- Merge expense categories from mapping table ---

        try:
            mapping_df =  pd.read_sql("SELECT * FROM expense_mapping", engine)
            if {"cost-element","business-type","expense-category"}.issubset(mapping_df.columns):

                expenses_all = expenses_all.merge(
                    mapping_df[["cost-element","business-type","expense-category"]],
                    on=["cost-element","business-type"],
                    how = "left"
                )
                print(f"Applied {len(mapping_df)} mappings to expenses_all")
                print(f" 'expense-category'  column added successfully")
            else:
                print("Mapping table missing expected columns.")

        except Exception as e:
            print(f" could not merge mapping table : {e}")
    


        # >>> NEW SECTION: CREATE SUMMARY (month × store × expense-category × business-type)
        if "posting-date" in expenses_all.columns:
            expenses_all["posting-date"] = pd.to_datetime(expenses_all["posting-date"],origin= "1899-12-30", unit = "d", errors="coerce")
            expenses_all["month"] = expenses_all["posting-date"].dt.to_period("M").astype(str)
        else:
            expenses_all["month"] = pd.NA

        #Derive store column
        expenses_all["store"] = expenses_all.apply(
            lambda x: x["business-area"] if str(x.get("business-type","")).lower() == "store" else "central",
            axis=1
        )

        #---ensure amounts column is numeric
        if "val-in-rep-cur-" in expenses_all.columns:
            
            expenses_all["val-in-rep-cur-"] = pd.to_numeric(expenses_all["val-in-rep-cur-"], errors = "coerce")
        print(expenses_all.columns)

        #---create summary----
        if {"month", "store", "expense-category", "val-in-rep-cur-"}.issubset(expenses_all.columns):
            print("\n creating summarized expenses sheet")
            summary = (
                expenses_all
                .groupby(["month","store","expense-category"], dropna=False)["val-in-rep-cur-"]
                .sum()
                .reset_index()
                .sort_values(["month","store","expense-category"])
            )

            summary["val-in-rep-cur-"] = summary["val-in-rep-cur-"].round(2)

            summary.to_sql("expenses_summary",engine, index = False, if_exists = "replace")
            summary_out_path = FOLDER2/"expenses_summary.csv"
            summary.to_csv(summary_out_path, index = False, encoding="utf-8")

            print(f"Expenses summary created with {len(summary)} rows")
            print(f" Exported to {summary_out_path}")
            print(f" Table 'expenses_summary' created in the database")
        else:
            print("skpping summary - required columns not found")

    else:
        print("No data frames created")