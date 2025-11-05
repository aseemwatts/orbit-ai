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
    FOLDER = Path("/Users/aseemwatts/orbit_mvp/sales_dumps/")
    FOLDER2 = Path("/Users/aseemwatts/orbit_mvp/")
    DB_URL = "postgresql+psycopg2://postgres:Aseem%40152123@localhost:5433/orbit"
    engine = create_engine(DB_URL)
    excel_files = list(FOLDER.glob("*.xlsb")) + list(FOLDER.glob("*.xlsx"))
    print(f"Folder found {len(excel_files)} Excel Files")

    dfs = []

    for file in excel_files:
        print(f" Processing {file.name}")
        ingestor = ExpenseIngestor(file,DB_URL)
        ingestor.load_excel(sheet_name=0).clean().categorize()
        df = ingestor.df

      
        df["source_file"] = file.name
        dfs.append(df)

    if dfs:
        all_columns = sorted({c for df in dfs for c in df.columns})
        for i, df in enumerate(dfs):
            missing = [c for c in all_columns if c not in df.columns]
            for c in missing:
                df[c] = pd.NA
            dfs[i] = df[all_columns]

        sales_all = pd.concat(dfs, ignore_index= True)
        sales_all = sales_all.loc[:,~sales_all.columns.duplicated()]
        #create a table 
        sales_all.to_sql("sales_consol",engine, index=False, if_exists="replace")

        print(f"consolidated {len(sales_all)} rows from {len(excel_files)} files")
        
        print(f"Table 'sales_consol' created in the database")
     
    else:
        print("No data frames created")