from sqlalchemy import create_engine
import pandas as pd

user = "postgres"
password = "admin"
host = "localhost"
port = "5432"
database = "hdb"

class PSQL:

    #---Initialization---------------------------------------------------------------------
    def __init__(self):
        self.engine = None
        self.get_connection()

    #---Get Database Connection------------------------------------------------------------
    def get_connection(self):
        # PostgreSQL connection URL
        connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        
        # Create SQLAlchemy engine
        engine = create_engine(connection_url, isolation_level="AUTOCOMMIT")
        
        # Test connection
        try:
            with engine.connect() as connection:
                print("Connected successfully!")
        except Exception as e:
            print("Connection failed:", e)

        self.engine = engine
        self.connection_url = connection_url

    #---Execute SQL Query------------------------------------------------------------------
    def query(self,sql, quiet=True):

        if str(sql).strip()=='':
            return None

        results =[]
        with self.engine.connect() as conn:
            result = conn.execute(sql)
            rows = result.fetchall()
    
        count=0
        for row in rows:
            if count<5 and not quiet:
                print(row)
                count += 1
            results.append(row)
        
        df = pd.DataFrame(results)
        df.index = df.index + 1
        print(f"Total Rows: {len(df)}")
        return df 

    #---Execute SQL Query------------------------------------------------------------------
    def execute(self,sql, quiet=True):

        if str(sql).strip()=='':
            return None

        results =[]
        with self.engine.connect() as conn:
            result = conn.execute(sql)

        return result