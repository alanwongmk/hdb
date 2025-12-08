import requests
import pandas as pd

#=====================================================================================
# Class: Downloader
#=====================================================================================
class Downloader():

    def __init__(self):
        self.nrecords = 0
        self.nfiles = 0
        self.getMetadata()

    def getMetadata(self):
        collection_id = 189          
        url = "https://api-production.data.gov.sg/v2/public/api/collections/{}/metadata".format(collection_id)

        response = requests.get(url)
        child_datasets = response.json()['data']['collectionMetadata']['childDatasets']

        self.child_datasets = child_datasets
        return child_datasets

    def download_dataset(self, child_id):
        
        poll_url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{child_id}/poll-download"
        print(poll_url)

        headers = {}
        
        resp = requests.get(poll_url, headers=headers)
        data = resp.json()
        
        if data.get("code") != 0:
            raise RuntimeError(f"Download failed: {data.get('errorMsg') or data}")
        
        download_url = data["data"]["url"]
        #simple_url= "/".join(download_url.split("/")[3:5]) + "..."
    
        output_filename = f"../Project-HDB-Store/DataLake/hdb_data_{child_id}.csv"
        file_resp = requests.get(download_url)
        with open(output_filename, "wb") as f:
            f.write(file_resp.content)

        s = file_resp.content.decode("utf-8")
        nrecords = len(s.split("\n")) - 2
        self.nrecords += nrecords
        self.nfiles += 1
        print(f"Downloading File#..: {self.index+1}")
        print(f"Download Child ID..: {child_id}")
        print(f"Saved to...........: {output_filename}")
        print(f"Number of records..: {nrecords}")
        print("-" * 100)

    def execute(self):
        child_datasets = self.getMetadata()
        for index, child_id in enumerate(child_datasets):
            self.index = index
            self.download_dataset(child_id)

#=====================================================================================
# Function: combineDownloadedFiles
#=====================================================================================
def combineDownloadedFiles(downloader):
    child_datasets = downloader.child_datasets
    
    #x = []
    counts = []
    df_hdbs = []
    
    for collection_id in child_datasets:
        df = pd.read_csv(f"../Project-HDB-Store/DataLake/hdb_data_{collection_id}.csv")
        if "remaining_lease" in df.columns:
            del df["remaining_lease"]
        #x.append(df.iloc[[0, 1, 2, 3, 4]]) #---Only take samples
        counts.append(len(df))
        df_hdbs.append(df)
    
    hdb_data = pd.concat(df_hdbs)
    hdb_data["year_month"] = pd.to_datetime(hdb_data["month"], format="%Y-%m")
    hdb_data = hdb_data.sort_values("year_month")
    hdb_data = hdb_data.set_index("year_month")
    hdb_data = hdb_data.drop('month', axis=1)

    hdb_data.to_csv("../Project-HDB-Store/Staging/Main.csv")
    print("Combined File Written in Staging/Main.csv")
    print("Total: ", sum(counts))
    print()
    
    return hdb_data
