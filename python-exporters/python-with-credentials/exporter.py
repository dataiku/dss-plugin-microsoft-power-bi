import os, json, requests, logging
from powerbi import *
from dataiku.exporter import Exporter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='power-bi plugin %(levelname)s - %(message)s')

class PowerBIExporter(Exporter):
    
    def __init__(self, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config
        self.row_index = 0
        self.row_buffer = {}
        self.row_buffer["rows"] = []
        # Plugin settings
        self.username        = self.config.get("username",      None)
        self.password        = self.config.get("password",      None)
        self.client_id       = self.config.get("client-id",     None)
        self.client_secret   = self.config.get("client-secret", None)        
        self.pbi_dataset     = self.config.get("dataset",       None)
        self.pbi_table       = "dss-data"
        self.pbi_overwrite   = self.config.get("overwrite",     None)
        self.pbi_buffer_size = self.config.get("buffer_size",   None)
        # Retrieve access token
        response = generate_access_token(
            self.username,
            self.password,
            self.client_id,
            self.client_secret
        )
        token = response.get("access_token")
        if token is None:
            logger.error("ERROR [-] Error while retrieving your Power BI access token, please check your credentials.")
            logger.error("ERROR [-] Azure authentication API response:")
            logger.error(json.dumps(response, indent=4))
            raise Exception("Authentication error")
        # Interacting with Power BI API's
        self.headers = {
            'Authorization': 'Bearer ' + token,
            'Content-Type': 'application/json'
        }
        self.pbi = PowerBI(token)

    def open(self, schema):
        self.schema = schema
        if self.pbi_overwrite:
            logger.info("[+] Looking for Power BI datasets with similar names...")
            datasets = self.pbi.get_dataset_by_name(self.pbi_dataset)
            for dataset in datasets:
                self.pbi.delete_dataset(dataset)
            response = self.pbi.create_dataset_from_schema(
                pbi_dataset=self.pbi_dataset, 
                pbi_table=self.pbi_table, 
                schema=schema
            )
            if response.get("id") is None:
                logger.error("ERROR [-] Error while creating your Power BI dataset.")
                logger.error("ERROR [-] Azure response:")
                logger.error(json.dumps(response, indent=4))
                raise Exception("Dataset creation error")
            self.dsid = response["id"]
            logger.info("[+] Created Power BI dataset ID {}".format(self.dsid))
        else:
            datasets = self.pbi.get_dataset_by_name(self.pbi_dataset)
            if len(datasets) > 0:
                logger.info("[+] Existing datasets: {}".format(datasets))
                self.dsid = datasets[0]
                logger.info("[+] Will use Power BI dataset ID {}".format(self.dsid))
            else:
                logger.error("ERROR [-] No existing dataset with name {}".format(self.pbi_dataset))
                logger.info("ERROR [-] Check 'Overwrite' to create a new one")
                raise Exception("Dataset creation error")
                
            
    def write_row(self, row):
        row_obj = {}
        for (col, val) in zip(self.schema["columns"], row):
            if col['type'] in ['int', 'bigint', 'tinyint', 'smallint']:
                row_obj[col["name"]] = int(val)
            else:
                row_obj[col["name"]] = val
        self.row_buffer["rows"].append(row_obj)
        if len(self.row_buffer["rows"]) > self.pbi_buffer_size:
            response = requests.post(
                "https://api.powerbi.com/v1.0/myorg/datasets/{}/tables/{}/rows".format(
                    self.dsid, 
                    self.pbi_table
                ),
                data=json.dumps(self.row_buffer["rows"]),
                headers=self.headers
            )
            logger.info("[+] Inserted {} records (response code: {})".format(
                len(self.row_buffer["rows"]), 
                response.status_code
            ))
            if not str(response.status_code).startswith('2'):
                logger.info("[-] Response code {} may indicate an issue while loading your records.".format(response.status_code))
                logger.info("[-] API response: {}".format(response.json()))
            self.row_buffer["rows"] = []
        self.row_index += 1
        
    def close(self):
        if len(self.row_buffer["rows"]) > 0:
            response = requests.post(
                "https://api.powerbi.com/v1.0/myorg/datasets/{}/tables/{}/rows".format(
                    self.dsid, 
                    self.pbi_table
                ),
                data=json.dumps(self.row_buffer["rows"]),
                headers=self.headers
            )
            logger.info("[+] Inserted {} records (response code: {})".format(
                len(self.row_buffer["rows"]), 
                response.status_code
            ))
            if not str(response.status_code).startswith('2'):
                logger.warning("[-] Response code {} may indicate an issue while loading your records.".format(response.status_code))
                logger.warning("[-] API response: {}".format(response.json()))
        logger.info("[+] Loading complete.")
        msg = ""
        msg = msg + "[+] {}".format("="*80) + "\n"
        msg = msg + "[+] Your Power BI dataset should be available at:" + "\n"
        msg = msg + "[+] https://app.powerbi.com/groups/me/datasets/{}".format(self.dsid) + "\n"
        msg = msg + "[+] {}".format("="*80)
        logger.info(msg)
