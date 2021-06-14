import json
from powerbi import PowerBI, generate_access_token
from dataiku.exporter import Exporter
from math import isnan
from safe_logger import SafeLogger

logger = SafeLogger("power-bi-v2 plugin", forbiden_keys=["ms-oauth_credentials", "password", "client-secret"])


class PowerBIExporter(Exporter):

    EMPTY_CONNECTION = {"username": None, "password": None, "client-id": None, "client-secret": None}

    def __init__(self, config, plugin_config):
        logger.info("config={}, plugin_config={}".format(logger.filter_secrets(config), logger.filter_secrets(plugin_config)))
        self.config = config
        self.plugin_config = plugin_config
        self.row_index = 0
        self.row_buffer = {}
        self.row_buffer["rows"] = []

        self.pbi_dataset = self.config.get("dataset", None)
        self.pbi_workspace = self.config.get("workspace", None)
        if self.pbi_workspace == "":
            self.pbi_workspace = None
        self.pbi_table = "dss-data"
        self.pbi_buffer_size = self.config.get("buffer_size", None)

        self.export_method = self.config.get("export_method", None)

        authentication_method = self.config.get("authentication_method", None)
        if authentication_method == "oauth":
            access_token = self.get_oauth_token_from_config(config)
            self.pbi = PowerBI(access_token)
            self.pbi_group_id = self.pbi.get_group_id_by_name(self.pbi_workspace)
        elif authentication_method == "credentials":
            basic_connection = self.config.get("basic_connection", self.EMPTY_CONNECTION)
            self.username = basic_connection.get("username", None)
            self.password = basic_connection.get("password", None)
            self.client_id = basic_connection.get("client-id", None)
            self.client_secret = basic_connection.get("client-secret", None)
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
            self.pbi = PowerBI(token)
            self.pbi_group_id = self.pbi.get_group_id_by_name(self.pbi_workspace)

    def get_oauth_token_from_config(self, config):
        access_token = config.get('powerbi_connection', {}).get('ms-oauth_credentials')
        if access_token is None:
            logger.error("ERROR [-] Error while reading your Power BI access token from Project Variables")
            raise Exception("Authentication error")
        if isinstance(access_token, dict):
            NO_INLINE_ERROR_MESSAGE = "OAuth settings cannot be used inlined. Please define a preset in Plugins > Microsoft Power BI v2 > Settings > Azure Single Sign On"
            logger.error("ERROR [-] {}".format(NO_INLINE_ERROR_MESSAGE))
            raise Exception("Authentication error. {}".format(NO_INLINE_ERROR_MESSAGE))
        return access_token

    def open(self, schema):
        self.schema = schema
        self.pbi.prepare_date_columns(self.schema)

        if self.export_method == "overwrite":
            datasets = self.pbi.get_dataset_by_name(self.pbi_dataset, pbi_group_id=self.pbi_group_id)
            if len(datasets) > 0:
                logger.warning("Emptying dataset {}".format(datasets[0]))
                self.pbi.empty_dataset(datasets[0], pbi_table=self.pbi_table, pbi_group_id=self.pbi_group_id)
                self.dsid = datasets[0]
                logger.info("[+] First emptied Power BI dataset ID for overwrite {}".format(self.dsid))
            else:
                logger.error("ERROR [-] No existing dataset with name {}".format(self.pbi_dataset))
                logger.error("ERROR [-] Select 'Create new dataset' to create a new one")
                raise Exception("Cannot overwrite: no existing dataset with name {}".format(self.pbi_dataset))

        elif self.export_method == "append":
            datasets = self.pbi.get_dataset_by_name(self.pbi_dataset, pbi_group_id=self.pbi_group_id)
            if len(datasets) > 0:
                self.dsid = datasets[0]
                logger.info("[+] Will append to Power BI dataset ID {}".format(self.dsid))
            else:
                logger.error("ERROR [-] No existing dataset with name {}".format(self.pbi_dataset))
                logger.error("ERROR [-] Select 'Create new dataset' to create a new one")
                raise Exception("Cannot overwrite: no existing dataset with name {}".format(self.pbi_dataset))

        else:  # new_dataset
            datasets = self.pbi.get_dataset_by_name(self.pbi_dataset, pbi_group_id=self.pbi_group_id)
            if len(datasets) > 0:
                logger.error("ERROR [-] Dataset with name {} already exists".format(self.pbi_dataset))
                raise Exception("Dataset '{}' already exists".format(self.pbi_dataset))
            response = self.pbi.create_dataset_from_schema(
                    pbi_dataset=self.pbi_dataset,
                    pbi_table=self.pbi_table,
                    pbi_group_id=self.pbi_group_id,
                    schema=schema
            )
            if response.get("id") is None:
                logger.error("ERROR [-] Error while creating your Power BI dataset.")
                logger.error("ERROR [-] Azure response:")
                logger.error(json.dumps(response, indent=4))
                raise Exception("Dataset creation error probably from Azure")

            self.dsid = response["id"]
            logger.info("[+] Created new Power BI dataset ID {}".format(self.dsid))

    def write_row(self, row):
        row_obj = {}
        for (col, val) in zip(self.schema["columns"], row):
            if col['type'] in ['int', 'bigint', 'tinyint', 'smallint']:
                row_obj[col["name"]] = int(val) if val is not None and not isnan(val) else None
            else:
                row_obj[col["name"]] = val
        self.row_buffer["rows"].append(row_obj)
        if len(self.row_buffer["rows"]) > self.pbi_buffer_size:
            self.pbi.post_table_row(
                self.row_buffer["rows"],
                self.dsid,
                self.pbi_table,
                pbi_group_id=self.pbi_group_id
            )
            self.row_buffer["rows"] = []
        self.row_index += 1

    def close(self):
        if len(self.row_buffer["rows"]) > 0:
            self.pbi.post_table_row(
                self.row_buffer["rows"],
                self.dsid,
                self.pbi_table,
                pbi_group_id=self.pbi_group_id
            )
        logger.info("[+] Loading complete.")
        msg = ""
        msg = msg + "[+] {}".format("="*80) + "\n"
        msg = msg + "[+] Your Power BI dataset should be available at:" + "\n"
        msg = msg + "[+] https://app.powerbi.com/groups/me/datasets/{}".format(self.dsid) + "\n"
        msg = msg + "[+] {}".format("="*80)
        logger.info(msg)
