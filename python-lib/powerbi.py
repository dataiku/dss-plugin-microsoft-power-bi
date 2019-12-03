import sys, json, requests, datetime, logging

# Data types mapping DSS => Power BI
fieldSetterMap = {
    'boolean':  'Boolean',
    'tinyint':  'Int64',
    'smallint': 'Int64',
    'int':      'Int64',
    'bigint':   'Int64',
    'float':    'Double',
    'double':   'Double',
    'date':     'String',
    'string':   'String',
    'array':    'String',
    'map':      'String',
    'object':   'String'
}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='power-bi plugin %(levelname)s - %(message)s')

# Main interactor object
class PowerBI(object):
    
    def __init__(self, token):
        self.token = token
        self.headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/json'
        }
        
    def get_datasets(self):
        endpoint = 'https://api.powerbi.com/v1.0/myorg/datasets'
        response = requests.get(endpoint, headers=self.headers)
        return response
    
    def get_dataset_by_name(self, name):
        data = self.get_datasets()
        datasets = data.json().get('value')
        ret = []
        if datasets:
            for dataset in datasets:
                if dataset['name'] == name:
                    ret.append(dataset['id'])
        return ret
    
    def delete_dataset(self, dsid):
        endpoint = 'https://api.powerbi.com/v1.0/myorg/datasets/{}'.format(dsid)
        response = requests.delete(endpoint, headers=self.headers)
        logger.info("[+] Deleted existing Power BI dataset {} (response code: {})...".format(
            dsid, response.status_code
        ))
        return response

    def create_dataset_from_schema(self, pbi_dataset=None, pbi_table=None, schema=None):
        # Build the Power BI Dataset schema
        columns = []
        for column in schema["columns"]:
            new_column = {}
            new_column["name"] = column["name"]
            new_column["dataType"] = fieldSetterMap.get(column["type"], "String")
            columns.append(new_column)
        # Power BI dataset definition
        payload = {
            "name": pbi_dataset,
            "defaultMode" : "PushStreaming",
            "tables": [
                {
                    "name": pbi_table,
                    "columns": columns
                }
            ]
        }
        response = requests.post(
            "https://api.powerbi.com/v1.0/myorg/datasets", 
            data=json.dumps(payload), 
            headers=self.headers
        )
        return response.json()

def generate_access_token(username=None, password=None, client_id=None, client_secret=None):
    """
      Call the Azure API's to retrieve an access token to interact with Power BI.
      Requires full credentials to be passed.
    """
    data = {
        "username"     : username,
        "password"     : password,
        "client_id"    : client_id,
        "client_secret": client_secret,
        "resource"     : "https://analysis.windows.net/powerbi/api",
        "grant_type"   : "password",
        "scope"        : "openid"
    }
    response = requests.post('https://login.microsoftonline.com/common/oauth2/token', data=data)
    return response.json()
    