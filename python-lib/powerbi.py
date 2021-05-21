import json
import requests
import logging

BASE_URL = "https://api.powerbi.com/v1.0/myorg"

GROUPS_API = BASE_URL + "/groups"
DATASETS_API = BASE_URL + "/datasets"
GROUP_DATASETS_API = BASE_URL + "/groups/{group_id}/datasets"
TABLE_ROWS_API = "{}/{}/tables/{}/rows"
API_404 = BASE_URL + "/lalala"
DEFAULT_PBI_TABLE = "dss-data"

# Data types mapping DSS => Power BI
fieldSetterMap = {
    'boolean':  'Boolean',
    'tinyint':  'Int64',
    'smallint': 'Int64',
    'int':      'Int64',
    'bigint':   'Int64',
    'float':    'Double',
    'double':   'Double',
    'date':     'dateTime',
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
        self.columns_with_date = None

    def get_datasets(self, pbi_group_id=None):
        endpoint = self.get_datasets_base_url(pbi_group_id=pbi_group_id)
        response = requests.get(endpoint, headers=self.headers)
        return response

    def get_dataset_by_name(self, name, pbi_group_id=None):
        data = self.get_datasets(pbi_group_id=pbi_group_id)
        datasets = data.json().get('value')
        ret = []
        if datasets:
            for dataset in datasets:
                if dataset['name'] == name:
                    ret.append(dataset['id'])
        return ret

    def delete_dataset(self, dsid, pbi_group_id=None):
        endpoint = '{}/{}'.format(self.get_datasets_base_url(pbi_group_id=pbi_group_id), dsid)
        response = requests.delete(endpoint, headers=self.headers)
        assert_response_ok(response, while_trying="deleting {}".format(dsid))
        logger.info("[+] Deleted existing Power BI dataset {} (response code: {})...".format(
            dsid, response.status_code
        ))
        return response

    def create_dataset_from_schema(self, pbi_dataset=None, pbi_table=DEFAULT_PBI_TABLE, pbi_group_id=None, schema=None):
        # Build the Power BI Dataset schema
        columns = []
        for column in schema["columns"]:
            new_column = {}
            new_column["name"] = column["name"]
            new_column["dataType"] = fieldSetterMap.get(column["type"], "String")
            columns.append(new_column)
        payload = {
            "name": pbi_dataset,
            "defaultMode": "PushStreaming",
            "tables": [
                {
                    "name": pbi_table,
                    "columns": columns
                }
            ]
        }

        json_response = self.post(
            self.get_datasets_base_url(pbi_group_id=pbi_group_id),
            data=json.dumps(payload)
        )
        return json_response

    def prepare_date_columns(self, schema):
        self.columns_with_date = []
        for column in schema["columns"]:
            if column["type"] == "date":
                self.columns_with_date.append(column["name"])
        if len(self.columns_with_date) > 0:
            self.json_filter = self.parse_json_dates
        else:
            self.json_filter = json.dumps

    def get_group_id_by_name(self, pbi_workspace=None):
        if pbi_workspace is None or pbi_workspace == "My workspace":
            return None
        json_response = self.get(GROUPS_API)
        groups = json_response.get("value", [])
        group = self.filter_group_by_name(groups, pbi_workspace)
        group_id = group.get("id")
        if group_id is None:
            raise Exception("The workspace named \"{workspace}\" does not exists on your Power BI account, or you do not have access to it".format(workspace=pbi_workspace))
        return group_id

    def filter_group_by_name(self, groups, pbi_workspace):
        lowercase_workspace_name = pbi_workspace.lower()
        for group in groups:
            if group.get("name", "").lower() == lowercase_workspace_name:
                return group
        return {}

    def get_datasets_base_url(self, pbi_group_id=None):
        # https://powerbi.microsoft.com/fr-fr/blog/introducing-the-power-bi-rest-api-v1-0/
        # https://api.powerbi.com/v1.0/myorg/datasets ->
        # https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets
        if pbi_group_id is None:
            ret = DATASETS_API
        else:
            ret = GROUP_DATASETS_API.format(group_id=pbi_group_id)
        return ret

    def get_datasets_autorefresh_url(self, pbi_group_id=None):
        return self.get_datasets_base_url(pbi_group_id) + "/{dataset_id}/refreshes"

    def refresh_dataset(self, dsid, pbi_group_id=None):
        endpoint = self.get_datasets_autorefresh_url(pbi_group_id=pbi_group_id).format(dataset_id=dsid)
        response = requests.post(endpoint, headers=self.headers)
        assert_response_ok(response, while_trying="Refreshed dataset {}".format(dsid))
        logger.info("[+] Refreshed Power BI dataset {} (response code: {})...".format(
            dsid, response.status_code
        ))
        return response

    def get(self, url):
        response = requests.get(url, headers=self.headers)
        assert_response_ok(response)
        json_response = response.json()
        return json_response

    def post(self, url, data, fail_on_errors=True):
        response = requests.post(
            url,
            data=data,
            headers=self.headers
        )
        assert_response_ok(response, fail_on_errors=fail_on_errors)
        if is_json_response(response):
            return response.json()
        else:
            return response

    def post_table_row(self, rows, dsid, pbi_table=DEFAULT_PBI_TABLE, pbi_group_id=None):
        new_data = self.json_filter(rows)
        response = self.post(
            TABLE_ROWS_API.format(
                self.get_datasets_base_url(pbi_group_id=pbi_group_id),
                dsid,
                pbi_table
            ),
            data=new_data,  # json.dumps(rows),
            fail_on_errors=False
        )
        return response

    def parse_json_dates(self, rows):
        ret = []
        for row in rows:
            for column_with_date in self.columns_with_date:
                row[column_with_date] = date_convertion(row[column_with_date])
            ret.append(row)
        return json.dumps(ret)


def date_convertion(pandas_date):
    ret = pandas_date.isoformat()
    if ret == "NaT":
        ret = None
    return ret


def is_json_response(response):
    return response.headers.get('content-type').find("application/json") >= 0


def assert_response_ok(response, while_trying=None, fail_on_errors=True):
    if response.status_code >= 400:
        if while_trying is None:
            handle_exception_message("Error {}: {}".format(response.status_code, response.content), fail_on_errors=fail_on_errors)
        else:
            handle_exception_message("Error {} while {}: {}".format(response.status_code, while_trying, response.content), fail_on_errors=fail_on_errors)


def handle_exception_message(message, fail_on_errors=True):
    if fail_on_errors:
        raise Exception(message)
    else:
        logger.error(message)


def generate_access_token(username=None, password=None, client_id=None, client_secret=None):
    """
      Call the Azure API's to retrieve an access token to interact with Power BI.
      Requires full credentials to be passed.
    """
    data = {
        "username": username,
        "password": password,
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://analysis.windows.net/powerbi/api",
        "grant_type": "password",
        "scope": "openid"
    }
    response = requests.post('https://login.microsoftonline.com/common/oauth2/token', data=data)
    assert_response_ok(response, while_trying="retrieving access token")
    return response.json()
