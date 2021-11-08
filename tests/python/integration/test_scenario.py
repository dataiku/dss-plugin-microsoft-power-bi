from dku_plugin_test_utils import dss_scenario

TEST_PROJECT_KEY = "PLUGINTESTPOWERBI"


def test_run_powerbi_onedrive_authentication_test(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="AuthenticationTest")


def test_run_powerbi_export_to_workspace(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="ExportToWorkspace")

# def test_run_powerbi_azure_oauth_export_to_workspace(user_dss_clients):
    # dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="AzureOAuthExportToWorkspace")
