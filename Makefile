PLUGIN_VERSION=2.0.2
PLUGIN_ID=microsoft-power-bi-v2

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip parameter-sets python-exporters code-env plugin.json python-lib
