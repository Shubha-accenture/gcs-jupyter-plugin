
import json

from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
import tornado

from gcs_jupyter_plugin import credentials, urls
from gcs_jupyter_plugin.controllers.gcs import (
    ListBucketsController,
    ListFilesController,
    LoadFileController,
    CreateFolderController,
    SaveFileController,
)


class CredentialsHandler(APIHandler):
    # The following decorator should be present on all verb methods (head, get, post,
    # patch, put, delete, options) to ensure only authorized user can request the
    # Jupyter server
    @tornado.web.authenticated
    async def get(self):
        cached = await credentials.get_cached()
        if cached["config_error"] == 1:
            self.log.exception(f"Error fetching credentials from gcloud")
        self.finish(json.dumps(cached))


class UrlHandler(APIHandler):
    url = {}

    @tornado.web.authenticated
    async def get(self):
        url_map = await urls.map()
        self.log.info(f"Service URL map: {url_map}")
        self.finish(url_map)
        return


class LogHandler(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        logger = self.log.getChild("DataprocPluginClient")
        log_body = self.get_json_body()
        logger.log(log_body["level"], log_body["message"])
        self.finish({"status": "OK"})


def setup_handlers(web_app):
    host_pattern = ".*$"

    base_url = web_app.settings["base_url"]
    application_url = "gcs-jupyter-plugin"

    def full_path(name):
        return url_path_join(base_url, application_url, name)

    handlersMap = {
        "credentials": CredentialsHandler,
        "getGcpServiceUrls": UrlHandler,
        "log": LogHandler,
        "api/storage/listBuckets": ListBucketsController,
        "api/storage/listFiles": ListFilesController,
        "api/storage/loadFile": LoadFileController,
        "api/storage/createFolder": CreateFolderController,
        "api/storage/saveFile": SaveFileController,
    }
    handlers = [(full_path(name), handler) for name, handler in handlersMap.items()]
    web_app.add_handlers(host_pattern, handlers)
