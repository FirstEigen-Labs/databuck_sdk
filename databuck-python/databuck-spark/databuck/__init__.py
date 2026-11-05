import os

from .license import LicenseError, download_config_file, require_license

auto_download_setting = (os.environ.get("DATABUCK_SDK_AUTO_DOWNLOAD")
                         or os.environ.get("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD")
                         or "1")
auto_download = auto_download_setting.lower() not in (
    "0", "false", "no"
)
if auto_download:
    os.environ["DATABUCK_SDK_CONFIG_FILE"] = download_config_file()
require_license()

from .sdk import DataBuck

if auto_download:
    os.environ["DATABUCK_SDK_JAR"] = DataBuck.download_jar()

__all__ = ["DataBuck", "LicenseError"]
