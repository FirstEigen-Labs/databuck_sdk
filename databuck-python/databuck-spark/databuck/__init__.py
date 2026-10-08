import os

from .sdk import DataBuck

if os.environ.get("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", "1").lower() not in ("0", "false", "no"):
    os.environ["DATABUCK_SPARK_SDK_JAR"] = DataBuck.download_jar()

__all__ = ["DataBuck"]
