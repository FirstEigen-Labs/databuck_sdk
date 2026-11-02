import os

from .license import LicenseError, require_license

require_license()

from .sdk import DataBuck, _jar_setting

if _jar_setting("AUTO_DOWNLOAD", "1").lower() not in ("0", "false", "no"):
    os.environ["DATABUCK_SDK_JAR"] = DataBuck.download_jar()

__all__ = ["DataBuck", "LicenseError"]
