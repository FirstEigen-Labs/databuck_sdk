"""Download the DataBuck Spark SDK JAR after installing the Python package."""

from .sdk import DataBuck


if __name__ == "__main__":
    print(DataBuck.download_jar())
