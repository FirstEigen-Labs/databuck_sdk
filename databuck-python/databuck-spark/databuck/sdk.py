import hashlib
import os
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path


JAR_URL = "https://tmplog1-pub.s3.us-east-1.amazonaws.com/databuck-spark-sdk.jar"
JAR_NAME = "databuck-spark-sdk.jar"
JAR_SHA256 = "c85e35442f9416bdfbe69d66e3e786affa3b1861d526a1cf363bb25a34f470e6"


class DataBuck:

    @staticmethod
    def jar_path() -> str:
        configured = os.environ.get("DATABUCK_SPARK_SDK_JAR")
        jar = (Path(configured).expanduser() if configured else
               Path(__file__).resolve().parent / "jars" / JAR_NAME)
        if not jar.is_file():
            raise FileNotFoundError(
                f"DataBuck Spark SDK JAR was not found at {jar}. "
                "Run `python -m databuck` to download it, or set "
                "DATABUCK_SPARK_SDK_JAR to an existing local JAR path."
            )
        return str(jar.resolve())

    @staticmethod
    def download_jar(destination=None) -> str:
        """Download the Spark JAR to the package path or a configured path."""
        if destination is None:
            configured = os.environ.get("DATABUCK_SPARK_SDK_JAR")
            destination = (Path(configured).expanduser() if configured else
                           Path(__file__).resolve().parent / "jars" / JAR_NAME)
        jar = Path(destination).expanduser()
        if jar.is_file():
            return str(jar.resolve())

        url = os.environ.get("DATABUCK_SPARK_SDK_JAR_URL", JAR_URL)
        expected_sha256 = os.environ.get("DATABUCK_SPARK_SDK_JAR_SHA256", JAR_SHA256)
        temporary = None
        try:
            jar.parent.mkdir(parents=True, exist_ok=True)
            print(f"databuck: downloading Spark JAR to {jar}", file=sys.stderr)
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=f".{JAR_NAME}.", suffix=".tmp",
                dir=jar.parent, delete=False
            ) as output:
                temporary = Path(output.name)
                digest = hashlib.sha256()
                with urllib.request.urlopen(url, timeout=60) as response:
                    total = None
                    if getattr(response, "headers", None) is not None:
                        total = response.headers.get("Content-Length")
                    total = int(total) if total else None
                    downloaded = 0
                    next_report = 50 * 1024 * 1024
                    while chunk := response.read(1024 * 1024):
                        output.write(chunk)
                        digest.update(chunk)
                        downloaded += len(chunk)
                        if downloaded >= next_report:
                            progress = f" / {total / (1024 * 1024):.1f} MiB" if total else ""
                            print(
                                f"databuck: downloaded {downloaded / (1024 * 1024):.1f} MiB{progress}",
                                file=sys.stderr,
                            )
                            next_report += 50 * 1024 * 1024
            if not zipfile.is_zipfile(temporary):
                raise ValueError("The downloaded file is not a valid JAR archive")
            if digest.hexdigest().lower() != expected_sha256.lower():
                raise ValueError("The downloaded JAR failed SHA-256 verification")
            os.replace(temporary, jar)
            print(f"databuck: JAR ready at {jar}", file=sys.stderr)
        except (OSError, ValueError, urllib.error.URLError) as exc:
            raise RuntimeError(
                f"Could not download the DataBuck Spark SDK JAR from {url} "
                f"to {jar}: {exc}. Set DATABUCK_SPARK_SDK_JAR to an existing "
                "local JAR path or DATABUCK_SPARK_SDK_JAR_URL to a reachable URL. "
                "If the JAR changes, also set DATABUCK_SPARK_SDK_JAR_SHA256."
            ) from exc
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return str(jar.resolve())

    # ------------------------------------------------------------------
    # PROFILE
    # ------------------------------------------------------------------

    @staticmethod
    def profile(df):
        return (
            df.sparkSession.sparkContext._jvm
            .com.databuck.sdk.DataBuckSDK
            .profile(df._jdf)
        )

    # ------------------------------------------------------------------
    # JAVA -> PYTHON CONVERSION HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _java_list_to_py(value):
        if value is None:
            return []

        return [value.get(i) for i in range(value.size())]

    @staticmethod
    def _java_set_to_py(value):
        if value is None:
            return []

        iterator = value.iterator()
        output = []

        while iterator.hasNext():
            output.append(iterator.next())

        return output

    @staticmethod
    def _java_map_to_py(value):
        if value is None:
            return {}

        output = {}
        iterator = value.entrySet().iterator()

        while iterator.hasNext():
            entry = iterator.next()
            output[entry.getKey()] = entry.getValue()

        return output

    @staticmethod
    def _py_map_to_java(jvm, values, value_type=str):
        jmap = jvm.java.util.LinkedHashMap()

        if values:
            for key, value in values.items():
                jmap.put(key, value_type(value))

        return jmap

    @staticmethod
    def _py_list_to_java(jvm, values, value_type=str):
        jlist = jvm.java.util.ArrayList()

        if values:
            for value in values:
                jlist.add(value_type(value))

        return jlist

    @staticmethod
    def _java_column_profile_to_py(profile):
        if profile is None:
            return None

        return {
            "column_name": profile.getColumnName(),
            "data_type": profile.getDataType(),
            "total_record_count": profile.getTotalRecordCount(),

            "percentage_missing": profile.getPercentageMissing(),
            "missing_value_count": profile.getMissingValueCount(),

            "unique_percentage": profile.getUniquePercentage(),
            "unique_count": profile.getUniqueCount(),

            "date_format": profile.getDateFormat(),

            "length_type": profile.getLengthType(),
            "fixed_length_value": profile.getFixedLengthValue(),
            "length_values": profile.getLengthValues(),

            "bad_data_check_eligible": profile.isBadDataCheckEligible(),

            "top_patterns": profile.getTopPatterns(),
            "all_patterns": profile.getAllPatterns(),

            "min_value": profile.getMinValue(),
            "max_value": profile.getMaxValue(),
            "mean": profile.getMean(),
            "std_dev": profile.getStdDev(),

            "percentile_1": profile.getPercentile1(),
            "percentile_25": profile.getPercentile25(),
            "percentile_75": profile.getPercentile75(),
            "percentile_99": profile.getPercentile99(),

            "min_length": profile.getMinLength(),
            "max_length": profile.getMaxLength(),
        }

    @staticmethod
    def _java_column_profiles_to_py(value):
        if value is None:
            return []

        output = []

        for i in range(value.size()):
            profile = value.get(i)

            converted = DataBuck._java_column_profile_to_py(profile)

            if converted is not None:
                output.append(converted)

        return output

    # ------------------------------------------------------------------
    # DISCOVER PROFILE
    # ------------------------------------------------------------------

    @staticmethod
    def discover_profile(df):
        """
        Run Java DataBuck profiling and expose the complete
        SDKProfileResult as Python dictionaries/lists.

        This method represents profiling/discovery information.
        It does not convert the information into executable rules.
        """

        profile = DataBuck.profile(df)

        return {
            "record_count": {
                "full": profile.getFullRecordCount(),
                "sampled": profile.getSampledRecordCount(),
            },

            "null_check": {
                "columns": DataBuck._java_list_to_py(
                    profile.getNullColList()
                ),
                "thresholds": DataBuck._java_map_to_py(
                    profile.getNullThreshold()
                ),
            },

            "primary_key": DataBuck._java_list_to_py(
                profile.getPrimarycolList()
            ),

            "duplicate_key": DataBuck._java_list_to_py(
                profile.getDuplicateKeyColList()
            ),

            "pattern_check": DataBuck._java_map_to_py(
                profile.getDefaultPatternCheckColumns()
            ),

            "all_pattern_frequencies": DataBuck._java_map_to_py(
                profile.getAllPatternFrequencies()
            ),

            "length_check": DataBuck._java_map_to_py(
                profile.getLengthRelationship()
            ),

            "data_drift": DataBuck._java_list_to_py(
                profile.getDataDriftColsList()
            ),

            "microsegment": DataBuck._java_set_to_py(
                profile.getMicrosegmentCols()
            ),

            "numerical_relationship": DataBuck._java_list_to_py(
                profile.getNumericalRelationship()
            ),

            "date_relationship": DataBuck._java_set_to_py(
                profile.getDateRelationship()
            ),

            "bad_data": DataBuck._java_list_to_py(
                profile.getBadDataColList()
            ),

            "string_columns": DataBuck._java_list_to_py(
                profile.getStringColumns()
            ),

            "numeric_columns": DataBuck._java_list_to_py(
                profile.getNumericColumns()
            ),

            "decimal_columns": DataBuck._java_list_to_py(
                profile.getDecimalColumns()
            ),

            "date_columns": DataBuck._java_map_to_py(
                profile.getDateColumns()
            ),

            "partition_column": profile.getPartitionColumn(),

            "column_profiles": DataBuck._java_column_profiles_to_py(
                profile.getColumnProfiles()
            ),
        }

    # ------------------------------------------------------------------
    # SDK RULE GENERATION
    # ------------------------------------------------------------------

    @staticmethod
    def discover_rules(df):
        jrules = (
            df.sparkSession.sparkContext._jvm
            .com.databuck.sdk.DataBuckSDK
            .discoverRules(df._jdf)
        )

        from .lake_rules import DiscoveredRules

        rules = DiscoveredRules()

        for i in range(jrules.size()):
            rule = jrules.get(i)

            rules.append({
                "ruleId": rule.getRuleId(),
                "ruleType": rule.getRuleType(),
                "columnName": rule.getColumnName(),
                "ruleName": rule.getRuleName(),
                "expression": rule.getExpression(),
                "threshold": rule.getThreshold(),
                "parameters": DataBuck._java_map_to_py(
                    rule.getParameters()
                ),
            })

        return rules

    # ------------------------------------------------------------------
    # NULL CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def null_check(
        df,
        id_app: int,
        run: int = 1,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.nullcheck
            .SDKNullCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setNullThresholds(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .nullCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # LENGTH CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def length_check(
        df,
        id_app: int,
        run: int = 1,
        length_values: dict | None = None,
        default_threshold: float = 0.0,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.lengthcheck
            .SDKLengthCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(float(default_threshold))
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setLengthValuesByColumn(
            DataBuck._py_map_to_java(jvm, length_values, str)
        )
        options.setThresholdsByColumn(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .lengthCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # MAX LENGTH CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def max_length_check(
        df,
        id_app: int,
        run: int = 1,
        max_length_values: dict | None = None,
        default_threshold: float = 0.0,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.maxlengthcheck
            .SDKMaxLengthCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(float(default_threshold))
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setMaxLengthValuesByColumn(
            DataBuck._py_map_to_java(jvm, max_length_values, str)
        )
        options.setThresholdsByColumn(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .maxLengthCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # DEFAULT CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def default_check(
        df,
        id_app: int,
        run: int = 1,
        default_values: dict | None = None,
        default_threshold: float = 0.0,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.defaultcheck
            .SDKDefaultCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(float(default_threshold))
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setDefaultValuesByColumn(
            DataBuck._py_map_to_java(jvm, default_values, str)
        )
        options.setThresholdsByColumn(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .defaultCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # RECORD COUNT ANOMALY
    # ------------------------------------------------------------------

    @staticmethod
    def record_count_anomaly(
        df,
        id_app: int,
        run: int = 1,
        columns: list[str] | None = None,
        default_threshold: float = 3.0,
        column_thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.recordcountanomaly
            .SDKRecordCountAnomalyOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(
            float(default_threshold)
        )
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setColumns(
            DataBuck._py_list_to_java(jvm, columns, str)
        )
        options.setColumnThresholds(
            DataBuck._py_map_to_java(jvm, column_thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .recordCountAnomaly(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # DATA DRIFT CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def data_drift_check(
        df,
        id_app: int,
        run: int = 1,
        columns: list[str] | None = None,
        default_threshold: float = 3.0,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.datadrift
            .SDKDataDriftCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(
            float(default_threshold)
        )
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setColumns(
            DataBuck._py_list_to_java(jvm, columns, str)
        )
        options.setThresholdsByColumn(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .dataDriftCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # NUMERICAL STATISTICS / DISTRIBUTION CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def numerical_statistics_check(
        df,
        id_app: int,
        run: int = 1,
        columns: list[str] | None = None,
        default_threshold: float = 3.0,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.numericalstatistics
            .SDKNumericalStatisticsCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(
            float(default_threshold)
        )
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setColumns(
            DataBuck._py_list_to_java(jvm, columns, str)
        )
        options.setThresholdsByColumn(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .numericalStatisticsCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # VALUE ANOMALY CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def value_anomaly_check(
        df,
        id_app: int,
        run: int = 1,
        columns: list[str] | None = None,
        default_threshold: float = 3.0,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.recordanomaly
            .SDKRecordAnomalyCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(
            float(default_threshold)
        )
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setColumns(
            DataBuck._py_list_to_java(jvm, columns, str)
        )
        options.setThresholdsByColumn(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .recordAnomalyCheck(df._jdf, options)
        )

    @staticmethod
    def record_anomaly_check(
        df,
        id_app: int,
        run: int = 1,
        columns: list[str] | None = None,
        default_threshold: float = 3.0,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        return DataBuck.value_anomaly_check(
            df=df,
            id_app=id_app,
            run=run,
            columns=columns,
            default_threshold=default_threshold,
            thresholds=thresholds,
            save_dashboard_summary=save_dashboard_summary,
        )

    # ------------------------------------------------------------------
    # PATTERN CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def pattern_check(
        df,
        id_app: int,
        run: int = 1,
        patterns: dict | None = None,
        default_threshold: float = 0.0,
        thresholds: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.patterncheck
            .SDKPatternCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(float(default_threshold))
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setPatternsByColumn(
            DataBuck._py_map_to_java(jvm, patterns, str)
        )
        options.setThresholdsByColumn(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .patternCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # DATE RULE CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def date_rule_check(
        df,
        id_app: int,
        run: int = 1,
        date_formats: dict | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.daterule
            .SDKDateRuleCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setDateFormatsByColumn(
            DataBuck._py_map_to_java(jvm, date_formats, str)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .dateRuleCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # DUPLICATE CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def duplicate_check(
        df,
        id_app: int,
        run: int = 1,
        primary_key_columns: list[str] | None = None,
        selected_columns: list[str] | None = None,
        default_threshold: float = 0.0,
        thresholds: dict | None = None,
        identity_check_enabled: bool | None = None,
        selected_fields_check_enabled: bool | None = None,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.duplicatecheck
            .SDKDuplicateCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(float(default_threshold))
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setPrimaryKeyColumns(
            DataBuck._py_list_to_java(jvm, primary_key_columns, str)
        )
        options.setSelectedColumns(
            DataBuck._py_list_to_java(jvm, selected_columns, str)
        )
        options.setThresholdsByColumn(
            DataBuck._py_map_to_java(jvm, thresholds, float)
        )

        if identity_check_enabled is not None:
            options.setIdentityCheckEnabled(
                bool(identity_check_enabled)
            )

        if selected_fields_check_enabled is not None:
            options.setSelectedFieldsCheckEnabled(
                bool(selected_fields_check_enabled)
            )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .duplicateCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # DEFAULT PATTERN CHECK
    # ------------------------------------------------------------------

    @staticmethod
    def default_pattern_check(
        df,
        id_app: int,
        run: int = 1,
        patterns: dict | None = None,
        default_threshold: float = 10.0,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.defaultpatterncheck
            .SDKDefaultPatternCheckOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setDefaultThreshold(float(default_threshold))
        options.setSaveDashboardSummary(save_dashboard_summary)
        options.setPatternsByColumn(
            DataBuck._py_map_to_java(jvm, patterns, str)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .defaultPatternCheck(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # DATA TRUST METRICS
    # ------------------------------------------------------------------

    @staticmethod
    def data_trust_metrics(
        df,
        id_app: int,
        run: int = 1,
        save_dashboard_summary: bool = True,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.datatrustmetrics
            .SDKDataTrustMetricsOptions()
        )

        options.setIdApp(id_app)
        options.setRun(run)
        options.setSaveDashboardSummary(save_dashboard_summary)

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .dataTrustMetrics(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # VALIDATE
    # ------------------------------------------------------------------

    @staticmethod
    def validate(
        df,
        id_app: int,
        null_check_enabled: bool | None = None,
        length_check_enabled: bool | None = None,
        max_length_check_enabled: bool | None = None,
        default_check_enabled: bool | None = None,
        record_count_anomaly_enabled: bool | None = None,
        data_drift_check_enabled: bool | None = None,
        numerical_statistics_check_enabled: bool | None = None,
        value_anomaly_check_enabled: bool | None = None,
        record_anomaly_check_enabled: bool | None = None,
        data_trust_metrics_enabled: bool | None = None,
        pattern_check_enabled: bool | None = None,
        date_rule_check_enabled: bool | None = None,
        duplicate_check_enabled: bool | None = None,
        default_pattern_check_enabled: bool | None = None,
        save_dashboard_summary: bool = True,
        null_thresholds: dict | None = None,
        length_values: dict | None = None,
        length_thresholds: dict | None = None,
        length_default_threshold: float = 0.0,
        max_length_values: dict | None = None,
        max_length_thresholds: dict | None = None,
        max_length_default_threshold: float = 0.0,
        default_check_values: dict | None = None,
        default_check_thresholds: dict | None = None,
        default_check_default_threshold: float = 0.0,
        record_count_thresholds: dict | None = None,
        record_count_default_threshold: float = 3.0,
        data_drift_columns: list[str] | None = None,
        data_drift_thresholds: dict | None = None,
        data_drift_default_threshold: float = 3.0,
        numerical_statistics_columns: list[str] | None = None,
        numerical_statistics_thresholds: dict | None = None,
        numerical_statistics_default_threshold: float = 3.0,
        value_anomaly_columns: list[str] | None = None,
        value_anomaly_thresholds: dict | None = None,
        value_anomaly_default_threshold: float = 3.0,
        record_anomaly_columns: list[str] | None = None,
        record_anomaly_thresholds: dict | None = None,
        record_anomaly_default_threshold: float | None = None,
        pattern_values: dict | None = None,
        pattern_thresholds: dict | None = None,
        pattern_default_threshold: float = 0.0,
        date_rule_formats: dict | None = None,
        duplicate_primary_key_columns: list[str] | None = None,
        duplicate_selected_columns: list[str] | None = None,
        duplicate_thresholds: dict | None = None,
        duplicate_default_threshold: float = 0.0,
        default_pattern_values: dict | None = None,
        default_pattern_default_threshold: float = 10.0,
    ):
        jvm = df.sparkSession.sparkContext._jvm

        options = (
            jvm.com.databuck.sdk.model.validate
            .SDKValidateOptions()
        )

        options.setIdApp(id_app)
        options.setSaveDashboardSummary(
            save_dashboard_summary
        )
        options.setRecordCountDefaultThreshold(
            float(record_count_default_threshold)
        )
        options.setDataDriftCheckDefaultThreshold(
            float(data_drift_default_threshold)
        )
        options.setNumericalStatisticsCheckDefaultThreshold(
            float(numerical_statistics_default_threshold)
        )
        options.setRecordAnomalyCheckDefaultThreshold(
            float(
                record_anomaly_default_threshold
                if record_anomaly_default_threshold is not None
                else value_anomaly_default_threshold
            )
        )
        options.setLengthDefaultThreshold(
            float(length_default_threshold)
        )
        options.setMaxLengthDefaultThreshold(
            float(max_length_default_threshold)
        )
        options.setDefaultCheckDefaultThreshold(
            float(default_check_default_threshold)
        )
        options.setPatternDefaultThreshold(
            float(pattern_default_threshold)
        )
        options.setDuplicateCheckDefaultThreshold(
            float(duplicate_default_threshold)
        )
        options.setDefaultPatternCheckDefaultThreshold(
            float(default_pattern_default_threshold)
        )

        if null_check_enabled is not None:
            options.setNullCheckEnabled(
                bool(null_check_enabled)
            )

        if length_check_enabled is not None:
            options.setLengthCheckEnabled(
                bool(length_check_enabled)
            )

        if max_length_check_enabled is not None:
            options.setMaxLengthCheckEnabled(
                bool(max_length_check_enabled)
            )

        if default_check_enabled is not None:
            options.setDefaultCheckEnabled(
                bool(default_check_enabled)
            )

        if record_count_anomaly_enabled is not None:
            options.setRecordCountAnomalyEnabled(
                bool(record_count_anomaly_enabled)
            )

        if data_drift_check_enabled is not None:
            options.setDataDriftCheckEnabled(
                bool(data_drift_check_enabled)
            )

        if numerical_statistics_check_enabled is not None:
            options.setNumericalStatisticsCheckEnabled(
                bool(numerical_statistics_check_enabled)
            )

        resolved_record_anomaly_enabled = (
            record_anomaly_check_enabled
            if record_anomaly_check_enabled is not None
            else value_anomaly_check_enabled
        )
        if resolved_record_anomaly_enabled is not None:
            options.setRecordAnomalyCheckEnabled(
                bool(resolved_record_anomaly_enabled)
            )

        if data_trust_metrics_enabled is not None:
            options.setDataTrustMetricsEnabled(
                bool(data_trust_metrics_enabled)
            )

        if pattern_check_enabled is not None:
            options.setPatternCheckEnabled(
                bool(pattern_check_enabled)
            )

        if date_rule_check_enabled is not None:
            options.setDateRuleCheckEnabled(
                bool(date_rule_check_enabled)
            )

        if duplicate_check_enabled is not None:
            options.setDuplicateCheckEnabled(
                bool(duplicate_check_enabled)
            )

        if default_pattern_check_enabled is not None:
            options.setDefaultPatternCheckEnabled(
                bool(default_pattern_check_enabled)
            )

        j_null_thresholds = DataBuck._py_map_to_java(
            jvm,
            null_thresholds,
            float,
        )
        options.setNullThresholds(
            j_null_thresholds
        )

        options.setLengthValues(
            DataBuck._py_map_to_java(jvm, length_values, str)
        )
        options.setLengthThresholds(
            DataBuck._py_map_to_java(jvm, length_thresholds, float)
        )
        options.setMaxLengthValues(
            DataBuck._py_map_to_java(jvm, max_length_values, str)
        )
        options.setMaxLengthThresholds(
            DataBuck._py_map_to_java(jvm, max_length_thresholds, float)
        )
        options.setDefaultCheckValues(
            DataBuck._py_map_to_java(jvm, default_check_values, str)
        )
        options.setDefaultCheckThresholds(
            DataBuck._py_map_to_java(jvm, default_check_thresholds, float)
        )

        j_record_thresholds = DataBuck._py_map_to_java(
            jvm,
            record_count_thresholds,
            float,
        )
        options.setRecordCountThresholds(
            j_record_thresholds
        )
        options.setDataDriftColumns(
            DataBuck._py_list_to_java(
                jvm,
                data_drift_columns,
                str,
            )
        )
        options.setDataDriftThresholds(
            DataBuck._py_map_to_java(jvm, data_drift_thresholds, float)
        )
        options.setNumericalStatisticsColumns(
            DataBuck._py_list_to_java(
                jvm,
                numerical_statistics_columns,
                str,
            )
        )
        options.setNumericalStatisticsThresholds(
            DataBuck._py_map_to_java(
                jvm,
                numerical_statistics_thresholds,
                float,
            )
        )
        options.setRecordAnomalyColumns(
            DataBuck._py_list_to_java(
                jvm,
                (
                    record_anomaly_columns
                    if record_anomaly_columns is not None
                    else value_anomaly_columns
                ),
                str,
            )
        )
        options.setRecordAnomalyThresholds(
            DataBuck._py_map_to_java(
                jvm,
                (
                    record_anomaly_thresholds
                    if record_anomaly_thresholds is not None
                    else value_anomaly_thresholds
                ),
                float,
            )
        )
        options.setPatternValues(
            DataBuck._py_map_to_java(jvm, pattern_values, str)
        )
        options.setPatternThresholds(
            DataBuck._py_map_to_java(jvm, pattern_thresholds, float)
        )
        options.setDateRuleFormats(
            DataBuck._py_map_to_java(jvm, date_rule_formats, str)
        )
        options.setDuplicatePrimaryKeyColumns(
            DataBuck._py_list_to_java(
                jvm,
                duplicate_primary_key_columns,
                str,
            )
        )
        options.setDuplicateSelectedColumns(
            DataBuck._py_list_to_java(
                jvm,
                duplicate_selected_columns,
                str,
            )
        )
        options.setDuplicateThresholds(
            DataBuck._py_map_to_java(jvm, duplicate_thresholds, float)
        )
        options.setDefaultPatternValues(
            DataBuck._py_map_to_java(jvm, default_pattern_values, str)
        )

        return (
            jvm.com.databuck.sdk.DataBuckSDK
            .validate(df._jdf, options)
        )

    # ------------------------------------------------------------------
    # VALIDATE RESULT HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def validate_check_results(result):
        checks = [
            {
                "check": "Record Count Anomaly",
                "enabled": "isRecordCountAnomalyEnabled",
                "executed": "isRecordCountAnomalyExecuted",
                "result": "getRecordCountAnomalyResult",
                "failed": "getTotalOutlierCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Data Drift Check",
                "enabled": "isDataDriftCheckEnabled",
                "executed": "isDataDriftCheckExecuted",
                "result": "getDataDriftCheckResult",
                "failed": "getTotalChangedCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Numerical Statistics Check",
                "enabled": "isNumericalStatisticsCheckEnabled",
                "executed": "isNumericalStatisticsCheckExecuted",
                "result": "getNumericalStatisticsCheckResult",
                "failed": "getFailedColumnCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Value Anomaly Check",
                "enabled": "isRecordAnomalyCheckEnabled",
                "executed": "isRecordAnomalyCheckExecuted",
                "result": "getRecordAnomalyCheckResult",
                "failed": "getTotalOutliers",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Data Trust Metrics",
                "enabled": "isDataTrustMetricsEnabled",
                "executed": "isDataTrustMetricsExecuted",
                "result": "getDataTrustMetricsResult",
                "failed": "getFailedDefinitionCount",
                "tested": "getTotalDefinitionCount",
            },
            {
                "check": "Null Check",
                "enabled": "isNullCheckEnabled",
                "executed": "isNullCheckExecuted",
                "result": "getNullCheckResult",
                "failed": "getTotalFailedCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Length Check",
                "enabled": "isLengthCheckEnabled",
                "executed": "isLengthCheckExecuted",
                "result": "getLengthCheckResult",
                "failed": "getTotalFailedCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Max Length Check",
                "enabled": "isMaxLengthCheckEnabled",
                "executed": "isMaxLengthCheckExecuted",
                "result": "getMaxLengthCheckResult",
                "failed": "getTotalFailedCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Default Check",
                "enabled": "isDefaultCheckEnabled",
                "executed": "isDefaultCheckExecuted",
                "result": "getDefaultCheckResult",
                "failed": "getTotalFailedCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Pattern Check",
                "enabled": "isPatternCheckEnabled",
                "executed": "isPatternCheckExecuted",
                "result": "getPatternCheckResult",
                "failed": "getTotalFailedCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Date Rule Check",
                "enabled": "isDateRuleCheckEnabled",
                "executed": "isDateRuleCheckExecuted",
                "result": "getDateRuleCheckResult",
                "failed": "getTotalFailedCount",
                "tested": "getTestedDateFieldCount",
            },
            {
                "check": "Duplicate Check",
                "enabled": "isDuplicateCheckEnabled",
                "executed": "isDuplicateCheckExecuted",
                "result": "getDuplicateCheckResult",
                "failed": "getTotalDuplicateCount",
                "tested": "getTestedColumnCount",
            },
            {
                "check": "Default Pattern Check",
                "enabled": "isDefaultPatternCheckEnabled",
                "executed": "isDefaultPatternCheckExecuted",
                "result": "getDefaultPatternCheckResult",
                "failed": "getTotalFailedCount",
                "tested": "getTestedColumnCount",
            },
        ]

        rows = []
        for check in checks:
            child = DataBuck._call_java(result, check["result"])
            rows.append({
                "check": check["check"],
                "enabled": DataBuck._call_java(result, check["enabled"]),
                "executed": DataBuck._call_java(result, check["executed"]),
                "status": DataBuck._call_java(child, "getOverallStatus"),
                "dqi": DataBuck._call_java(child, "getDqi"),
                "total_records": DataBuck._call_java(
                    child,
                    "getTotalRecordCount",
                ),
                "tested": DataBuck._call_java(child, check["tested"]),
                "failed": DataBuck._call_java(child, check["failed"]),
                "summary_count": DataBuck._java_size(
                    DataBuck._call_java(child, "getSummaries")
                ),
            })

        return rows

    @staticmethod
    def _call_java(value, method_name):
        if value is None:
            return None

        method = getattr(value, method_name, None)
        if method is None:
            return None

        return method()

    @staticmethod
    def _java_size(value):
        if value is None:
            return None

        return value.size()

    # ------------------------------------------------------------------
    # AGENTIC / CONTEXT-AWARE DISCOVERY
    # ------------------------------------------------------------------

    @staticmethod
    def discover(df, context: dict):

        if df is None:
            raise ValueError("df cannot be None")

        if not isinstance(context, dict):
            raise TypeError(
                "context must be a dictionary"
            )

        from .agentic_rules import discover_rules

        return discover_rules(df, context)

    @staticmethod
    def discover_and_export(df, path, *, context: dict | None = None,
                            table_name: str | None = None):
        """Export profiling rules, adding BuckGPT rules when context is given."""
        if df is None:
            raise ValueError("df cannot be None")
        if context is not None and not isinstance(context, dict):
            raise TypeError("context must be a dictionary or None")

        from .lake_rules import _export_format, _validated_table_name
        if _export_format(path) == "yaml":
            _validated_table_name(table_name)

        rules = DataBuck.discover_rules(df)
        print("\nAuto-discovered DataBuck rules ({}):".format(len(rules)))
        for index, rule in enumerate(rules, start=1):
            print("Rule {}: {}".format(index, rule))
        context_rules = DataBuck.discover(df, context) if context is not None else None
        output_path = rules.to_lake(
            path, df=df, context=context, context_rules=context_rules,
            table_name=table_name
        )
        return output_path

    # ------------------------------------------------------------------
    # COUNT
    # ------------------------------------------------------------------

    @staticmethod
    def count(df) -> int:
        return df.count()
