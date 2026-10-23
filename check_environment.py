"""Pre-flight check for the BDA E-Commerce Analytics project.

Verifies Python, Java, required packages and a minimal local Spark session.
The dataset (*.xlsx) is reported but NOT required for the environment check.

Exit code 0 = ready. Exit code 1 = something must be fixed (message says what).
"""
import glob
import os
import platform
import subprocess
import sys

failures = []
warnings = []

print("=== BDA project environment check ===")
print("OS      :", platform.system(), platform.release())
print("Python  :", platform.python_version(), sys.executable)

# 1. Python version
if sys.version_info < (3, 10):
    failures.append(f"Python {platform.python_version()} is too old; need Python 3.10+.")
else:
    print("Python version: OK (3.10+)")

# 2. Java (needed by Spark)
try:
    proc = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=60)
    first_line = ((proc.stderr or "") + "\n" + (proc.stdout or "")).strip().splitlines()[0]
    print("Java    :", first_line)
    if "17" not in first_line and "11" not in first_line and "1.8" not in first_line:
        warnings.append(f"Untested Java version ({first_line}); Java 11 or 17 is recommended.")
except Exception as exc:  # noqa: BLE001 - report any launch problem plainly
    failures.append(f"Java not found or not runnable ({exc}); install Java 11 or 17.")

# 3. Required Python packages
for package in ("pyspark", "pandas", "matplotlib", "openpyxl", "notebook"):
    try:
        module = __import__(package)
        print(f"{package:10}: {getattr(module, '__version__', 'installed')}")
    except ImportError:
        failures.append(f"Missing package '{package}'; run: pip install -r requirements.txt")

# 4. Minimal Spark session (JVM-side only: no data leaves Java, works everywhere)
if not any(f.startswith("Missing package 'pyspark'") for f in failures):
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("BDA-env-check")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        print("Spark   : version", spark.version, "on", spark.sparkContext.master)
        assert spark.range(5).count() == 5
        print("Spark smoke test (spark.range + count): OK")
        spark.stop()
    except Exception as exc:  # noqa: BLE001 - surface the real Spark error
        failures.append(f"Spark session failed: {exc}")

# 5. Dataset (warning only: setup can succeed before the file is copied in)
project_dir = os.path.dirname(os.path.abspath(__file__))
xlsx_files = glob.glob(os.path.join(project_dir, "*.xlsx"))
if xlsx_files:
    print("Dataset : found", [os.path.basename(f) for f in xlsx_files])
else:
    warnings.append("No *.xlsx dataset in the project folder yet; copy 'Online Retail.xlsx' next to the notebook.")

# 6. Main notebook present (warning only)
notebook_path = os.path.join(project_dir, "BDA_Complete_Analytics.ipynb")
if os.path.exists(notebook_path):
    print("Notebook: found BDA_Complete_Analytics.ipynb")
else:
    warnings.append("BDA_Complete_Analytics.ipynb not found in the project folder.")

# 7. Spark MLlib import (warning only: K-Means section needs pyspark.ml).
# On Python 3.12+ PySpark 3.5.5 requires a `distutils` stub; the notebook
# installs one itself, so a failure here is informational, not blocking.
if not any(f.startswith("Missing package 'pyspark'") for f in failures):
    try:
        import pyspark.ml.clustering  # noqa: F401
        print("MLlib   : pyspark.ml imports OK (K-Means ready)")
    except Exception as exc:  # noqa: BLE001 - report plainly, never fail setup
        warnings.append(
            f"pyspark.ml import raised ({exc}); the notebook applies a tested "
            "local `distutils` stub for Python 3.12+ at runtime. If K-Means still "
            "fails, prefer Python 3.10/3.11 (see README)."
        )

print("--------------------------------------")
for w in warnings:
    print("WARNING:", w)
if failures:
    print("RESULT: NOT READY")
    for f in failures:
        print("FAIL:", f)
    sys.exit(1)
print("RESULT: READY - you can run the notebook.")
