"""Interactive, read-only Spark SQL console for the canonical UNSW-NB15 Parquet data."""
import argparse
import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession


DISPLAY_LIMIT = 50
READ_ONLY_PREFIXES = ("INSERT", "UPDATE", "DELETE", "MERGE", "DROP", "ALTER", "TRUNCATE", "CREATE", "REPLACE", "LOAD")


def split_statements(text):
    """Split semicolon-terminated SQL without splitting quoted literals."""
    complete, start, quote, index = [], 0, None, 0
    while index < len(text):
        char = text[index]
        if quote:
            if char == quote:
                if quote in ("'", '"') and index + 1 < len(text) and text[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in ("'", '"', "`"):
            quote = char
        elif char == ";":
            statement = text[start:index].strip()
            if statement:
                complete.append(statement)
            start = index + 1
        index += 1
    return complete, text[start:]


def render(value, width=80):
    text = "NULL" if value is None else str(value)
    return text if len(text) <= width else text[: width - 1] + "…"


def print_table(frame, display_limit):
    rows = frame.limit(display_limit + 1).collect()
    shown, more = rows[:display_limit], len(rows) > display_limit
    columns = frame.columns
    if not columns:
        print("Statement completed (no tabular result).")
        return
    values = [[render(row[index]) for index in range(len(columns))] for row in shown]
    widths = [len(column) for column in columns]
    for row in values:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    separator = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    print(separator)
    print("|" + "|".join(f" {column:<{widths[index]}} " for index, column in enumerate(columns)) + "|")
    print(separator)
    for row in values:
        print("|" + "|".join(f" {value:<{widths[index]}} " for index, value in enumerate(row)) + "|")
    print(separator)
    if more:
        print(f"Rows shown: {len(shown)} (first {display_limit}; total matching rows were not counted).")
    else:
        print(f"Rows shown: {len(shown)} (all results fit within {display_limit}; no separate total count was run).")


def execute(spark, statement, display_limit):
    upper = statement.lstrip().upper()
    if upper.startswith(READ_ONLY_PREFIXES):
        print("SQL ERROR: this classroom console is read-only; data-changing statements are disabled.", flush=True)
        return
    started = time.perf_counter()
    try:
        print("Executing SQL...", flush=True)
        result = spark.sql(statement)
        print_table(result, display_limit)
        print(f"Execution time: {time.perf_counter() - started:.3f} seconds", flush=True)
    except KeyboardInterrupt:
        spark.sparkContext.cancelAllJobs()
        print("Query cancelled. The Spark SQL console remains available.", flush=True)
    except Exception as error:  # Spark exceptions vary between analysis and execution stages.
        print(f"SQL ERROR: {str(error).splitlines()[0]}", flush=True)


def help_text():
    print("Commands: HELP, SCHEMA, TABLES, CLEAR, EXIT")
    print("Terminate SQL with a semicolon. Example:")
    print("  SELECT attack_cat_canonical, COUNT(*) AS flows")
    print("  FROM network_flows WHERE label = 1 GROUP BY attack_cat_canonical ORDER BY flows DESC;")
    print("network_flows is a read-only temp view over canonical Snappy Parquet.", flush=True)


def read_console_line(prompt):
    """Avoid input() buffering differences across Docker Compose/PowerShell TTYs."""
    sys.stdout.write(prompt)
    sys.stdout.flush()
    line = sys.stdin.readline()
    if line == "":
        raise EOFError
    return line.rstrip("\r\n")


def command(spark, text, display_limit):
    upper = text.strip().upper()
    if upper == "HELP":
        help_text(); return False
    if upper == "SCHEMA":
        execute(spark, "DESCRIBE network_flows", display_limit); return False
    if upper == "TABLES":
        execute(spark, "SHOW TABLES", display_limit); return False
    if upper == "CLEAR":
        print("\033[2J\033[H", end=""); return False
    if upper in ("EXIT", "QUIT"):
        return True
    execute(spark, text, display_limit)
    return False


def run_file(spark, path, display_limit):
    statements, leftover = split_statements(Path(path).read_text(encoding="utf-8"))
    if leftover.strip():
        statements.append(leftover.strip())
    for statement in statements:
        print(f"spark-sql> {statement}")
        if command(spark, statement, display_limit):
            break


def interactive(spark, display_limit):
    help_text()
    pending = ""
    while True:
        try:
            line = read_console_line("spark-sql> " if not pending else "       -> ")
        except (EOFError, KeyboardInterrupt):
            print("\nClosing Spark SQL console.", flush=True)
            return
        if not pending and line.strip().upper() in ("HELP", "SCHEMA", "TABLES", "CLEAR", "EXIT", "QUIT"):
            if command(spark, line, display_limit):
                return
            continue
        statements, pending = split_statements(pending + line + "\n")
        if not pending.strip():
            pending = ""
        for statement in statements:
            if command(spark, statement, display_limit):
                return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", default="/workspace/data/processed/unsw_nb15")
    parser.add_argument("--master", default="spark://spark-master:7077")
    parser.add_argument("--display-limit", type=int, default=DISPLAY_LIMIT)
    parser.add_argument("--file", help="Run semicolon-terminated statements then exit (integration-test mode).")
    args = parser.parse_args()
    if args.display_limit < 1:
        raise SystemExit("--display-limit must be at least 1")
    if not Path(args.parquet).is_dir():
        raise SystemExit(f"Canonical Parquet is unavailable: {args.parquet}")
    # Run Python directly (rather than through spark-submit) so this REPL owns
    # the Docker TTY.  spark-submit's PythonRunner gives the child Python process
    # a pipe for stdin, which prevents interactive input on Windows Compose.
    spark = (SparkSession.builder.appName("unsw-spark-sql-console").master(args.master)
             .config("spark.driver.memory", "1g")
             .config("spark.executor.memory", "2g")
             .config("spark.executor.cores", "2")
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.read.parquet(args.parquet).createOrReplaceTempView("network_flows")
        print(f"Connected to Spark. Registered network_flows from {args.parquet}.", flush=True)
        if args.file:
            run_file(spark, args.file, args.display_limit)
        else:
            interactive(spark, args.display_limit)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
