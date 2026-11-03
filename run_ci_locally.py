import subprocess
import sys


def run_command(command, description):
    print(f"\n{'='*50}")
    print(f"RUNNING: {description}")
    print(f"{'='*50}")

    # Explicitly set check=False to manually handle exit codes below
    result = subprocess.run(command, check=False, shell=True)

    if result.returncode != 0:
        print(f"\n[FAILED] {description} failed with exit code {result.returncode}.")
        sys.exit(1)
    else:
        print(f"\n[PASSED] {description} completed successfully.")


def main():
    print("Starting Local CI Pipeline (Simulating GitHub Actions)...")

    # Step 1: Linting with Ruff
    run_command("python -m ruff check dags/ spark_jobs/ lambda/ scripts/ tests/", "Ruff Linting")

    # Step 2: Unit Testing with PyTest (Inside Docker)
    # Note: MSYS_NO_PATHCONV=1 is not needed here because Python's subprocess
    # on Windows uses cmd.exe, which does not perform Unix-to-Windows path translation.
    run_command(
        "docker compose exec airflow-scheduler pytest /opt/airflow/tests/ -v",
        "PyTest Suite",
    )

    print("\n" + "="*50)
    print("ALL CI CHECKS PASSED! READY TO PUSH TO GITHUB.")
    print("="*50)


if __name__ == "__main__":
    main()