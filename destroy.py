import subprocess
import shutil
import os

print("Stopping containers...")
subprocess.run(["docker", "compose", "down"])

print("Cleaning up data...")
if os.path.exists("localstack-data"):
    shutil.rmtree("localstack-data")

print("Environment cleaned up!")