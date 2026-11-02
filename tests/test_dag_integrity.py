import os
import sys

from airflow.models import DagBag

# Ensure the dags folder is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def test_dag_imports_and_loads():
    """Test that the DAG file has no import errors."""
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    assert "orders_lakehouse_etl" in dag_bag.dags, "DAG 'orders_lakehouse_etl' not found!"

def test_dag_has_expected_tasks():
    """Test that the DAG has the correct tasks and dependencies."""
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.dags["orders_lakehouse_etl"]
    
    expected_tasks = {"seed_dynamodb", "wait_for_lambda_s3", "spark_clean_validate", "quality_gate", "run_analytics_dashboard"}
    actual_tasks = {task.task_id for task in dag.tasks}
    
    assert expected_tasks == actual_tasks, f"Task mismatch. Expected: {expected_tasks}, Got: {actual_tasks}"