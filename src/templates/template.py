import os
from datetime import datetime
import numpy as np
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from scipy.stats import linregress

@dag(
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["dynamic_dag__model_train"],
)
def {{ dag_name }}():
    vendor_name = "{{ vendor_name }}"
    bucket_name = Variable.get("bucket_name")

    start_task = DummyOperator(task_id="start")

    @task
    def load_and_validate():
        df = pd.read_parquet(
            f"s3://{bucket_name}/work_zone/data_science_project/datasets/{vendor_name}/train.parquet",
            storage_options={"anon": False}
        )
        return df

    validate_data = GreatExpectationsOperator(
        task_id="data_quality",
        data_context_root_dir="./dags/gx",
        data_asset_name="train_{{ vendor_name }}",
        dataframe_to_validate="{% raw %}{{ ti.xcom_pull(task_ids='load_and_validate') }}{% endraw %}",
        execution_engine="PandasExecutionEngine",
        expectation_suite_name="de-c2w4a1-expectation-suite",
        return_json_dict=True,
        fail_task_on_validation_failure=True,
    )

    @task
    def train_and_evaluate(bucket_name: str, vendor_name: str):
        datasets_path = f"s3://{bucket_name}/work_zone/data_science_project/datasets"
        train = pd.read_parquet(f"{datasets_path}/{vendor_name}/train.parquet", storage_options={"anon": False})
        test = pd.read_parquet(f"{datasets_path}/{vendor_name}/test.parquet", storage_options={"anon": False})

        X_train = train[["distance"]].to_numpy()[:, 0]
        X_test = test[["distance"]].to_numpy()[:, 0]
        y_train = train[["trip_duration"]].to_numpy()[:, 0]
        y_test = test[["trip_duration"]].to_numpy()[:, 0]

        model = linregress(X_train, y_train)
        y_pred_test = model.slope * X_test + model.intercept
        performance = np.sqrt(np.average((y_pred_test - y_test) ** 2))
        print("--- performance RMSE ---")
        print(f"test: {performance:.2f}")
        return performance

    def _is_deployable(ti):
        performance = ti.xcom_pull(task_ids="train_and_evaluate")
        if performance < 500:
            print(f"is deployable: {performance}")
            return "deploy"
        else:
            print("is not deployable")
            return "notify"

    is_deployable_task = BranchPythonOperator(
        task_id="is_deployable",
        python_callable=_is_deployable,
        do_xcom_push=False,
    )

    @task
    def deploy():
        print("Deploying...")

    @task
    def notify(message):
        print(f"{message}. Notify to mail: admin@{{ vendor_name }}.com")

    end_task = DummyOperator(task_id="end", trigger_rule="none_failed_or_skipped")

    (
        start_task
        >> load_and_validate()
        >> validate_data
        >> train_and_evaluate(bucket_name, vendor_name)
        >> is_deployable_task
        >> [deploy(), notify("Not deployed")]
        >> end_task
    )

dag_{{ dag_name }} = {{ dag_name }}()
