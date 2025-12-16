import os
import re
import json
import random
import hashlib
import datetime as dt
import logging
from typing import List, Dict, Any, Optional
from airflow.models import Variable
import boto3
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

MODEL_ID = "cohere.embed-v4:0"
REGION_NAME = 'us-east-1'
VECTOR_BUCKET = Variable.get("VECTOR_BUCKET")
VECTOR_INDEX = Variable.get("VECTOR_INDEX")
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

def get_s3vectors_client(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY):
    return boto3.client(
        "s3vectors",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1",
    )


def get_bedrock_client(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY):
    return boto3.client(
        "bedrock-runtime",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1",
    )


def get_text_embedding(text_to_embed: str) -> Optional[List[float]]:
    bedrock = get_bedrock_client(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

    if not text_to_embed or not isinstance(text_to_embed, str):
        logger.info("❌ Error: Input text must be a non-empty string.")
        return None

    body = {
        "texts": [text_to_embed],
        "input_type": "search_document"
    }

    try:
        resp = bedrock.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body),
        )

        out = json.loads(resp["body"].read())
        embeddings = out["embeddings"]["float"]

        if not embeddings:
            logger.info("⚠️ The model did not return any vector.")
            return None

        vector = embeddings[0]
        logger.info(f"✅ Embedding successfully generated. Dimension: {len(vector)}")
        return vector

    except ClientError as e:
        logger.info(f"❌ Bedrock Runtime Error: {e.response['Error']['Message']}")
        return None
    except Exception as e:
        logger.info(f"❌ An unexpected error occurred: {e}")
        return None


def format_top_solutions(vector_search_results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    top_results = vector_search_results
    output_dict = {}

    for index, result in enumerate(top_results):
        rank_key = f"top{index + 1}"
        solution_text = result['metadata'].get("solution", "N/A")
        distance_value = result.get("distance", 0.0)

        output_dict[rank_key] = {
            "solution": solution_text,
            "distance": distance_value
        }
    return output_dict


def on_task_failure(context):
    ti = context["ti"]
    dag_id = ti.dag_id
    task_id = ti.task_id

    logical_date = context["logical_date"].isoformat()
    exc = context.get("exception")
    
    if exc:
        import traceback
        traceback_str = "".join(traceback.format_exception(
            type(exc), exc, exc.__traceback__
        ))

        error_message = traceback_str.strip().split('\n')[-1]
        line_info = "Not found in traceback"
        match = re.search(r'File "(.*/dags/.*?\.py)", line (\d+)', traceback_str)
        if match:
            file_name = os.path.basename(match.group(1))
            line_number = match.group(2)
            line_info = f"{file_name}, line {line_number}"

        fingerprint_id = hashlib.sha256(
            f'{dag_id}{task_id}{error_message}'.encode('utf-8')
        ).hexdigest()
        status = 'unresolved'
        solution = ""

        summary_dict = {
            "fingerprint_id": fingerprint_id,
            "dag_id": dag_id,
            "task_name": task_id,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat() + "Z",
            "task_error": f"{error_message} (at {line_info})",
            "logical_date": logical_date,
            "log_url": ti.log_url,
            "status": status,
            "solution": solution
        }
        
        logger.info("--- FAILED TASK SUMMARY (from callback) ---")
        logger.info(json.dumps(summary_dict, indent=2))
        logger.info("-------------------------------------------")
        ti.xcom_push(key="error_summary", value=summary_dict)


def task_zero_division():
    res = 10 / random.choice([1, 0, 5])
    return res


def task_row_count_check():
    count = random.choice(["1", 4, "25", "30"])
    msg = "The number of records is " + count
    return msg


def task_dependency_version():
    installed = "2.25.0"
    required_major = 2
    required_minor = 32
    parts = installed.split(".")
    if int(parts[0]) < required_major or (
        int(parts[0]) == required_major and int(parts[1]) < required_minor
    ):
        raise RuntimeError(f"Dependency 'requests' too old: {installed} < 2.32.0")
    return "deps ok"


def task_access_error():
    return "access ok"


def task_db_conn():
    return "db ok"


def log_error_summary(**ctx):
    failed_ti = next(
        (ti for ti in ctx["dag_run"].get_task_instances() if ti.state == "failed"),
        None
    )

    if not failed_ti:
        logger.info("Could not find a failed task instance in the context.")
        return None

    summary_dict = failed_ti.xcom_pull(key="error_summary", task_ids=failed_ti.task_id)

    if summary_dict:
        logger.info("--- FAILED TASK SUMMARY (from log_error_summary task) ---")
        logger.info(json.dumps(summary_dict, indent=2))
        logger.info("--------------------------------------------------------")
        return summary_dict
    else:
        logger.info(f"Could not retrieve error summary from XComs for task: {failed_ti.task_id}")
        return None


def hint_to_solve_callable(**ctx):
    ti = ctx["ti"]
    summary_dict = ti.xcom_pull(task_ids="log_error_summary", key="return_value")

    if not summary_dict or "task_error" not in summary_dict:
        logger.info("❌ Could not retrieve a valid error summary from the previous task.")
        return

    error_message = summary_dict["task_error"]
    logger.info(f"INFO: Generating embedding for error: {error_message}")

    embedding = get_text_embedding(error_message)

    if not embedding:
        logger.info("❌ Failed to generate embedding. Cannot query for solutions.")
        return

    try:
        logger.info("⏳: Querying vector database for similar errors...")
        s3vectors = get_s3vectors_client(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
        response = s3vectors.query_vectors(
            vectorBucketName=VECTOR_BUCKET,
            indexName=VECTOR_INDEX,
            queryVector={"float32": embedding},
            topK=3,
            returnDistance=True,
            returnMetadata=True
        )

        retrieval_results = response.get("vectors", [])
        if not retrieval_results:
            logger.info("⚠️ Knowledge Base returned no matching solutions.")
            return

        final_output = format_top_solutions(retrieval_results)

        readable_output = {
            "suggestion": final_output.get("top1", {}).get("solution", "N/A"),
            "similarity_score": round(
                float(final_output.get("top1", {}).get("distance", 0.0)),
                4
            )
        }

        logger.info("💡 How to Solve this error: 👇🏻")
        logger.info(json.dumps(readable_output, indent=2))
        logger.info("\n")

    except Exception as e:
        logger.info(f"❌ Error querying vectors: {e}")


default_args = {
    "owner": "alex",
    "retries": 0,
    "on_failure_callback": on_task_failure,
}

with DAG(
    dag_id="smart_airflow_dag",
    start_date=dt.datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vector-logs", "observability"]
) as dag:

    init_task = EmptyOperator(task_id="init")

    zero_division_task = PythonOperator(task_id="zero_division", python_callable=task_zero_division)
    row_count_check_task = PythonOperator(task_id="row_count_check", python_callable=task_row_count_check)
    dependency_version_task = PythonOperator(task_id="dependency_version", python_callable=task_dependency_version)
    s3_access_task = PythonOperator(task_id="s3_access", python_callable=task_access_error)
    db_conn_task = PythonOperator(task_id="db_conn", python_callable=task_db_conn)

    log_summary_task = PythonOperator(
        task_id="log_error_summary",
        python_callable=log_error_summary,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    hint_to_solve_task = PythonOperator(
        task_id="hint_to_solve",
        python_callable=hint_to_solve_callable
    )

    end_task = EmptyOperator(task_id="end", trigger_rule="one_success")

    internal_tasks = [
        zero_division_task,
        row_count_check_task,
        s3_access_task,
        db_conn_task,
        dependency_version_task
    ]

    init_task >> internal_tasks[0]
    for i in range(len(internal_tasks) - 1):
        internal_tasks[i] >> internal_tasks[i+1]
    internal_tasks[-1] >> end_task

    internal_tasks >> log_summary_task >> hint_to_solve_task
