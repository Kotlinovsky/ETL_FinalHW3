from airflow import DAG
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from scripts.mongo_extractor import MongoExtractor
from scripts.postgresql_saver import PostgresqlSaver
from scripts.replication_planner import ReplicationPlanner
from scripts.watermark_store import WatermarkStore

CHUNK_SIZE = 5
PIPELINES_CONFIG = {
    "sessions": {
        "collection_name": "UserSessions",
        "watermark_field": "start_time",
    },
    "event_logs": {
        "collection_name": "EventLogs",
        "watermark_field": "timestamp",
    },
    "support_tickets": {
        "collection_name": "SupportTickets",
        "watermark_field": "created_at",
    },
    "recommendations": {
        "collection_name": "UserRecommendations",
        "watermark_field": "last_updated",
    },
    "moderation_queue": {
        "collection_name": "ModerationQueue",
        "watermark_field": "submitted_at",
    }
}


# Задача - планировщик ETL-процесса для определенного пайплайна.
def plan_pipeline_task(pipeline_name: str, collection_name, watermark_field: str):
    store = WatermarkStore()
    extractor = MongoExtractor()
    planner = ReplicationPlanner(extractor, store, CHUNK_SIZE)
    manifest = planner.plan(pipeline_name, collection_name, watermark_field)
    return [
        {
            "pipeline_name": pipeline_name,
            "collection_name": PIPELINES_CONFIG[pipeline_name]["collection_name"],
            "watermark_field": PIPELINES_CONFIG[pipeline_name]["watermark_field"],
            "chunk_idx": i,
        }
        for i in range(manifest["chunks"])
    ]


# Задача - почанковое извлечение данных из MongoDB.
def extract_chunk_task(pipeline_name: str, collection_name, watermark_field: str, chunk_idx: int):
    store = WatermarkStore()
    extractor = MongoExtractor()
    manifest = store.get_manifest(pipeline_name)
    rows = extractor.read_collection_chunk(collection_name, watermark_field, manifest["watermark"], chunk_idx,
                                           CHUNK_SIZE)

    # Записываем извлеченные чанки.
    store.set_raw_chunk(pipeline_name, chunk_idx, rows)
    return {"pipeline_name": pipeline_name, "chunk_idx": chunk_idx}


def transform_chunk_task(pipeline_name: str, chunk_idx: int):
    store = WatermarkStore()
    rows = store.get_raw_chunk(pipeline_name, chunk_idx)
    payload = []

    # Производим маппинг данных из MongoDB в PostgreSQL.
    if pipeline_name == "sessions":
        sessions_rows = []
        sessions_pages_rows = []
        sessions_actions_rows = []

        # Производим маппинг сессий пользователей.
        for row in rows:
            sessions_rows.append([
                row["session_id"],
                row["user_id"],
                row["start_time"],
                row["end_time"],
                row["device"],
            ])

            # Произведем маппинг посещенных пользователем страниц.
            # Произведем маппинг действий, произведенных в ходе сессии.
            for page in row.get("pages_visited", []):
                sessions_pages_rows.append([row["session_id"], page])
            for action in row.get("actions", []):
                sessions_actions_rows.append([row["session_id"], action])

            payload = {
                "sessions_rows": sessions_rows,
                "sessions_pages_rows": sessions_pages_rows,
                "sessions_actions_rows": sessions_actions_rows,
            }
    elif pipeline_name == "event_logs":
        event_logs_rows = []

        for row in rows:
            event_logs_rows.append([
                row["event_id"],
                row["timestamp"],
                row["event_type"],
                row["details"],
            ])

        payload = {"event_logs_rows": event_logs_rows}
    elif pipeline_name == "support_tickets":
        support_tickets_rows = []
        support_messages_rows = []

        for row in rows:
            support_tickets_rows.append([
                row["ticket_id"],
                row["user_id"],
                row["status"],
                row["issue_type"],
                row["created_at"],
                row["updated_at"],
            ])

            for msg in row.get("messages", []):
                support_messages_rows.append([
                    row["ticket_id"],
                    msg["sender"],
                    msg["message"],
                    msg["timestamp"],
                ])

        payload = {
            "support_tickets_rows": support_tickets_rows,
            "support_messages_rows": support_messages_rows,
        }
    elif pipeline_name == "recommendations":
        recommendations_rows = []

        for row in rows:
            for product in row.get("recommended_products", []):
                recommendations_rows.append([
                    row["user_id"],
                    product,
                    row["last_updated"]
                ])

        payload = {"recommendations_rows": recommendations_rows}
    elif pipeline_name == "moderation_queue":
        moderation_queue_rows = []
        moderation_flags_rows = []

        for row in rows:
            moderation_queue_rows.append([
                row["review_id"],
                row["user_id"],
                row["product_id"],
                row["review_text"],
                row["rating"],
                row["moderation_status"],
                row["submitted_at"],
            ])

            for flag in row.get("flags", []):
                moderation_flags_rows.append([row["review_id"], flag])

        payload = {
            "moderation_queue_rows": moderation_queue_rows,
            "moderation_flags_rows": moderation_flags_rows,
        }

    # Сохраняем данные для следующей стадии - сохранения в PostgreSQL.
    store.set_transformed_chunk(pipeline_name, chunk_idx, payload)
    return {"pipeline_name": pipeline_name, "chunk_idx": chunk_idx}


def load_chunk_task(pipeline_name: str, chunk_idx: int):
    store = WatermarkStore()
    saver = PostgresqlSaver()
    payload = store.get_transformed_chunk(pipeline_name, chunk_idx)

    if pipeline_name == "sessions":
        saver.save_sessions(payload["sessions_rows"], payload["sessions_pages_rows"], payload["sessions_actions_rows"])
    elif pipeline_name == "event_logs":
        saver.save_event_logs(payload["event_logs_rows"])
    elif pipeline_name == "support_tickets":
        saver.save_support_tickets(payload["support_tickets_rows"], payload["support_messages_rows"])
    elif pipeline_name == "recommendations":
        saver.save_recommendations(payload["recommendations_rows"])
    elif pipeline_name == "moderation_queue":
        saver.save_moderation_queue(payload["moderation_queue_rows"], payload["moderation_flags_rows"])

    return {"pipeline_name": pipeline_name, "chunk_idx": chunk_idx}


def finalize_task(pipeline_name: str):
    store = WatermarkStore()
    manifest = store.get_manifest(pipeline_name)
    max_watermark = manifest.get("max_watermark", 0)
    store.set_watermark(pipeline_name, max_watermark)


with DAG(dag_id="mongo_to_postgres_replication", start_date=datetime(2026, 3, 8)) as dag:
    begin, end = EmptyOperator(task_id="begin"), EmptyOperator(task_id="end")
    extract_tasks, plan_tasks = {}, {}

    # Планируем репликацию коллекций, указанных в конфиге.
    for pipeline_name, config in PIPELINES_CONFIG.items():
        # Создаем задачу на планирование процесса репликации.
        plan_task = PythonOperator(
            task_id=pipeline_name,
            python_callable=plan_pipeline_task,
            op_kwargs={
                "pipeline_name": pipeline_name,
                "collection_name": config["collection_name"],
                "watermark_field": config["watermark_field"]
            }
        )

        # Создаем задачи на извлечение данных из MongoDB.
        extract_task = PythonOperator.partial(
            task_id=f"extract_{pipeline_name}",
            python_callable=extract_chunk_task,
        ).expand(op_kwargs=plan_task.output)

        # Создаем задачу на обработку полученных данных.
        transform_task = PythonOperator.partial(
            task_id=f"transform_{pipeline_name}",
            python_callable=transform_chunk_task,
        ).expand(op_kwargs=extract_task.output)

        # Создаем задачу на сохранение данных в Postgresql.
        load_task = PythonOperator.partial(
            task_id=f"load_{pipeline_name}",
            python_callable=load_chunk_task,
        ).expand(op_kwargs=transform_task.output)

        # И саму задачу на финализацию пайплайна.
        finalize_task_instance = PythonOperator(
            task_id=f"finalize_{pipeline_name}",
            python_callable=finalize_task,
            op_kwargs={
                "pipeline_name": pipeline_name,
            }
        )

        # Теперь построим сам процесс.
        begin >> plan_task >> extract_task >> transform_task >> load_task >> finalize_task_instance >> end
