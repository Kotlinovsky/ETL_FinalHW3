from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch


# Реализовал пакетное выполнение запроса для более высокой производительности.
class PostgresqlSaver:
    def __init__(self, pg_conn_id="postgres_default"):
        self.pg_conn_id = pg_conn_id

    def _get_pg_connection(self):
        hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        return hook.get_conn()

    def save_sessions(self, sessions_rows, sessions_pages_rows, sessions_actions_rows):
        with self._get_pg_connection() as conn:
            with conn.cursor() as cursor:
                if sessions_rows:
                    execute_batch(cursor,
                                  """
                                  INSERT INTO sessions (session_id, user_id, start_time, end_time, device)
                                  VALUES (%s, %s, %s, %s, %s) ON CONFLICT (session_id) DO NOTHING
                                  """,
                                  sessions_rows,
                                  )

                if sessions_pages_rows:
                    execute_batch(cursor,
                                  """
                                  INSERT INTO session_pages (session_id, page_url)
                                  VALUES (%s, %s) ON CONFLICT DO NOTHING
                                  """,
                                  sessions_pages_rows,
                                  )

                if sessions_actions_rows:
                    execute_batch(cursor,
                                  """
                                  INSERT INTO session_actions (session_id, action_name)
                                  VALUES (%s, %s) ON CONFLICT DO NOTHING
                                  """,
                                  sessions_actions_rows,
                                  )
                conn.commit()

    def save_event_logs(self, event_logs_rows):
        if not event_logs_rows:
            return

        with self._get_pg_connection() as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor,
                              """
                              INSERT INTO event_logs (event_id, event_time, event_type, details)
                              VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
                              """, event_logs_rows)
                conn.commit()

    def save_support_tickets(self, support_tickets_rows, support_messages_rows):
        with self._get_pg_connection() as conn:
            with conn.cursor() as cursor:
                if support_tickets_rows:
                    execute_batch(cursor,
                                  """
                                  INSERT INTO support_tickets (ticket_id, user_id, status, issue_type, created_at, updated_at)
                                  VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (ticket_id) DO NOTHING
                                  """,
                                  support_tickets_rows)
                if support_messages_rows:
                    execute_batch(cursor, """
                                          INSERT INTO support_messages (ticket_id, sender_role, message_text, message_time)
                                          VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
                                          """,
                                  support_messages_rows)
                conn.commit()

    def save_recommendations(self, recommendations_rows):
        if not recommendations_rows:
            return

        with self._get_pg_connection() as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor,
                              """
                              INSERT INTO user_recommendations (user_id, recommended_product, last_updated)
                              VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                              """, recommendations_rows)
                conn.commit()

    def save_moderation_queue(self, moderation_queue_rows, moderation_flags_rows):
        with self._get_pg_connection() as conn:
            with conn.cursor() as cursor:
                if moderation_queue_rows:
                    execute_batch(cursor,
                                  """
                                  INSERT INTO moderation_queue (review_id, user_id, product_id, review_text, rating, moderation_status, submitted_at)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (review_id) DO NOTHING
                                  """, moderation_queue_rows)
                if moderation_flags_rows:
                    execute_batch(cursor, """
                    INSERT INTO moderation_flags (review_id, flag_name) VALUES (%s, %s) ON CONFLICT DO NOTHING
                    """, moderation_flags_rows)
                conn.commit()
