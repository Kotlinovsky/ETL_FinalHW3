-- Создаем таблицу для сессий пользователей.
CREATE TABLE IF NOT EXISTS sessions(
    session_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    device VARCHAR(50) NOT NULL
);

-- Создаем таблицу для статистики посещенных в ходе сессии страниц.
CREATE TABLE IF NOT EXISTS session_pages(
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) NOT NULL,
    page_url TEXT NOT NULL
);

-- Создаем таблицу для действий, выполненных в ходе сессии.
CREATE TABLE IF NOT EXISTS session_actions(
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) NOT NULL,
    action_name VARCHAR(100) NOT NULL
);

-- Создаем таблицу для событий, произошедших в системе.
CREATE TABLE IF NOT EXISTS event_logs(
    event_id VARCHAR(100),
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    details TEXT,
    PRIMARY KEY (event_id, event_time)
);

-- Создаем таблицу для хранения обращений в поддержку.
CREATE TABLE IF NOT EXISTS support_tickets(
    ticket_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- Создаем таблицу для хранения сообщений обращения в техподдержку.
CREATE TABLE IF NOT EXISTS support_messages(
    id SERIAL PRIMARY KEY,
    ticket_id VARCHAR(100) NOT NULL,
    sender_role VARCHAR(50) NOT NULL,
    message_text TEXT NOT NULL,
    message_time TIMESTAMP NOT NULL
);

-- Создаем таблицу для хранения рекомендаций пользователю.
CREATE TABLE IF NOT EXISTS user_recommendations(
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    recommended_product VARCHAR(100) NOT NULL,
    last_updated TIMESTAMP NOT NULL
);

-- Создаем таблицу для хранения очереди модерации отзывов.
CREATE TABLE IF NOT EXISTS moderation_queue(
    review_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    moderation_status VARCHAR(50),
    submitted_at TIMESTAMP NOT NULL,
    review_text TEXT,
    rating INT
);

-- Создаем таблицу для хранения флажков заявки в очереди модерации.
CREATE TABLE IF NOT EXISTS moderation_flags(
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(100) NOT NULL,
    flag_name VARCHAR(100) NOT NULL
);

-- Создадим уникальные индексы, чтобы избегать дублирования данных.
CREATE UNIQUE INDEX IF NOT EXISTS idx_session_pages_unique ON session_pages(session_id, page_url);
CREATE UNIQUE INDEX IF NOT EXISTS idx_session_actions_unique ON session_actions(session_id, action_name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_support_messages_unique ON support_messages(ticket_id, sender_role, message_text, message_time);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_recommendations_unique ON user_recommendations(user_id, recommended_product);
CREATE UNIQUE INDEX IF NOT EXISTS idx_moderation_flags_unique ON moderation_flags(review_id, flag_name);
