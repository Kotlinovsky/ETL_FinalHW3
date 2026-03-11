db = db.getSiblingDB('etl_database');

// Константы для генерации объектов в MongoDB.
const users = ["user_1", "user_2", "user_3", "user_4", "user_5"];
const products = ["prod_101", "prod_102", "prod_103", "prod_104", "prod_105"];
const pages = ["/home", "/catalog", "/product/101", "/cart", "/profile"];
const actions = ["login", "view_product", "add_to_cart", "logout"];
const statuses = ["open", "in_progress", "closed"];
const issueTypes = ["payment", "delivery", "refund"];
const moderationStatuses = ["pending", "approved", "rejected"];
const flags = ["contains_images", "spam_suspected"];

// Функция для выборки случайного элемента из массива.
function randomItemFromArray(array) {
  return array[Math.floor(Math.random() * array.length)];
}

// Генерируем сессии пользователей.
for (let i = 1; i <= 20; i++) {
  db.UserSessions.insertOne({
    session_id: `sess_${i}`,
    user_id: randomItemFromArray(users),
    start_time: new Date(2025, 0, i, 9, 0, 0),
    end_time: new Date(2025, 0, i, 9, 30, 0),
    pages_visited: [randomItemFromArray(pages), randomItemFromArray(pages)],
    actions: [randomItemFromArray(actions), randomItemFromArray(actions)],
    device: randomItemFromArray(["mobile", "desktop"]),
  });
}

// Генерируем события в системе.
for (let i = 1; i <= 20; i++) {
  db.EventLogs.insertOne({
    event_id: `evt_${i}`,
    event_type: randomItemFromArray(["click", "view", "purchase"]),
    timestamp: new Date(2025, 0, i, 10, 0, 0),
    details: randomItemFromArray(pages),
  });
}

// Генерируем обращения в поддержку.
for (let i = 1; i <= 10; i++) {
  db.SupportTickets.insertOne({
    ticket_id: `ticket_${i}`,
    user_id: randomItemFromArray(users),
    status: randomItemFromArray(statuses),
    issue_type: randomItemFromArray(issueTypes),
    created_at: new Date(2025, 0, i, 12, 0, 0),
    updated_at: new Date(2025, 0, i, 14, 0, 0),
    messages: [
      {
        sender: "user",
        message: `Проблема ${i}`,
        timestamp: new Date(2025, 0, i, 12, 0, 0),
      },
      {
        sender: "support",
        message: `Ответ ${i}`,
        timestamp: new Date(2025, 0, i, 14, 0, 0),
      },
    ],
  });
}

// Генерируем рекомендации пользователям.
for (let i = 0; i < users.length; i++) {
  db.UserRecommendations.insertOne({
    user_id: users[i],
    last_updated: new Date(2025, 0, i + 1, 8, 0, 0),
    recommended_products: [randomItemFromArray(products), randomItemFromArray(products)],
  });
}

// Генерируем объекты в очереди модерации отзывов.
for (let i = 1; i <= 10; i++) {
  db.ModerationQueue.insertOne({
    review_id: `rev_${i}`,
    review_text: `Отзыв ${i}`,
    user_id: randomItemFromArray(users),
    product_id: randomItemFromArray(products),
    moderation_status: randomItemFromArray(moderationStatuses),
    submitted_at: new Date(2025, 0, i, 11, 0, 0),
    rating: Math.floor(Math.random() * 5) + 1,
    flags: [randomItemFromArray(flags)],
  });
}

// Создадим индексы для инкрементального ETL.
db.UserSessions.createIndex({ session_id: 1 }, { unique: true });
db.UserSessions.createIndex({ start_time: 1 });
db.EventLogs.createIndex({ event_id: 1 }, { unique: true });
db.EventLogs.createIndex({ timestamp: 1 });
db.SupportTickets.createIndex({ ticket_id: 1 }, { unique: true });
db.SupportTickets.createIndex({ created_at: 1 });
db.UserRecommendations.createIndex({ user_id: 1 }, { unique: true });
db.UserRecommendations.createIndex({ last_updated: 1 });
db.ModerationQueue.createIndex({ review_id: 1 }, { unique: true });
db.ModerationQueue.createIndex({ submitted_at: 1 });
