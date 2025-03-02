# Сервіс даних про населення

Цей сервіс збирає дані про населення країн і зберігає їх у базі даних Postgres, використовуючи Docker Compose.

## Як запустити

1. Клонуйте репозиторій:
```bash
git clone https://github.com/ap75/devpower_test.git .
```

2. Запустіть базу даних і отримайте дані:
```bash
docker-compose up get_data
```

3. Виведіть агреговані дані про населення за регіонами:
```bash
docker-compose up print_data
```

## Змінні середовища
- `DATABASE_URL`: Строка підключення до бази даних Postgres.
- `DATA_SOURCE`: Джерело даних, `wikipedia` (за замовчуванням), або `statisticstimes`.

## Цікаві функції
- Асинхронне отримання даних.
- Перемикання джерел даних через змінну середовища.
