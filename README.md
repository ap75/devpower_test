# Сервіс даних про населення

Цей сервіс збирає дані про населення країн і зберігає їх у базі даних Postgres, використовуючи Docker Compose.

## Як запустити

1. Клонуйте репозиторій:
```bash
git clone https://github.com/ap75/devpower_test.git .
```

2. Підготуйте файл .env з налаштуваннями, або скористуйтеся наданим прикладом:
   - For Linux/macOS:
   ```bash
   cp .env.sample .env
   ```
   - For Windows (PowerShell):
   ```powershell
   Copy-Item .env.sample .env
   ```
3. Запустіть базу даних і отримайте дані:
```bash
docker-compose up get_data
```

4. Виведіть агреговані дані про населення за регіонами:
```bash
docker-compose up print_data
```

## Змінні середовища
- `DATABASE_URL`: Строка підключення до бази даних Postgres.
- `DATA_SOURCE`: Джерело даних, `wikipedia` (за замовчуванням), або `statisticstimes`.

## Особливості
- Асинхронне отримання даних.
- Перемикання джерел даних через змінну середовища.
