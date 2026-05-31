# Kfg-analizator (Termux Edition)

Автоматический сборщик и валидатор прокси-конфигов для Android/Termux.

## Установка в Termux

```bash
pkg update && pkg install python -y
pip install -r requirements.txt
```

## Запуск

```bash
# Полный цикл (сбор + экспорт + валидация)
python main.py

# Только сбор
python main.py --collect-only

# Только валидация уже собранных
python main.py --validate-only
```

## Настройка источников

Отредактируй `sources.yaml` и добавь свои источники:
- `github` — сканирование репозиториев
- `telegram` — парсинг каналов t.me/s/
- `raw_url` — прямые ссылки

## Особенности Termux-версии

- Убраны GitHub Actions, скрипты генерации README и прочий репозиторийный хлам
- Автоограничение конкурентности на ARM-устройствах
- Fallback с lxml на html.parser
- Построчное разбиение больших файлов (не режет URI)
- Корректная дедупликация без слома query-параметров
