## Проект, описывающий ETL процесс для составления оборотной ведомости по счетам бухгалтерской кредитной организации (101 форма)

### Подготовка к запуску
- Клонируйте репорзиторий:
```sh
git clone https://github.com/tsukerin/project_assignment.git
cd project_assignment
```
- Перейдите в папку с проектом, создайте виртуальное окружение, активируйте его и установите зависимости:
```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
- Запустите `docker-compose.yml` для поднятия AirFlow:
```sh
docker compose up
```
