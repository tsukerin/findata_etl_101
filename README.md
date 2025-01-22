## Проект, описывающий ETL процесс для составления оборотной ведомости по счетам бухгалтерской кредитной организации (101 форма)
**Для составления оборотной ведомости были использованы следующие источники:**
- Таблицы `md_account_d`, `md_currency_d` и `md_exchange_rate_d` содержат информацию о счетах, валютах и курсах валют соответственно. В данных таблицах есть поля `data_actual_date` и `data_actual_end_date`, по которым можно определить, какие именно записи актуальны на нужную дату. Идентификаторы записей имеют окончание «`_rk`» (например, `account_rk` – идентификатор счета).
- Таблица `md_ledger_account_s` – это справочник балансовых счетов. Он регулируется Центральным банком. По нему можно определить, к какой главе и к каким разделам относятся счета первого (первые 3 цифры номера счета) и второго (первые 5 цифр номера счета) порядка.
- Таблица `ft_posting_f` – это таблица проводок (операций) в рабочем дне (поле `oper_date`), которая состоит из двух частей: счет дебета и счет кредита, изменяющих баланс на сумму проводки.
Для загрузки этих таблиц в **детальный слой** был использован DAG `insert_data`, для расчета витрин используется DAG `create_dm`, а для выгрузки результирующей витрины - `export_data`.
---
### Подготовка к запуску
- Клонируйте репорзиторий:
```sh
git clone https://github.com/tsukerin/project_assignment.git \
cd project_assignment
```
- Перейдите в папку с проектом, создайте виртуальное окружение, активируйте его и установите зависимости:
Win:
```sh
python -m venv .venv \
source .venv/Scripts/activate \
pip install -r requirements.txt
```
Mac:
```sh
python3 -m venv .venv \
source .venv/bin/activate \
pip3 install -r requirements.txt
```
- Запустите `docker-compose.yml` для поднятия AirFlow:
```sh
docker compose up
```
- Перейдите по адресу `http://localhost:8080/`, чтобы продолжить работу с AirFlow 
- Чтобы AirFlow увидел наши таблицы-источники, загрузите папку dags в docker контейнер:
```sh
docker cp ./dags project_assignment-airflow-worker-1
```
- Для расчета витрины запустите последовательно DAG'и `insert_data`, `create_dm` и `export_data`.
