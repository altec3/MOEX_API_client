# API клиент Московской биржи (MOEX)
### Отбор ценных бумаг по заданным критериям. 
*Стек:  
Python:3.11,  
aiohttp:3.8.5,  
Pandas:2.1.0,  
Pydantic:2.3.0*
####
### Описание

---
 
Используя API MOEX, клиент производит выгрузку ценных бумаг по заданным критериям.
На данный момент реализована работа с облигациями.  

На первом этапе загружается список доступных (на указанных режимах торговли) облигаций.
Далее проводится фильтрация данного списка по указанным критериям.  
Итоговый список бумаг, удовлетворяющих критериям, выводится в консоль.
####
#### TODO:
* Реализовать отбор акций
### Работа с клиентом

---
1. В папке [config](config) разместить файл config.py с настройками клиента (см. файл **[config_example.py](config/config_example.py)**)
2. Установить зависимости:
```python
pip install poetry
poetry install
poetry shell
```
3. Запустить скрипт:

```python
python main.py
```