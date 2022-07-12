# TaskSolutions
Task Solutions for FELD M


* **CSV** Folder includes the CSVs from task3 results, and the tables from sqlite db in CSV file format.
* **XML-Rates** Contains the xml file for the conversion rates.
* **requirements.txt** Contains the libraries to be installed. (run: *pip install -r requirements.txt*)
* **solutions.py** Contains the one and only script for 5 tasks. The usage is shown below.
* **transactions.db** Sqlite3 database which contains the two tables (Devices, Transactions).


Usage options in the console:
```python
- PostgreSQL solutions

python3 solutions.py postgresql postgresql://[username][password]@[host] [sqlite.db] [task#]
#usage: python3 solutions.py postgresql postgresql://postgres:postgres@localhost transactions.db task1(or all)
# NOTE: PORT HAS TO BE CHANGED MANUALLY IF 5232

- Sqlite solutions
python3 solutions.py sqlite [sqlite.db] [task#]
#usage: python3 solutions.py sqlite transactions.db task1(or all to run them at once)
```

# Task 1 Solution:
commands: 
* python3 solutions.py sqlite transactions.db task1
* python3 solutions.py postgresql postgresql://postgres:postgres@localhost transactions.db task1

**TASK1 Write a Python script to find out which visitor created the most revenue.**

**Query**: most_revenue_query = 'SELECT visitor_id, SUM(revenue) total_revenue FROM Transactions GROUP BY visitor_id ORDER BY 2 DESC LIMIT 1'

**PostgreSQL Changes:** Only "" around the table names.
![image](https://user-images.githubusercontent.com/45731847/178430789-8a74fe09-e43a-4946-95f2-677b9114a6b0.png)

# Task 2 Solution:
commands: 
* python3 solutions.py sqlite transactions.db task2
* python3 solutions.py postgresql postgresql://postgres:postgres@localhost transactions.db task2
**TASK2 Write a Python script to find out on which day most revenue for users who ordered via a mobile phone was created..**

**Query:** most_revenue_day_query = "SELECT strftime('%Y-%m-%d', t.datetime), sum(t.revenue) total_revenue FROM Transactions t JOIN Devices d ON t.device_type = d.id WHERE d.device_name = 'Mobile Phone' GROUP BY strftime('%Y-%m-%d', `datetime`) ORDER BY total_revenue DESC LIMIT 1"

**PostgreSQL Changes:** "" around the table names, TO_CHAR(t.datetime, 'YYYY-MM-DD') instead of strftime functions
![image](https://user-images.githubusercontent.com/45731847/178431147-ef8f387d-6c06-4684-8063-13d402952d25.png)

# Task 3 Solution:
commands: 
* python3 solutions.py sqlite transactions.db task3
* python3 solutions.py postgresql postgresql://postgres:postgres@localhost transactions.db task3
**TASK3 Write a Python script that combines the contents of Devices and Transactions and store it as a single flat file including the column names.**

**Query**: task3_query = 'SELECT t.*, d.device_name device_name FROM Transactions t JOIN Devices d on t.device_type = d.id'

PostgreSQL Changes: "" around the table names, file named _psql.csv
![image](https://user-images.githubusercontent.com/45731847/178432129-1ad53bba-d3cd-49b2-89ef-31cdc1c36095.png)

