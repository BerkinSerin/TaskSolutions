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
#usage: python3 solutions.py sqlite transactions.db task1(or all)
```
