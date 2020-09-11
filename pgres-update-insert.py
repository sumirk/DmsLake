import psycopg2
import random
import time
import sys
db_host = sys.argv[1]

con = psycopg2.connect(database='postgres', user='master',password='master123', host=db_host)
cur = con.cursor()
con.set_session(autocommit=True)
query_create_table = "create table orders2 (order_id SERIAL PRIMARY KEY, product_id integer, total_price integer, customer_id integer, created_at TIMESTAMP)"
try:
    cur.execute(query_create_table)
except Exception as e:
    print(e)

query_insert = "INSERT INTO orders2(product_id, total_price, customer_id, created_at) VALUES(%s,%s,%s,%s)"

while True:
    product_id = random.randint(1,1000)
    total_price = random.randint(100,200)
    customer_id = random.randint(1,1000)
    created_at = time.strftime('%Y-%m-%d %H:%M:%S')
    update_create = random.choice([0,1])
    sql_select_query = "select * from orders2 where order_id = %s"
    if update_create == 0:
        order_id = random.randint(1,10)
        try:
          cur.execute(sql_select_query, (order_id, ))
          record = cur.fetchone()
          sql_update_query = "Update orders2 set total_price = %s, created_at= %s where order_id = %s"
          cur.execute(sql_update_query, (total_price, created_at,record[0]))
          time.sleep(2)
        except Exception as e:
          time.sleep(2)
          print(e)
          continue
    else:
        try:
          cur.execute(query_insert,(product_id,total_price,customer_id,created_at))
          time.sleep(2)
        except Exception as e:
          time.sleep(2)
          print(e)
          continue