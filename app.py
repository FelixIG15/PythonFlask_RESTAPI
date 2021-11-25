import os
from flask import Flask, request
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt

app = Flask(__name__)

conn_format = "postgresql://{user}:{pass}@{host}:{port}/{db}"
conn_string = conn_format.format(**{
    "user": os.environ.get("POSTGRES_USER"),
    "pass": os.environ.get("POSTGRES_PASS"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
    "db": os.environ.get("POSTGRES_DB"),
})

conn = create_engine(conn_string)
print(conn_string)

@app.route("/")
def request_hello():
    return "Hello from root page"

@app.route("/request_get", methods=["GET"])
def request_from_get():
    return "Request GET"

@app.route("/request_post", methods=["POST"])
def request_from_post():
    return "Request POST"

@app.route("/request_all", methods=["GET", "POST", "PUT"])
def request_all():
    template = "Request All with method {}"
    return template.format(request.method)  


#endpoint 1
@app.route("/sales/list/<types>", methods=["GET"])
def request_type(types):
    query = f"SELECT DISTINCT {types} FROM supermarket_sales"
    dataframe = pd.read_sql(query, conn)
    result = dataframe[types]
    result = list(result)
    return {"data": result}

#endpoint2
@app.route("/sales/date/<date>", methods=["GET"])
def request_date(date):
    date = dt.datetime.strptime(date,'%Y-%m-%d').strftime('%d/%m/%Y')
    query = f"SELECT * FROM supermarket_sales WHERE date = '{date}'"
    dataframe = pd.read_sql(query, conn)
    result = dataframe.to_dict(orient="records")
    return {"data": result}

#endpoint3
@app.route("/sales/summary/<types>", methods=["GET"])
def request_total_sales(types):
    query = f"SELECT * FROM supermarket_sales;"
    dataframe = pd .read_sql(query, conn)
    df= pd.DataFrame(dataframe)
    df['total']= df['unit_price'] * df['quantity']
    result = df.groupby([types]).sum(['total']).reset_index()
    result = result[[types,'total']]
    result = result.to_dict(orient="records")
    return {"data": result}


if __name__ == "__main__":
    app.run()