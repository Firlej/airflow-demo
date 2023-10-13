# airflow-demo

```bash
export AIRFLOW_HOME=~/airflow-demo/airflow

AIRFLOW_VERSION=2.7.1

# Extract the version of Python you have installed. If you're currently using Python 3.11 you may want to set this manually as noted above, Python 3.11 is not yet supported.
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.7.1 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.8.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

```
terraform init
terraform plan --out 1.plan
terraform apply "1.plan"
```


```bash
airflow db clean -y --clean-before-timestamp '2024-01-01 00:00:00+01:00'
```

```bash
airflow variables import ~/airflow-demo/airflow/variables.json
```

[https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html)
```bash
sudo docker run -itd -e POSTGRES_USER=myuser -e POSTGRES_PASSWORD=mypassword -p 5432:5432 --name my_postgres_container postgres
export AIRFLOW_HOME=~/airflow-demo/airflow
airflow standalone
```