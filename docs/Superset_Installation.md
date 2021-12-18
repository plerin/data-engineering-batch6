# Superset Installation Instructions (in Ubuntu)


## Python Related Upgrade

```
sudo apt update
sudo apt install -y build-essential autoconf libtool python3-dev libssl-dev libsasl2-dev libldap2-dev 
sudo apt install python3-pip
sudo pip3 install image cryptography apache-superset psycopg2-binary
```

## Superset Login Account Creation

Create a login account for Superset (don't forget to note the credentials somewhere)
```
sudo apt install -y virtualenv
virtualenv -p python3 superset
source superset/bin/activate

export FLASK_APP=superset
flask fab create-admin
```

## Postgres Installation

### Install Postgres Server

Create a PostgreSQL user/password and a DB. We will use an external Postgres instance for this
```
sudo apt-get install -y postgresql postgresql-contrib
```

### Next create a user and a database to be used by Airflow to store its data

```
$ sudo su postgres
$ psql
psql (10.12 (Ubuntu 10.12-0ubuntu0.18.04.1))
Type "help" for help.

postgres=# CREATE USER superset PASSWORD 'superset';
CREATE ROLE
postgres=# CREATE DATABASE superset;
CREATE DATABASE
postgres=# \q
$ exit
```

### Restart Postgres (as ubuntu user)

```
sudo service postgresql restart
```

## Create a superset config

```
PYTHONPATH=/home/ubuntu/superset-conf/
```

```
$ vi /home/ubuntu/superset-conf/superset_config.py

SUPERSET_WEBSERVER_PORT = 8088
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset@127.0.0.1/superset'
```

```
$ echo "export PYTHONPATH=$HOME/conf" >> ~/.bashrc
$ . ~/.bashrc
```

## Initialize Superset

```
PYTHONPATH=/home/ubuntu/superset-conf superset db upgrade
superset init
```

## Run Superset as a background service

Create startup scripts
```
$ sudo vi /etc/systemd/system/superset-server.service
```

```
[Unit]
Description=Visualization platform by Airbnb

[Service]
Type=simple
ExecStart=/home/ubuntu/superset/bin/superset run -h 0.0.0.0 -p 8088 --with-threads --reload

[Install]
WantedBy=multi-user.target
```

Run the server
```
$ sudo systemctl daemon-reload
$ sudo systemctl start superset-server
$ sudo systemctl status superset-server
...
```

## See Also
* https://superset.incubator.apache.org/installation.html

