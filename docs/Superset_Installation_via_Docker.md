# Superset Installation via Docker

```
sudo apt-get update
sudo apt install docker.io
sudo curl -L "https://github.com/docker/compose/releases/download/1.23.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
sudo systemctl enable docker
sudo systemctl start docker

git clone https://github.com/apache/superset/
cd superset
sudo docker-compose up &
```

The installation is done when you see the following message and then it will take some time to initialize:
```
superset_init            | ######################################################################
superset_init            | 
superset_init            | 
superset_init            | Init Step 4/4 [Starting] -- Loading examples
superset_init            | 
superset_init            | 
superset_init            | ######################################################################
```

By default, this will create an account (admin/admin) and you need to delete this.

To add a new admin user:

```
sudo docker-compose exec superset bash -c 'export FLASK_APP=superset && flask fab create-admin'
```

Some other useful info:

```
$ sudo docker ps
CONTAINER ID        IMAGE                                COMMAND                  CREATED             STATUS                   PORTS                              NAMES
d26e8c43eca0        incubator-superset_superset          "/usr/bin/docker-ent…"   3 weeks ago         Up 12 days (healthy)     8080/tcp, 0.0.0.0:8088->8088/tcp   superset_app
0314e6743998        incubator-superset_superset-worker   "/usr/bin/docker-ent…"   3 weeks ago         Up 12 days (unhealthy)   8080/tcp                           superset_worker
1bb296fc9e24        postgres:10                          "docker-entrypoint.s…"   3 weeks ago         Up 12 days               127.0.0.1:5432->5432/tcp           superset_db
07f1befd7715        redis:3.2                            "docker-entrypoint.s…"   3 weeks ago         Up 12 days               127.0.0.1:6379->6379/tcp           superset_cache
$ sudo docker exec -ti superset_db bash
root@1bb296fc9e24:/# export
...
declare -x POSTGRES_DB="superset"
declare -x POSTGRES_PASSWORD="superset"
declare -x POSTGRES_USER="superset"
...
```

## Superset Details:

* http://SUPERSET_HOST:8088/
* Redshift connection string:
  * postgresql+psycopg2://(user):(password)@redshift_host:5439/prod

![](images/superset_database_configuration.png)
