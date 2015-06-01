# Intent Query Service

## ezCentos deployement settings

buildpacks/intents-query/intents-query-manifest.yml

```ini
Application:
  name: common_services
  datasets: []
  security_id: client
  auths: [U,USA,FOUO]

  Services:
    -
      type: Thrift
      language: Java
      service_name: intentQuery
      resources:
        cpu: small
        mem: small
        disk: small
      scaling:
          number_of_instances: 1
      artifact_info:
        bin: intents-query-service-2.1-SNAPSHOT-jar-with-dependencies.jar
        config: []
```

Clear Impala metastore

```shell
sudo rm -rf /var/run/impala/metastore_db/
```

Start the services

```shell
cd /vagrant
sudo scripts/startImpala.sh
sudo scripts/startRedis.sh
sudo scripts/startService.sh ins
sudo scripts/startService.sh intents-query
```

Deploy Impala scan client if not exist

```shell
su hdfs
hdfs dfs -mkdir /user/extds
hdfs dfs -chown impala:impala /user/extds
hdfs dfs -chmod 777 /user/extds
exit
hdfs dfs -put ezbakejars/intents-client-2.1-SNAPSHOT-jar-with-dependencies.jar /user/extds/scanclient.jar
```

Create data source / app schema

```shell
impala-shell
create DATA SOURCE intentqueryscanclient LOCATION "/user/extds/scanclient.jar" class "ezbake.query.basequeryableprocedure.client.Client" API_VERSION "V1";
create table if not exists yourApp_PERSON (firstName STRING, lastName STRING, country STRING, secUUID STRING) PRODUCED BY DATA SOURCE intentqueryscanclient;
exit;
```
