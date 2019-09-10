# apache-camel-hdfs

## Testing / running locally
In order to test it locally you need to have access to a hadoop cluster that has kerberos enabled. 
One way to do this is by starting a secure hadoop cluster in a docker container.
https://github.com/Knappek/docker-hadoop-secure

```bash
docker-compose up -d
docker ps
docker exec -it <container-id> /bin/bash
```

Connect to the KDC container
(as root)
```bash
$ kadmin.local -q "addprinc -pw hadoop hdfs-user"
$ kadmin.local -q "xst -norandkey -k /hdfs-user.keytab hdfs-user@EXAMPLE.COM"
```

Then connect to the Hadoop container
```bash
docker exec -it <container-id> /bin/bash
```

```bash
kinit
# enter root password
hdfs dfs -mkdir /user/hdfs-user
hdfs dfs -chown hdfs-user:hdfs-user /user/hdfs-user
```
