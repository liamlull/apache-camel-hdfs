# apache-camel-hdfs

## Configuration
Setting up a Hadoop connection requires some configuration and configuration files. 

When using the factory you automatically get a HA configuration that uses Kerberos. In order to get this working you have to add some files to the filesystem of your server and point the configuration to those files.

The kerberos.configFile needs to contain the krb5.conf (https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html)

This is an example of the krb5.conf file

```
[libdefaults]
 default_realm = EXAMPLE.COM

[realms]
 EXAMPLE.COM = {
  kdc = 1.22.0.2
  kpasswd_server = 1.22.0.2
  admin_server = 1.22.0.2
  kpasswd_protocol = SET_CHANGE
 }

```

### Keytab
In order to connect and authenticate to the Hadoop cluster you need a valide KeyTab file. This keytab also needs to be placed on the filesystem.
 
For more information about token renewal and Hadoop see: 
https://stackoverflow.com/questions/34616676/should-i-call-ugi-checktgtandreloginfromkeytab-before-every-action-on-hadoop?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa

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
