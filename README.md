
# HDFS client
This library allow to connect to the Hadoop datalab cluster without any system installation (except Java).
* A maven dependency can be import in your Java application for use hdfs
* A command line interface can be used on your machine _hdfs dfs_ 
 
## HDFS in Java application

```xml
<dependency>
	<groupId>com.tony.hdfs</groupId>
	<artifactId>HdfsClient</artifactId>
	<version>1.0</version>
</dependency>
```

### Configuration

Define your hadoop.properties in your project

```
hadoop.cluster=clustername
hadoop.failoverProxy=org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
hadoop.namenodes=nn1,nn2
hadoop.rpcAddress=[DNS_NAMENODE1]:[PORT_RPC],[DNS_NAMENODE2]:[PORT_RPC]
hadoop.httpAddress=[DNS_NAMENODE1]:[PORT_HTTP],[DNS_NAMENODE2]:[PORT_HTTP]
hadoop.krb5Url=hadoop/krb5.conf
hadoop.jaasConfUrl=hadoop/jaas.conf
```

Note: Example __krb5.conf__ and __jaas.conf__ are embedded in jar and must be overriden

## Usage

```java
Properties prop = new Properties();

ClassLoader classLoader = getClass().getClassLoader();
 
InputStream input = new FileInputStream("./hadoop.properties");
prop.load(input);

client = new HadoopClient();

client.setHadoopCluster(prop.getProperty("hadoop.cluster"));
client.setNamenodes(prop.getProperty("hadoop.namenodes"));
client.setHttpAaddress(prop.getProperty("hadoop.httpAddress"));
client.setRpcAddress(prop.getProperty("hadoop.rpcAddress"));
client.setHadoopProxy(prop.getProperty("hadoop.failoverProxy"));

# For use internal krb5 and jaas files
URL jaas = classLoader.getResource(prop.getProperty("hadoop.jaasConfUrl"));
URL krb5 = classLoader.getResource(prop.getProperty("hadoop.krb5Url"));

# For use external krb5 and jaas files
#URL jaas = new File(prop.getProperty("hadoop.jaasConfUrl")).toURL();
#URL krb5 = new File(prop.getProperty("hadoop.krb5Url")).toURL();

client.setJaasConfUrl(jaas);
client.setKrbConfUrl(krb5);

String keytabPath = new File("xxx.keytab").getPath();

FileSystem fs = client.hadoopConnectionWithKeytab(keytabPath, "xxx@xxx.CORP");

// or with user/password
FileSystem fs = client.hadoopConnectionWithUserPassword("xxx@xxx.CORP", "xxx");
```


## Command line Interface
The project provide a fat jar with the original Hadoop client _hdfs dfs_ interface usable on your local machine.

### Configuration
Copy next to the __hadoop-client-cli.jar__ :
* your __hadoop.properties__
* the __krb5.conf__
* the __jaas.conf__
* your keytab __xxx.keytab__ (if keytab authentication)

Example _hadoop.properties_ for CLI with keytab

```
hadoop.cluster=clustername
hadoop.failoverProxy=org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
hadoop.namenodes=nn1,nn2
hadoop.rpcAddress=[DNS_NAMENODE1]:[PORT_RPC],[DNS_NAMENODE2]:[PORT_RPC]
hadoop.httpAddress=[DNS_NAMENODE1]:[PORT_HTTP],[DNS_NAMENODE2]:[PORT_HTTP]
hadoop.krb5Url=krb5.conf
hadoop.jaasConfUrl=jaas.conf

#Keytab auth
hadoop.keytab=xxx.keytab
hadoop.principal=xxx@xxx.CORP
#hadoop.password=XXX
```

Example _hadoop.properties_ for CLI with user/pass authentication

```
#User/pass auth
#hadoop.keytab=xxx.keytab
hadoop.principal=xx@xxx.CORP
hadoop.password=XXX

```

_jaas.conf_

```
HdfsHaSample {
  com.sun.security.auth.module.Krb5LoginModule required client=TRUE debug=true;
};
```

### Usage

```bash
java -jar hadoop-client-cli.jar -ls /
```

#### Deploy CLI
Deploy jar and files in __%userprofile%/hdfs__ and add directory to Windows PATH.

Add __hdfs.bat__

```
@ECHO OFF

setlocal
cd /d %~dp0
java -jar %userprofile%/hdfs/hadoop-client-cli.jar %*
```

Usage in cmd:

```
hdfs -ls /
```