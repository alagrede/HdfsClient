
# CLI example

## Usage

```
hdfsclient add <local_path> <hdfs_path>
hdfsclient read <hdfs_path>
hdfsclient delete <hdfs_path>
hdfsclient mkdir <hdfs_path>
hdfsclient copyfromlocal <local_path> <hdfs_path>
hdfsclient copytolocal <hdfs_path> <local_path>
hdfsclient modificationtime <hdfs_path>
hdfsclient getblocklocations <hdfs_path>
hdfsclient gethostnames

```

# Kerberos Java example
## Configuration

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
    
nputStream input = classLoader.getResourceAsStream("hadoop.properties");
prop.load(input);

client = new HadoopClient();
	
client.setHadoopCluster(prop.getProperty("hadoop.cluster"));
client.setNamenodes(prop.getProperty("hadoop.namenodes"));
client.setHttpAaddress(prop.getProperty("hadoop.httpAddress"));
client.setRpcAddress(prop.getProperty("hadoop.rpcAddress"));
client.setHadoopProxy(prop.getProperty("hadoop.failoverProxy"));
client.setJaasConfUrl(prop.getProperty("hadoop.jaasConfUrl"));
client.setKrbConfUrl(prop.getProperty("hadoop.krb5Url"));

FileSystem fs = client.hadoopConnectionWithKeytab("xxx.keytab", "xxx@xxx.CORP");
	
// or with user/password
FileSystem fs = client.hadoopConnectionWithUserPassword("xxx@xxx.CORP", "XXX");

```