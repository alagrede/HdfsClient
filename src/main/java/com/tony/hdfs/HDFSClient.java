package com.tony.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;

public class HDFSClient {

	private Configuration getConf() throws IOException, URISyntaxException, NoSuchAlgorithmException, LoginException {
		HDFSClientKerberos client = new HDFSClientKerberos();
		
        final Properties prop = new Properties();

        InputStream input = new FileInputStream("./hadoop.properties");
        prop.load(input);

		client = new HDFSClientKerberos();
		
		client.setHadoopCluster(prop.getProperty("hadoop.cluster"));
		client.setNamenodes(prop.getProperty("hadoop.namenodes"));
		client.setHttpAaddress(prop.getProperty("hadoop.httpAddress"));
		client.setRpcAddress(prop.getProperty("hadoop.rpcAddress"));
		client.setHadoopProxy(prop.getProperty("hadoop.failoverProxy"));
		
		URL jaas = new File(prop.getProperty("hadoop.jaasConfUrl")).toURL();
		URL krb5 = new File(prop.getProperty("hadoop.krb5Url")).toURL();
		client.setJaasConfUrl(jaas);
		client.setKrbConfUrl(krb5);
		
		Configuration conf = client.prepareConfiguration();
		
		String keytabProp = prop.getProperty("hadoop.keytab");

		if (keytabProp != null) {
			String keytabPath = getResource(keytabProp);
			client.loginWithKeytab(keytabPath, prop.getProperty("hadoop.principal"));
			
		} else { 
			client.loginWithUserPassword(prop.getProperty("hadoop.principal"), prop.getProperty("hadoop.password"));
		}
		
		return conf;
	}
	
	protected String getResource(String file) {
		return new File(file).getPath();
	}
	
	/**
	   * main() has some simple utility methods
	   * @param argv the command and its arguments
	   * @throws Exception upon error
	   */
	  public static void main(String argv[]) throws Exception {
	    FsShell shell = newShellInstance();
	    
	    Configuration conf = new HDFSClient().getConf();
	    // Bug fix
	    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    
	    conf.setQuietMode(false);
	    shell.setConf(conf);
	    
	    int res;
	    try {
	      res = ToolRunner.run(shell, argv);
	    } finally {
	      shell.close();
	    }
	    System.exit(res);
	  }
	  
	  protected static FsShell newShellInstance() {
	    return new FsShell();
	    //return new DFSAdmin();
	  }
}