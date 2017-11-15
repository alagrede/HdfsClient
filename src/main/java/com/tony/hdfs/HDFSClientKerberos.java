package com.tony.hdfs;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;


public class HDFSClientKerberos {

	private String krbConfUrl;
	private String hadoopCluster;
	private String hadoopProxy;
	private String namenodes;
	private String rpcAaddress;
	private String httpAaddress;
	private String jaasConfUrl;
	
	public void setJaasConfUrl(String jaasConfUrl) {
		this.jaasConfUrl = jaasConfUrl;
	}

	public void setKrbConfUrl(String krbConfUrl) {
		this.krbConfUrl = krbConfUrl;
	}

	public void setHadoopCluster(String hadoopCluster) {
		this.hadoopCluster = hadoopCluster;
	}

	public void setHadoopProxy(String hadoopProxy) {
		this.hadoopProxy = hadoopProxy;
	}

	public void setNamenodes(String namenodes) {
		this.namenodes = namenodes;
	}

	public void setRpcAddress(String rpcAaddress) {
		this.rpcAaddress = rpcAaddress;
	}

	public void setHttpAaddress(String httpAaddress) {
		this.httpAaddress = httpAaddress;
	}

	public FileSystem hadoopConnectionWithKeytab(String keytabUrl, String principal) throws URISyntaxException, NoSuchAlgorithmException, IOException {

		Configuration conf = prepareConfiguration();

		URL keytab = getResource(keytabUrl);
		UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());
		return FileSystem.get(conf);
	}
	
	public FileSystem hadoopConnectionWithUserPassword(final String principal, final String password) throws URISyntaxException, NoSuchAlgorithmException, IOException, LoginException {
		
		Configuration conf = prepareConfiguration();
		
		final char[] passwordChar = password.toCharArray();
		
		URL jaasConfigUrl = getResource(jaasConfUrl);
		java.security.URIParameter uriParam = new java.security.URIParameter(jaasConfigUrl.toURI());
		javax.security.auth.login.Configuration jaasConfig = sun.security.provider.ConfigFile.getInstance("JavaLoginConfig", uriParam);

		LoginContext lc = new LoginContext("HdfsHaSample", new Subject(), new CallbackHandler() {
			
			@Override
			public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
				for (Callback c : callbacks) {
					if (c instanceof NameCallback) {
						((NameCallback) c).setName(principal);
					}
					if (c instanceof PasswordCallback) {
						((PasswordCallback) c).setPassword(passwordChar);
					}
				}
			}
		}, jaasConfig);
		lc.login();

		UserGroupInformation.loginUserFromSubject(lc.getSubject());

		return FileSystem.get(conf);
	}
	
	private Configuration prepareConfiguration() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://" + hadoopCluster);
		conf.set("hadoop.security.authentication", "kerberos");

		URL krb5Url = getResource(krbConfUrl);
		System.setProperty("java.security.krb5.conf", krb5Url.getPath());

		if (!"".equals(hadoopProxy)) {
			conf.set("dfs.client.failover.proxy.provider."+ hadoopCluster, hadoopProxy);
		}
		conf.set("dfs.nameservices", hadoopCluster);
		conf.set("dfs.ha.namenodes." + hadoopCluster, namenodes);

		if (namenodes.contains(",")) {
			String[] nn = namenodes.split(",");

			for (int i = 0; i < nn.length; i++) {
				conf.set("dfs.namenode.rpc-address." + hadoopCluster + "." + nn[i], rpcAaddress.split(",")[i]);
				conf.set("dfs.namenode.http-address." + hadoopCluster + "." + nn[i], httpAaddress.split(",")[i]);
			}
		} else {
			conf.set("dfs.namenode.rpc-address." + hadoopCluster + "." + namenodes, rpcAaddress);
			conf.set("dfs.namenode.http-address." + hadoopCluster + "." + namenodes, httpAaddress);
		}

		UserGroupInformation.setConfiguration(conf);
		return conf;
	}

	protected URL getResource(String file) {
		ClassLoader classLoader = getClass().getClassLoader();
		return classLoader.getResource(file);
	}
}
