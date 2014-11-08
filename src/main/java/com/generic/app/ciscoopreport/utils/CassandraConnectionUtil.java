/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.generic.app.ciscoopreport.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CassandraConnectionUtil {

//		private static final Log log = LogFactory.getLog(CassandraConnectionUtil.class);

	private static CassandraConnectionUtil instance = null;
	private String[] nodes1=null;

    PoolingOptions poolingOptions = new PoolingOptions();


	private Cluster cluster;
	
	private Session session;
	
	private CassandraConnectionUtil(String serverNames) {
	//	log.debug("Goint to setup cluster");
		setCluster(serverNames);
//		log.debug("cluster setup completed");
	}
	
	public static CassandraConnectionUtil getInstance(String serverNames) {
		if (instance == null) {
			synchronized (CassandraConnectionUtil.class) {
				if (instance == null) {
	//				log.debug("Creating instance of CassandraConnectionUtil class");
					instance = new CassandraConnectionUtil(serverNames);
				}
			}
		}
		return instance;
	}
	private void setNodes(String[] nodes) {
		//127.0.0.1, 127.0.0.2, 127.0.0.3
		this.nodes1 = nodes;
		System.out.println(nodes);
	}
	private void setCluster(String serverNames) {
	//	EnvProps props = EnvProps.getInstance();
		String nodes[]=serverNames.split(",");
		//String nodes[] = {"ras1", "ras11"};
		setNodes(nodes);
		int serverPort = 9042;//Integer.parseInt(props.getProperty("cassandra.db.port"));
		 boolean allowRemoteDCSForLocalConsistency = false;
	      int usedHostsPerRemoteDC = 2;
		try {
			if (cluster == null) {
			
				cluster = new Cluster.Builder()
				//.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
				.addContactPoints(nodes1)
				//.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("DC1",2)))
				//.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("DC1")))
				.withPort(serverPort)
				.withCredentials("cassandra", "cassandra")
			//	.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("Cassandra", usedHostsPerRemoteDC))
				.withPoolingOptions( poolingOptions)
				.withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE))
				 //.withRetryPolicy(new LoggingRetryPolicy(Policies.defaultRetryPolicy()))
				//new LoggingRetryPolicy(Policies.defaultRetryPolicy())
				.build();
				
			}
		} catch (Exception e) {
			//log.error(e, e);
		}
	}
	
	public Session getSession(){
		if (session == null) {
			System.out.println("Session is null");
			synchronized (CassandraConnectionUtil.class) {
				if (session == null) {
	//				log.debug("Creating instance of CassandraConnectionUtil class");
					session = cluster.connect("cisco");
				}
			}
		}
		else
		{
			System.out.println("Session is not null");
		}
		return session;
		
	}
	
	public void shutdown(){
		System.out.println("Checking if session is null");
		if(null != session){
			session.close();
	        session.getCluster().close();
		}
	}
}
