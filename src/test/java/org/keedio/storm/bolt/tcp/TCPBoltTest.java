package org.keedio.storm.bolt.tcp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Test;

public class TCPBoltTest {
	
	@Test
    public void testTCPBolt() {
		
		MkClusterParam mkClusterParam = new MkClusterParam();
	    mkClusterParam.setSupervisors(1);
	    Config daemonConf = new Config();
	    daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
	    
	    mkClusterParam.setDaemonConf(daemonConf);
	    
	    TestJob testJob = new TestJob() {
            @SuppressWarnings("rawtypes")
			@Override
            public void run(ILocalCluster cluster) throws IOException {
                TopologyBuilder builder = new TopologyBuilder();

                builder.setSpout("fakeKafkaSpout", new FeederSpout(new Fields("field1")));

                builder.setBolt("TCPBolt", new TCPBolt())
                .shuffleGrouping("fakeKafkaSpout");

                StormTopology topology = builder.createTopology();

                MockedSources mockedSources = new MockedSources();

                //Our spout will be processing this values.
                mockedSources.addMockData("fakeKafkaSpout",new Values("fieldValue1".getBytes()));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(1);
                conf.put("tcp.bolt.port", "7657");
                conf.put("tcp.bolt.host", "localhost");
                conf.put(Config.NIMBUS_SEEDS, Collections.singletonList("localhost"));
                conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
                

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);
               
               Future<String> future = startServer();

                Testing.completeTopology(cluster, topology, completeTopologyParam);

                // check whether the result is right
                try {
					Assert.assertEquals("fieldValue1", future.get());
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
            }
	    };
	
	    Testing.withSimulatedTimeLocalCluster(mkClusterParam, testJob);
    }
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Future<String> startServer() {
        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);

        Callable<String> serverTask = new Callable() {
            @Override
            public String call() {
            	BufferedReader in;
                try {
                    ServerSocket serverSocket = new ServerSocket(7657);
                    Socket clientSocket = serverSocket.accept();
                    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    serverSocket.close();
                    return in.readLine();
                    } catch (IOException e) {
                    System.err.println("Unable to process client request");
                    e.printStackTrace();
                }
                return "";
            }
        };
        return clientProcessingPool.submit(serverTask);
    }

}

