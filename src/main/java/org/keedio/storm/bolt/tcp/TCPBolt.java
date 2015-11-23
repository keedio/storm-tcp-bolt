package org.keedio.storm.bolt.tcp;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TCPBolt extends BaseRichBolt {

    private static final long serialVersionUID = 8831211985061474513L;

    public static final Logger LOG = LoggerFactory
            .getLogger(TCPBolt.class);

    private Socket socket;
    private DataOutputStream output;
    private String host;
    private int port;
    private OutputCollector collector;


    @Override
    public void cleanup() {
        try {
        	if (socket != null)
        		socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        
    	try {
			loadBoltProperties(stormConf);
			connectToHost();
			this.collector = collector;
        } catch (ConfigurationException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
    	return null;
    }

    public void execute(Tuple input) {
    	
    	if (output!= null)
    	{	
	        try {
	            output.write(input.getBinary(0));
	            collector.ack(input);
	        } catch (SocketException se) {
	            collector.reportError(se);
	            collector.fail(input);
	            LOG.error("Connection with server lost");
	            connectToHost();
	        } catch (IOException e) {
	            collector.reportError(e);
	            collector.fail(input);
	            e.printStackTrace();
	        }
    	}
    }

    @SuppressWarnings("rawtypes")
	private void loadBoltProperties(Map stormConf) throws ConfigurationException {
    	
    	try {
	    	if (!stormConf.containsKey("tcp.bolt.host") || !stormConf.containsKey("tcp.bolt.port"))
	    		throw new ConfigurationException("\"tcp.bolt.host\" and \"tcp.bolt.port\" properties must be"
	    				+ "set in configuration file ");
	    	
	        host = (String) stormConf.get("tcp.bolt.host");
        
            port = Integer.parseInt((String) stormConf.get("tcp.bolt.port"));
        } catch (NumberFormatException e) {
            LOG.error("Error parsing tcp bolt from config file");
            e.printStackTrace();
            throw new NumberFormatException();
        }
    }

    private void connectToHost() {

        int retryDelayMs = 1000;
        boolean connected = false;

        while (!connected) {
            try {
                LOG.info("Trying to establish connection with host: " + host + " port: " + port);
                socket = new Socket(host, port);
                output = new DataOutputStream(socket.getOutputStream());
                connected = true;
            } catch (ConnectException e) {
                LOG.warn("Error establising TCP connection with host: " + host + " port: " + port);
                try {
                    Thread.sleep(retryDelayMs);
                    if (retryDelayMs < 10000)
                        retryDelayMs *= 1000;
                    continue;
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
