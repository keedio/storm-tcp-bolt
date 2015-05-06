package com.keedio.storm.bolt;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TCPBolt extends BaseRichBolt {

	private static final Pattern hostnamePattern =
			    Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9-]*(\\.([a-zA-Z0-9][a-zA-Z0-9-]*))*$");
	  
	private static final long serialVersionUID = 8831211985061474513L;

	public static final Logger LOG = LoggerFactory
			.getLogger(TCPBolt.class);
	
	private Socket socket;
	private DataOutputStream output;
	private String host;
	private int port;
	private OutputCollector collector;
    private Date lastExecution = new Date();
    
    // Declaramos el adaptador y las metricas de yammer
    private MetricRegistry metric;
    private Meter meter;
    private com.codahale.metrics.Histogram histogram;
	

	@Override
	public void cleanup() {
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		loadBoltProperties(stormConf);
		connectToHost();
		this.collector = collector;
        
		// Tiempo de notificacion de metricas en los diferentes bolts
        //stormConf.put(YammerFacadeMetric.FACADE_METRIC_TIME_BUCKET_IN_SEC, 10);
        
        metric = new MetricRegistry();
        meter = metric.meter("meter");
        histogram = metric.histogram("histogram");
        
        com.codahale.metrics.JmxReporter reporter = com.codahale.metrics.JmxReporter.forRegistry(metric).inDomain(metricsPath()).build();
        reporter.start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void execute(Tuple input) {
		try {
			output.writeBytes(input.getBinary(0) + "\n");
            collector.ack(input);

            // Añadimos al throughput e inicializamos el date
            Date actualDate = new Date();
            long aux = (actualDate.getTime() - lastExecution.getTime())/1000;
            lastExecution = actualDate;
            
            // Registramos para calculo de throughput
            histogram.update(aux);
		} catch (SocketException se){
            meter.mark();
            collector.reportError(se);
            collector.fail(input);
			LOG.error("Connection with server lost");
			connectToHost();
		} catch (IOException e) {
			collector.reportError(e);
			collector.fail(input);
            meter.mark();
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void loadBoltProperties(Map stormConf){
		host = (String) stormConf.get("tcp.bolt.host");
		try {
			port = Integer.parseInt((String) stormConf.get("tcp.bolt.port"));
		}catch (NumberFormatException e){
			LOG.error("Error parsing tcp bolt from config file");
			e.printStackTrace();
			throw new NumberFormatException();
		}
	}
	
	private void connectToHost(){
		
		int retryDelay=1;
		boolean connected = false;
		
		while (!connected){
			try {
				LOG.info("Trying to establish connection with host: "+host+" port: "+port);
				socket = new Socket(host,port);
				output = new DataOutputStream(socket.getOutputStream());
				connected = true;
			}
			catch (ConnectException e){
				LOG.warn("Error establising TCP connection with host: "+host+" port: "+port);
				try{			
					Thread.sleep(retryDelay*1000);
					if (retryDelay < 60)
							retryDelay*=2;
					continue;
				}
				catch (InterruptedException ie){
					ie.printStackTrace();
				}
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private String metricsPath() {
	    final String myHostname = extractHostnameFromFQHN(detectHostname());
	    return myHostname;
	}

	  private static String detectHostname() {
		    String hostname = "hostname-could-not-be-detected";
		    try {
		      hostname = InetAddress.getLocalHost().getHostName();
		    }
		    catch (UnknownHostException e) {
		      LOG.error("Could not determine hostname");
		    }
		    return hostname;
		  }

		  private static String extractHostnameFromFQHN(String fqhn) {
		    if (hostnamePattern.matcher(fqhn).matches()) {
		      if (fqhn.contains(".")) {
		        return fqhn.split("\\.")[0];
		      }
		      else {
		        return fqhn;
		      }
		    }
		    else {
		      // We want to return the input as-is
		      // when it is not a valid hostname/FQHN.
		      return fqhn;
		    }
		  }
		
}
