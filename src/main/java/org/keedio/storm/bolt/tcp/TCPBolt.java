package org.keedio.storm.bolt.tcp;

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
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.keedio.storm.bolt.tcp.metrics.MetricsController;
import org.keedio.storm.bolt.tcp.metrics.MetricsEvent;
import org.keedio.storm.bolt.tcp.metrics.SimpleMetric;
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
    private MetricsController mc;
    private int refreshTime;
    private Date lastExecution = new Date();

    //for Ganglia only
    private String hostGanglia, reportGanglia;
    private GMetric.UDPAddressingMode modeGanglia;
    private int portGanglia, ttlGanglia;
    private int minutesGanglia;


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

        if (stormConf.get("refreshtime") == null)
            refreshTime = 10;
        else
            refreshTime = Integer.parseInt((String) stormConf.get("refreshtime"));

        //check if in topology's config ganglia.report is set to "yes"
        if (loadGangliaProperties(stormConf)) {
            mc = new MetricsController(hostGanglia, portGanglia, modeGanglia, ttlGanglia, minutesGanglia);
        } else {
            mc = new MetricsController();
        }

        mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "written"));
        mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "error_IO"));
        mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "error_Connection"));

        // Registramos la metrica para su publicacion
        SimpleMetric written = new SimpleMetric(mc.getMetrics(), "written", SimpleMetric.TYPE_METER);
        SimpleMetric error_IO = new SimpleMetric(mc.getMetrics(), "error_IO", SimpleMetric.TYPE_METER);
        SimpleMetric error_Connection = new SimpleMetric(mc.getMetrics(), "error_Connection", SimpleMetric.TYPE_METER);
        SimpleMetric histogram = new SimpleMetric(mc.getMetrics(), "histogram", SimpleMetric.TYPE_HISTOGRAM);

        context.registerMetric("written", written, refreshTime);
        context.registerMetric("error_IO", error_IO, refreshTime);
        context.registerMetric("error_Connection", error_Connection, refreshTime);
        context.registerMetric("histogram", histogram, refreshTime);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void execute(Tuple input) {
        // Añadimos al throughput e inicializamos el date
        Date actualDate = new Date();
        long aux = (actualDate.getTime() - lastExecution.getTime()) / 1000;
        lastExecution = actualDate;

        // Registramos para calculo de throughput
        mc.manage(new MetricsEvent(MetricsEvent.UPDATE_THROUGHPUT, aux));

        try {
            output.writeBytes(input.getString(0) + "\n");
            collector.ack(input);
            mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "written"));


        } catch (SocketException se) {
            mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "error_Connection"));
            collector.reportError(se);
            collector.fail(input);
            LOG.error("Connection with server lost");
            connectToHost();
        } catch (IOException e) {
            collector.reportError(e);
            collector.fail(input);
            mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "error_IO"));
            e.printStackTrace();
        }
    }

    @SuppressWarnings("rawtypes")
    private void loadBoltProperties(Map stormConf) {
        host = (String) stormConf.get("tcp.bolt.host");
        try {
            port = Integer.parseInt((String) stormConf.get("tcp.bolt.port"));
        } catch (NumberFormatException e) {
            LOG.error("Error parsing tcp bolt from config file");
            e.printStackTrace();
            throw new NumberFormatException();
        }
    }

    private void connectToHost() {

        int retryDelay = 1;
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
                    Thread.sleep(retryDelay * 1000);
                    if (retryDelay < 60)
                        retryDelay *= 2;
                    continue;
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            } catch (IOException e) {
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
        } catch (UnknownHostException e) {
            LOG.error("Could not determine hostname");
        }
        return hostname;
    }

    private static String extractHostnameFromFQHN(String fqhn) {
        if (hostnamePattern.matcher(fqhn).matches()) {
            if (fqhn.contains(".")) {
                return fqhn.split("\\.")[0];
            } else {
                return fqhn;
            }
        } else {
            // We want to return the input as-is
            // when it is not a valid hostname/FQHN.
            return fqhn;
        }
    }

    /**
     * ganglia's server properties are taken from main topology's config
     *
     * @param stormConf
     * @return
     */
    private boolean loadGangliaProperties(Map stormConf) {
        boolean loaded = false;
        reportGanglia = (String) stormConf.get("ganglia.report");
        if (reportGanglia.equals("yes")) {
            hostGanglia = (String) stormConf.get("ganglia.host");
            portGanglia = Integer.parseInt((String) stormConf.get("ganglia.port"));
            ttlGanglia = Integer.parseInt((String) stormConf.get("ganglia.ttl"));
            minutesGanglia = Integer.parseInt((String) stormConf.get("ganglia.minutes"));
            String stringModeGanglia = (String) stormConf.get("ganglia.UDPAddressingMode");
            modeGanglia = GMetric.UDPAddressingMode.valueOf(stringModeGanglia);
            loaded = true;
        }
        return loaded;
    }

}
