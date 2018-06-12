package de.hska.iwi.vsys.bdelab.streaming;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.net.URI;
import java.net.URISyntaxException;

public class SplitSentenceBolt extends NoisyBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	
        String sentence = tuple.getString(4);
        
        String[] infoArray = sentence.split("\\s");
        infoArray[1] = normalizeUrl(infoArray[1]);
        try {
        	collector.emit(new Values(infoArray[0], infoArray[1], Integer.parseInt(infoArray[2]) ));
        } catch (Exception e) {
        	
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "url", "epoch-time"));
    }
    
    private String normalizeUrl(String url){
    	URI uri = null;
		try {
			uri = new URI(url);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
    	uri.normalize();
    	return uri.getHost() + uri.getPath();
    }
}
