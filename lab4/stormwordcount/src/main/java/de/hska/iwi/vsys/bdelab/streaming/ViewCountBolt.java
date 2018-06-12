package de.hska.iwi.vsys.bdelab.streaming;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class ViewCountBolt extends NoisyBolt {
    private Map<Integer, Map<String, Integer>> counts = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String url = tuple.getString(0);
        int hour = tuple.getInteger(1);
        
        Map<String, Integer> hourBucked = counts.get(hour);
        if (hourBucked == null) {
        	hourBucked = new HashMap<>();
        	counts.put(hour, hourBucked);
        }
        
        Integer count = hourBucked.get(url);
        if (count == null) {
            count = 0;
        }
        count++;
        hourBucked.put(url, count);

        Values values = new Values(hour, url, count);
        
        this.printCounts();

        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hour-bucked", "url", "count"));
    }
    
    private void printCounts(){
    	System.out.println("Current state of Pageviews per Hour:");
    	for(Map.Entry<Integer, Map<String, Integer>> hourBucked : this.counts.entrySet()){
    		System.out.println("Hour-Bucked " + hourBucked.getKey() + ":");
    		for(Map.Entry<String, Integer> urlBucked : hourBucked.getValue().entrySet()){
    			System.out.println("    " + urlBucked.getKey() + " : " + urlBucked.getValue());
    		}
    	}
    	System.out.println();
    }
}
