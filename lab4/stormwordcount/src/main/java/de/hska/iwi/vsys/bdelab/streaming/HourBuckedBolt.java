package de.hska.iwi.vsys.bdelab.streaming;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class HourBuckedBolt extends NoisyBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String url = tuple.getString(1);
        int hourBucked = this.getHourNumber( tuple.getInteger(2) );

        Values values = new Values(url, hourBucked);

        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "hour-bucked"));
    }
    
    private int getHourNumber(int timestamp){
    	return timestamp / 3600;
    }
}
