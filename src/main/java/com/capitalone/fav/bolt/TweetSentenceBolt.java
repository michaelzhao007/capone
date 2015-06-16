package com.capitalone.fav.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TweetSentenceBolt extends BaseRichBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 11221L;

	OutputCollector collector;
	
	//private String[] containedWords = {"capitalOne","Capital One","CapitalOne","capital one","Capital One","Spark","spark","credit"};
    private String[] containedWords = {"is","are","am","you"};

	@Override
	public void execute(Tuple tuple) {
		String tweet = tuple.getString(0);
		String delims ="[.?!]+";
		String[] tokens = tweet.split(delims);
	    for(String token:tokens) {
	    	for(int i = 0; i < containedWords.length; i++) {
	    	if(token.contains(containedWords[i])) {
	    	   		collector.emit(new Values(token));
	    	   		break;
	    	}
	       }
	    }
	}

	@Override
	public void prepare(Map map, TopologyContext topCxt, OutputCollector optCol) {
		// TODO Auto-generated method stub
		collector = optCol;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tweet-word"));
	}
}
