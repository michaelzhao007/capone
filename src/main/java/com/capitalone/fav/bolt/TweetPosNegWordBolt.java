package com.capitalone.fav.bolt;

import java.util.Map;

import com.capitalone.fav.model.CapOneQaTw;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TweetPosNegWordBolt extends BaseRichBolt{
   
	private OutputCollector collector;
	
    private double posSco=0;
    private double negSco=0;
    
    private double score=0; 
	
	private String[] posWords={
		"good","Good","Great","great","awesome","Awesome","nice","excellent","Excellent",
		"patient","kind","responsive","professional","honest","impressive"
	};
	
	private String[] negWords = {
		"misleading","intolerable","bait","stupid","fail","aweful","terrible","bad","worst","scam","steal","horrible","nightmare"
	};
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String word = tuple.getString(0);
	    String delim = "[ ]+";
	    String[] tokens = word.split(delim);
	    for(String token:tokens) {
	      for(String pos : posWords) {
	    	  if(token.equalsIgnoreCase(pos)) posSco++;
	      }
	      for(String neg: negWords) {
	    	  if(token.equalsIgnoreCase(neg)) negSco++;
	      }
	    }
	    score = posSco/(posSco+negSco);
	    System.out.println("score: " +score + " posSco: " +posSco + " negSco: "+negSco);
   		//CapOneQaTw tw = new CapOneQaTw();
   		//tw.setNegSco(negSco);
   		//tw.setPosSco(posSco);
   		//tw.setScore(score);
	    collector.emit(new Values(score, posSco, negSco, word));
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector optCol) {
		// TODO Auto-generated method stub
		collector = optCol;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sco","pos","neg","word"));
	}

}
