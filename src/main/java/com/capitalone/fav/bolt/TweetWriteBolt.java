package com.capitalone.fav.bolt;

import java.net.UnknownHostException;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TweetWriteBolt extends BaseRichBolt
{
     MongoClient client = null;
	  @Override
	  public void prepare(
	      Map                     map,
	      TopologyContext         topologyContext,
	      OutputCollector         outputCollector)
	  {
	    // instantiate mongo connection
	    try {
			client = getClient();
		  
		    
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }

	@Override
	public void execute(Tuple tuple) {
		  DB db = client.getDB("caponesocmedia");
		  DBCollection table = db.getCollection("score");
		  BasicDBObject doc = new BasicDBObject();	
		  if(!tuple.getDouble(0).isNaN()) {
		  doc.put("score", tuple.getDouble(0));
		  doc.put("positive", tuple.getDouble(1));
		  doc.put("negative", tuple.getDouble(2));
		  doc.put("word", tuple.getString(3));
		  table.insert(doc);
		  }
		  
	}
	
	
	private MongoClient getClient() throws UnknownHostException {
		if(client==null) return  new MongoClient("localhost",27017);
		else return client;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	
}
