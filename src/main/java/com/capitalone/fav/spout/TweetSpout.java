package com.capitalone.fav.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TweetSpout extends BaseRichSpout {
	SpoutOutputCollector collector;

	TwitterStream twitterStream;

	LinkedBlockingQueue<String> queue = null;

	String custkey, custsecret, accesstoken, accesssecret;

	private class TweetListener implements StatusListener {

		@Override
		public void onException(Exception e) {
			// TODO Auto-generated method stub
			e.printStackTrace();
		}

		@Override
		public void onDeletionNotice(StatusDeletionNotice arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onScrubGeo(long arg0, long arg1) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onStallWarning(StallWarning arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onStatus(Status status) {
			// TODO Auto-generated method stub
			queue.offer(status.getText());
		}

		@Override
		public void onTrackLimitationNotice(int arg0) {
			// TODO Auto-generated method stub

		}

	};

	public TweetSpout(String key, String secret, String token,
			String tokensecret) {
		custkey = key;
		custsecret = secret;
		accesstoken = token;
		accesssecret = tokensecret;
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
        String ret = queue.poll();
        if(ret==null) {
        	Utils.sleep(50);
        	return;
        }
        collector.emit(new Values(ret));
	}

	@Override
	public void open(Map map, TopologyContext topContext,
			SpoutOutputCollector sptOptCol) {
		// TODO Auto-generated method stub
		queue = new LinkedBlockingQueue<String>(1000);
		collector = sptOptCol;
		ConfigurationBuilder config = new ConfigurationBuilder()
				.setOAuthConsumerKey(custkey)
				.setOAuthConsumerSecret(custsecret)
				.setOAuthAccessToken(accesstoken)
				.setOAuthAccessTokenSecret(accesssecret);
        TwitterStreamFactory fact = new TwitterStreamFactory(config.build());
        twitterStream = fact.getInstance();
        twitterStream.addListener(new TweetListener());
        twitterStream.sample();
	}
	
	@Override
	public void close() {
		twitterStream.shutdown();
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer dec) {
		// TODO Auto-generated method stub
		dec.declare(new Fields("tweet"));

	}

}
