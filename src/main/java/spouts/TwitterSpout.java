package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {

    private Client client;
    SpoutOutputCollector collector;
    //TODO: Replace with Kafka
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        endpoint.trackTerms((List<String>) conf.get("terms"));

//        List<Long> followings = Arrays.asList(552385752L);
//        List<Location> locs = Arrays.asList(new Location(new Location.Coordinate(-122.75,36.8), new Location.Coordinate(-121.75,37.8)));
//        this isn't working when included with track terms
//        endpoint.followings(followings);
//        this is producing"locations=com.twitter.hbc.core.endpoint.Location%40325e9e34"
//        endpoint.locations(locs);
//        System.out.println(endpoint.getPostParamString());

        Authentication hosebirdAuth = new OAuth1((String) conf.get("consumerKey"),
                (String) conf.get("consumerSecret"),
                (String) conf.get("token"),
                (String) conf.get("tokenSecret"));

        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(hosebirdAuth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        client.connect();
    }

    @Override
    public void close() {
        client.stop();
    }

    @Override
    public void nextTuple() {
        try {
            while(!client.isDone()) {
                String msg = queue.take();
                collector.emit(new Values(msg));
            }
        } catch (InterruptedException e) {
            collector.reportError(e);
        }
    }
}
