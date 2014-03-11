import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.SentenceSplitter;
import bolts.WordCounter;
import connection.ClusterConnection;
import spouts.TwitterSpout;

import java.util.Arrays;

public class TwitterTopologyRunner {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSpout());
        builder.setBolt("sentence-splitter", new SentenceSplitter()).shuffleGrouping("twitter-spout");
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("sentence-splitter", new Fields("word"));

        //Configuration
        Config conf = new Config();
        conf.setDebug(false);
        conf.put("consumerKey", "vH049xiF0HDqATxo1QCg");
        conf.put("consumerSecret", "ipDRPCsKGT1BvRsoji5pjbCrVNlabL3mUfrvjcqwaU");
        conf.put("token", "552385752-jIN7RyiWVuk7tIA5In0FPkYBBwogBzDCEzOZciDK");
        conf.put("tokenSecret", "nIE4zqzrRbBRUUQnmRgRbm8QV3HvPUFSq3zD46xyW4laE");
        conf.put("terms", Arrays.asList(args));

        initializeDatabase();

        //Topology run
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Twitter-Topology", conf, builder.createTopology());
        //Thread.sleep(10000);
        //cluster.shutdown();
    }

    //TODO: Put this in a class
    public static void initializeDatabase() {

        ClusterConnection cluster = new ClusterConnection("TwitterCluster");
        String[] cfs = {"words"};
        cluster.createKeySpace("wordsKeyspace", cfs);

    }
}
