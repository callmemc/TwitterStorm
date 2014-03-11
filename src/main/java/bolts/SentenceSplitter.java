package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;

public class SentenceSplitter extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String tweet = tuple.getStringByField("tweet");

        try {
            JSONObject tweetJSON = new JSONObject(tweet);
            String[] words = tweetJSON.getString("text").split(" ");
            for (String word : words) {
                word = word.trim();
                if(!word.isEmpty()) {
                    word = word.toLowerCase();
                    collector.emit(new Values(word));
                }
            }
            collector.ack(tuple);
        } catch (JSONException e) {
            System.out.println(e.getMessage());
            collector.fail(tuple);  //am i supposed to do this?
        }

    }

}
