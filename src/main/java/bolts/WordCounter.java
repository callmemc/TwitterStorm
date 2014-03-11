package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import connection.ClusterConnection;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.hector.api.exceptions.HectorException;

import java.util.Map;

public class WordCounter extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        collector = this.collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        ClusterConnection cluster = new ClusterConnection("TwitterCluster");

        ClusterConnection.Template template = new ClusterConnection.Template(cluster.getCluster(), "wordsKeyspace", "words");

        //read
        ColumnFamilyResult<String, String> res = template.getResult(word);
        Integer count = res.getInteger("count");

        //write
        ColumnFamilyUpdater<String, String> updater = template.getUpdater(word);
        if (count == null) { //does this need to be 0???
            updater.setInteger("count", 0);
        } else {
            updater.setInteger("count", count + 1);
        }

        ColumnFamilyResult<String, String> oldRes = template.getResult(word);
        count = oldRes.getInteger("count");

//        System.out.println("***Before Count for " + word + ": " + count);

        try {
            template.getTemplate().update(updater);
        } catch (HectorException e) {
            collector.reportError(e);
        }

        ColumnFamilyResult<String, String> newRes = template.getResult(word);
        count = newRes.getInteger("count");

        System.out.println("***After Count for " + word + ": " + count);
    }

}
