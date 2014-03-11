package connection;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import java.util.ArrayList;
import java.util.List;

public class ClusterConnection {

    Cluster cluster;

    public ClusterConnection(String name) {
        this.cluster = HFactory.getOrCreateCluster(name, new CassandraHostConfigurator("localhost:9160"));
    }

    public void createKeySpace(String keySpaceName, String[] columnFamilies) {

        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keySpaceName);

        if (keyspaceDef == null) {
            List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();

            for (String cf : columnFamilies) {
                cfDefs.add(HFactory.createColumnFamilyDefinition(keySpaceName, cf));
            }

            keyspaceDef = HFactory.createKeyspaceDefinition(keySpaceName, ThriftKsDef.DEF_STRATEGY_CLASS,
                1, cfDefs);
            this.cluster.addKeyspace(keyspaceDef, true);
        }

    }

    public Cluster getCluster() {
        return this.cluster;
    }

    public static class Template {

        ThriftColumnFamilyTemplate<String, String> template;

        public Template(Cluster cluster, String keySpaceName, String cf) {
            Keyspace keySpace = HFactory.createKeyspace(keySpaceName, cluster); //gets keyspace
            this.template = new ThriftColumnFamilyTemplate<String, String>(keySpace, cf,
                    StringSerializer.get(), StringSerializer.get());
        }

        public ColumnFamilyResult<String, String> getResult(String key) {
            return template.queryColumns(key);
        }

        public ColumnFamilyUpdater<String, String> getUpdater(String key) {
            return template.createUpdater(key);
        }

        public ThriftColumnFamilyTemplate<String, String> getTemplate() {
            return this.template;
        }

    }
}

