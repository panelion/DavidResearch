import com.nexr.platform.search.client.transport.NexRTransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 10/13/11
 * Time: 3:27 PM
 */
public class CreateIndexClient {

    // private final Router _router;

    private final NexRTransportClient _nodeClient;

    public CreateIndexClient(String clusterName, String serverList) {
        /*RouterFactory factory = RouterFactory.getInstance();
        _router  = factory.create(properties);*/

        Settings finalSettings = settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client", true)
                .put("stage.type", "local").build();

        NexRTransportClient transportClient = NexRTransportClient.create(finalSettings);

        String[] lists = serverList.split(",");

        for(String list : lists) {
            String[] serverInfo = list.trim().split(":");
            if(serverInfo.length == 2) transportClient.addTransportAddress(new InetSocketTransportAddress(serverInfo[0], Integer.parseInt(serverInfo[1])));
        }

        _nodeClient = transportClient;
    }

    public void deleteAllIndex() {
        _nodeClient.admin().indices().prepareDelete("_all").execute().actionGet();
    }

    public void createIndex(String indexName, Settings.Builder builder) {
        _nodeClient.admin().indices().prepareCreate(indexName).setSettings(builder).execute().actionGet();
        _nodeClient.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().execute().actionGet();
    }

    public static void main(String[] args) throws IOException {
        String clusterName = "equalize_test";
        String serverList = "10.1.8.3:9300,10.1.8.4:9300,10.1.8.5:9300,10.1.8.6:9300,10.1.8.7:9300,10.1.8.8:9300";

        String prefixIndexName = "test_";
        int indexCount = 10;

        CreateIndexClient client = new CreateIndexClient(clusterName, serverList);

        client.deleteAllIndex();

        for (int i = 0 ; i < indexCount; i++) {
            client.createIndex((prefixIndexName + i), ImmutableSettings.settingsBuilder());
        }

    }
}
