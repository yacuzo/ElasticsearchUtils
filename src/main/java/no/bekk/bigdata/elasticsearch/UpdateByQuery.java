package no.bekk.bigdata.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Created with IntelliJ IDEA.
 * User: andre b. amundsen
 * Date: 18/07-13
 * Time: 12:18
 * To change this template use File | Settings | File Templates.
 */
public class UpdateByQuery implements Util{
    Parameters param;
    TransportClient client;
    long totalUpdated = 0;
    long totalHits = 0;

    @Override
    public void initialize(Parameters parameters) {
        param = parameters;

        if (param.filter.isEmpty()){
            System.out.print("No filter given! Aborting. Set filter with argument --filter=YourFilterHere");
            throw new RuntimeException("Missing parameter: filter!");
        }
        if (param.setTo > 0){
            System.out.print("No value to set! Aborting. Set value with argument --value=1 (a Short)");
            throw new RuntimeException("Missing parameter: value!");
        }


        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", parameters.clusterName)
                .build();
        client = new TransportClient(settings);
        System.out.println("Connecting to: " + parameters.hosts);
        String[] hosts = parameters.hosts.split(",");

        for (String s : hosts)
            client.addTransportAddress(new InetSocketTransportAddress(s, parameters.port));

    }

    @Override
    public void run() {
        SimpleTrans[] transactions;

        if (param.logging) {
            System.out.println("Starting bulk requests with bulksize: " + param.bulkSize +" and filter: " + param.filter);
        }

        do {
            SearchResponse response = client.prepareSearch()
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addFields(new String[]{"id"})
                    .setSize(param.bulkSize)
                    .setFilter(FilterBuilders.queryFilter(
                            QueryBuilders.matchQuery("description", param.filter)))
                    .execute()
                    .actionGet();

            transactions = responseToTransactions(response);
            BulkResponse bulkResponse = sendBulkUpdate(transactions);

            //TODO check bulk response for errors
        } while (totalUpdated < totalHits);
        //TODO print confirmation or something
    }

    private BulkResponse sendBulkUpdate(SimpleTrans[] transactions) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (SimpleTrans trans : transactions) {
            bulkRequest.add(client.prepareUpdate(trans.index, "trans", trans.id)
                    .setScript("ctx._source.category=\"" + param.setTo + "\"")
            ); //TODO make the field to set a variable (if ever needed)
        }
        return bulkRequest.execute().actionGet();
    }

    private SimpleTrans[] responseToTransactions(SearchResponse response) {
        SearchHit[] hits = response.getHits().getHits();
        int hitsThisBulk = hits.length;
        totalUpdated += hitsThisBulk;

        SimpleTrans[] transactions = new SimpleTrans[hitsThisBulk];
        //TODO write this.
        for(int i = 0; i < hitsThisBulk; i++) {
            transactions[i] = new SimpleTrans(hits[i].getId(), hits[i].getIndex());
        }
        return transactions;
    }

    private class SimpleTrans {
        String id;
        String index;

        SimpleTrans(String id, String index) {
            this.id = id;
            this.index = index;
        }
    }
}
