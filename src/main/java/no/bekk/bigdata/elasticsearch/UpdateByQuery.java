package no.bekk.bigdata.elasticsearch;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


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
    AtomicLong totalUpdated = new AtomicLong(0);
    AtomicLong totalHits = new AtomicLong();
    AtomicInteger runningThreads = new AtomicInteger(0);
    BulkProcessor bulkProcessor;

    @Override
    public void initialize(Parameters parameters) {
        param = parameters;

        if (param.filter.isEmpty()){
            System.out.print("No filter given! Aborting. Set filter with argument --filter=YourFilterHere");
            throw new RuntimeException("Missing parameter: filter!");
        }
        if (param.setTo < 0){
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

        bulkProcessor = createBulkProcessor();
    }

    private BulkProcessor createBulkProcessor() {
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                runningThreads.incrementAndGet();
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse response) {
                handleBulkResponseError(response);
                totalUpdated.getAndAdd((response.getItems().length));
                if (param.logging) {
                    System.out.println("" + response.getItems().length + " updates in " +
                            response.getTook() + ". " + (totalHits.get() - totalUpdated.get()) + " doc remaining");
                }
                runningThreads.decrementAndGet();
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                throw new RuntimeException(throwable.getMessage());
            }
        }).setBulkActions(param.bulkSize).setConcurrentRequests(2).build();
        return bulkProcessor;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();

        if (param.logging) {
            System.out.println("Starting bulk requests with \nbulksize: " + param.bulkSize +
                    "\nfilter: " + param.filter);
        }

        SearchResponse response = startScroll();
        String scrollId = response.getScrollId();
        totalHits.set(response.getHits().totalHits());

        if(param.logging) {
            System.out.println("Total hits found: " + response.getHits().totalHits() + ".");
        }

        searchAndUpdate(scrollId, response);

        bulkProcessor.close();

        waitForBulks();
        client.admin().indices().prepareFlush().setRefresh(true).execute();

        if (param.logging) {
            TimeValue timeTaken = new TimeValue(System.currentTimeMillis() - startTime);
            double docsPerSec = totalUpdated.get() * 1000 / timeTaken.getMillis();
            System.out.println("Updated " + totalUpdated.get() + " docs in " + timeTaken);
            System.out.println("" + docsPerSec + " docs per second.");
        }
    }

    private void waitForBulks() {
        boolean waitForBulks = true;
        while (waitForBulks) {
            if (runningThreads.get() == 0) {
                waitForBulks = false;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException IEx) {
                    System.out.print("Too much coffee. Can't sleep! Spinning...");
                }
            }
        }
    }

    private void searchAndUpdate(String scrollId, SearchResponse response) {
        SimpleTrans[] transactions = extractTransFromResponse(response);

        if (transactions == null)
            return;

        addToBulk(transactions);

        do {

            SearchResponse scrollResponse = client.prepareSearchScroll(scrollId).setScroll("5m").execute().actionGet();
            int hitsThisResponse = scrollResponse.getHits().hits().length;

            if (param.logging) {
                System.out.println("" + hitsThisResponse + " hits in " + scrollResponse.getTook() + ".");
            }

            if (hitsThisResponse == 0) //aka there is no more to update
                break;

            transactions = extractTransFromResponse(response);
            addToBulk(transactions);
        } while (totalUpdated.get() < totalHits.get());
    }

    private void addToBulk(SimpleTrans[] transactions) {
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("val", param.setTo);

        for (SimpleTrans trans : transactions) {
            boolean valid = trans.checkValidity();
            if (valid)
                bulkProcessor.add(client.prepareUpdate(trans.index, "trans", trans.id)
                        .setScript("ctx._source.category = val;")
                        .setScriptParams(scriptParams)
                        .setRouting(trans.accNo)
                        .request()
                );
            else
                System.out.print("Invalid trans!!!!");
        }
    }

    private void handleBulkResponseError(BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            System.out.println(bulkResponse.getItems()[0].getFailureMessage());
        }
    }

    private SearchResponse startScroll() {
        FilterBuilder filter = FilterBuilders.notFilter(FilterBuilders.termFilter("category",param.setTo));
        SearchResponse response = client.prepareSearch()
                .setQuery(QueryBuilders.filteredQuery(
                        QueryBuilders.matchQuery("description", param.filter),
                        filter
                ))
                .setScroll("5m")
                .addFields("accountNumber")
                .setSize(param.bulkSize)
                .execute()
                .actionGet();
        return response;
    }

    //May be useful as a separate plugin?
    private void sendMultiGet(SimpleTrans[] transactions) {
        MultiGetRequestBuilder mgrb = client.prepareMultiGet();
        for (SimpleTrans trans : transactions) {
            mgrb.add(new MultiGetRequest.Item(trans.index, "trans", trans.id).routing(trans.accNo));
        }
        MultiGetResponse resp = mgrb.execute().actionGet();
        for (MultiGetItemResponse r : resp) {
            System.out.println(r.getResponse().isExists() + " " + r.getId());
        }
    }

    private SimpleTrans[] extractTransFromResponse(SearchResponse response) {
        SearchHit[] hits = response.getHits().getHits();
        int hitsThisBulk = hits.length;

        SimpleTrans[] transactions = new SimpleTrans[hitsThisBulk];
        for(int i = 0; i < hitsThisBulk; i++) {
            long accNo = hits[i].getFields().get("accountNumber").getValue();
            transactions[i] = new SimpleTrans(hits[i].getId(), hits[i].getIndex(), accNo);
        }

        if (hitsThisBulk == 0) {
            return null;
        } else {
            return transactions;
        }
    }

    private class SimpleTrans {
        String id = "";
        String index = "";
        String accNo = "";

        SimpleTrans(String id, String index, long accNo) {
            this.id = id.trim();
            this.index = index.trim();
            this.accNo = ""+accNo;
        }

        public boolean checkValidity() {
            if (id.isEmpty())
                return false;

            if (index.isEmpty())
                return false;

            if (accNo.isEmpty())
                return false;

            return true;
        }
    }
}
