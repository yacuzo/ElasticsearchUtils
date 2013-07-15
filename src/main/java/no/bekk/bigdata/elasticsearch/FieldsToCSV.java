package no.bekk.bigdata.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: andre b. amundsen
 * Date: 15/07-13
 * Time: 10:17
 * To change this template use File | Settings | File Templates.
 */
public class FieldsToCSV implements Util {
    Parameters parameters;
    TransportClient client;
    FileWriter fileWriter;

    @Override
    public void initialize(Parameters parameters) {
        this.parameters = parameters;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", parameters.clusterName)
                .build();
        client = new TransportClient(settings);
        System.out.println("Connecting to: " + parameters.hosts);
        String[] hosts = parameters.hosts.split(",");

        for (String s : hosts)
            client.addTransportAddress(new InetSocketTransportAddress(s, parameters.port));

        try {
            fileWriter = new FileWriter(parameters.fileName);
        } catch (IOException exception) {
            System.out.print(exception.getMessage());
            throw new RuntimeException("Could not open file for writing!");
        }
    }

    @Override
    public void run() {
        if (parameters.logging) {
            System.out.printf(
                    "Starting search with fields: %s. Size: %d.",
                    parameters.fields,
                    parameters.size
            );
        }
        SearchResponse response = client.prepareSearch()
                                        .setQuery(QueryBuilders.matchAllQuery())
                                        .addFields(parameters.fields)
                                        .setSize(parameters.size)
                                        .execute()
                                        .actionGet();

        if (parameters.logging) {
            System.out.printf(
                    "Search took %s and returned %d results. Writing to file %s.%n",
                    response.getTook(),
                    response.getHits().hits().length,
                    parameters.fileName
            );
        }

        try {
            for(SearchHit hit : response.getHits())
                fileWriter.append(hit.getFields().toString());

            fileWriter.close();
        } catch (IOException exception) {
            System.out.print(exception.getMessage());
            throw new RuntimeException("Could not write to file!");
        }

    }
}
