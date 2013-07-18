package no.bekk.bigdata.elasticsearch;

import java.io.IOException;

import static no.bekk.bigdata.elasticsearch.Parameters.*;
/**
 * Created with IntelliJ IDEA.
 * User: andre b. amundsen
 * Date: 15/07-13
 * Time: 09:50
 * To change this template use File | Settings | File Templates.
 */
public class Main {

    private static void printHelp() {
        System.out.println("Possible parameters:");
        System.out.println(" --util             --- Which util to run. values: csv, ubq. Defaults to " + DEFAULT_UTIL);
        System.out.println(" --filter           --- String to filter on for UpdateByQuery. No default!");
        System.out.println(" --bulksize         --- Set the size of bulk requests. Defaults to " + DEFAULT_BULK_SIZE);
        System.out.println(" --logging=off        --- turn off logging to console");
        System.out.println(" --hosts=URL         ---- URL to hosts(comma separated). Defaults to " + DEFAULT_HOSTS);
        System.out.println(" --clustername=name         --- Name of elasticsearch cluster. Defaults to " + DEFAULT_CLUSTER);
        System.out.println(" --size=int         --- Number of requested hits. Defaults to " + DEFAULT_SIZE );
        System.out.println(" --fields=fields         --- Comma separated field names to request. Defaults to " + DEFAULT_FIELDS );
        System.out.println(" --repeatcount=int         --- Number of sources per CSV line. Defaults to " + DEFAULT_REPEAT_COUNT );
    }

    public static void main(String args[]) throws IOException, IllegalAccessException, InstantiationException {
        System.out.println("Usage: start with --help to get parameter list");
        Parameters parameters = new Parameters();
        for (String param : args) {
            String value = param.contains("=") ? param.split("=")[1] : "";


            if(param.startsWith("--util")) {
                switch(value.trim()) {
                    case "csv":
                        parameters.util = FieldsToCSV.class;
                        break;
                    case "ubq":
                        parameters.util = UpdateByQuery.class;
                        break;
                }
            }

            if (param.startsWith("--help")) {
                printHelp();
                return;
            }

            if (param.startsWith("--logging")) {
                if ("off".equals(value)) {
                    parameters.logging = false;
                }
            }

            if (param.startsWith("--hosts")) {
                parameters.hosts = value.trim();
            }

            if (param.startsWith("--clustername")) {
                parameters.clusterName = value.trim();
            }

            if (param.startsWith("--size")) {
                parameters.size = Integer.parseInt(value.trim());
            }

            if (param.startsWith("--repeatCount")) {
                parameters.repeatCount = Integer.parseInt(value.trim());
            }

            if (param.startsWith("--fields")) {
                parameters.fields = value.trim();
            }

            if (param.startsWith("--bulksize")) {
                parameters.bulkSize = Integer.parseInt(value.trim());
            }

            if (param.startsWith("--filter")) {
                parameters.filter = value.trim();
            }

            if (param.startsWith("--value")) {
                parameters.setTo = Short.parseShort(value.trim());
            }

        }

        Util util = parameters.util.newInstance();
        util.initialize(parameters);
        util.run();
    }
}
