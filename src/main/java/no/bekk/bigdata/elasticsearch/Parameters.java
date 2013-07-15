package no.bekk.bigdata.elasticsearch;

/**
 * Created with IntelliJ IDEA.
 * User: andre b. amundsen
 * Date: 15/07-13
 * Time: 09:55
 * To change this template use File | Settings | File Templates.
 */
public class Parameters {
    public static final String DEFAULT_HOSTS = "localhost";
    public static final int DEFAULT_PORT = 9300;
    public static final boolean DEFAULT_LOGGING = true;
    public static final String DEFAULT_CLUSTER = "TransactionCluster";
    public static final int DEFAULT_SIZE = 50;
    public static final String DEFAULT_FIELDS = "accountNumber";
    public static final String DEFAULT_FILE_NAME = "response.txt";

    public static String hosts = DEFAULT_HOSTS;
    public static int port = DEFAULT_PORT;
    public static boolean logging = DEFAULT_LOGGING;
    public static String clusterName = DEFAULT_CLUSTER;
    public static int size = DEFAULT_SIZE;
    public static String fields = DEFAULT_FIELDS;
    public static String fileName = DEFAULT_FILE_NAME;
}
