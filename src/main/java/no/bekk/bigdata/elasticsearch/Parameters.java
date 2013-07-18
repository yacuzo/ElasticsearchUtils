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
    public static final String DEFAULT_FILE_NAME = "response.csv";
    public static final int DEFAULT_REPEAT_COUNT = 1;
    public static final int DEFAULT_BULK_SIZE = 1000;
    public static final Class<? extends Util> DEFAULT_UTIL = Util.class;

    public String hosts = DEFAULT_HOSTS;
    public int port = DEFAULT_PORT;
    public boolean logging = DEFAULT_LOGGING;
    public String clusterName = DEFAULT_CLUSTER;
    public int size = DEFAULT_SIZE;
    public String fields = DEFAULT_FIELDS;
    public String fileName = DEFAULT_FILE_NAME;
    public int repeatCount = DEFAULT_REPEAT_COUNT;
    public int bulkSize = DEFAULT_BULK_SIZE;
    public Class<? extends Util> util = DEFAULT_UTIL;
    public String filter = "";
    public short setTo = -1;
}
