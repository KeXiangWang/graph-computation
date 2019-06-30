package ed.inf.grape.util;

import ed.inf.grape.graph.Edge;

public class KV {

	/** coordinator service name */
	public static final String COORDINATOR_SERVICE_NAME = "grape-coordinator";

	/** coordinator RMI service port */
	public static int RMI_PORT = 1099;

	public static int MAX_THREAD_LIMITATION = Integer.MAX_VALUE;

	public static String GRAPH_FILE_PATH_PREFIX = null;
	public static int PARTITION_COUNT = -1;
	public static String OUTPUT_DIR = null;
	public static String RESULT_DIR = null;
	public static String FREQUENT_EDGE = null;

	public static boolean ENABLE_COORDINATOR = false;
	public static boolean ENABLE_ASSEMBLE = false;
	public static boolean ENABLE_SYNC = false;
	public static boolean ENABLE_LOCAL_BATCH = false;
	public static boolean ENABLE_LOCAL_INCREMENTAL = false;
	public static boolean ENABLE_LOCAL_MESSAGE = false;

	public static boolean ENABLE_OPT;
	public static boolean ENABLE_FILTERX;

	public static int PARAMETER_K;
	public static int PARAMETER_B;
	public static int PARAMETER_N;
	public static int PARAMETER_ETA;
	public static double PARAMETER_LAMBDA;

	public static int QUERY_X_LABEL;
	public static int QUERY_X_FILTER;
	public static int QUERY_Y_LABEL;
	public static int EDGE_X_Y_ATTRIBUTE;

	public static int PERSON_LABEL;
	public static int EXPEND_WIDTH;

	public static int LEAST_MESSAGE;

	/** load constant from properties file */
	static {
		try {
			RMI_PORT = Config.getInstance().getIntProperty("RMI_PORT");

			MAX_THREAD_LIMITATION = Config.getInstance().getIntProperty(
					"THREAD_LIMIT_ON_EACH_MACHINE");

			GRAPH_FILE_PATH_PREFIX = Config.getInstance().getStringProperty(
					"GRAPH_FILE_PATH_PREFIX");

			OUTPUT_DIR = Config.getInstance().getStringProperty("OUTPUT_DIR");

			PARTITION_COUNT = Config.getInstance().getIntProperty("PARTITION_COUNT");

			ENABLE_COORDINATOR = Config.getInstance().getBooleanProperty("ENABLE_COORDINATOR");

			ENABLE_ASSEMBLE = Config.getInstance().getBooleanProperty("ENABLE_ASSEMBLE");

			ENABLE_SYNC = Config.getInstance().getBooleanProperty("ENABLE_SYNC");

			ENABLE_LOCAL_BATCH = Config.getInstance().getBooleanProperty("ENABLE_LOCAL_BATCH");

			ENABLE_LOCAL_INCREMENTAL = Config.getInstance().getBooleanProperty(
					"ENABLE_LOCAL_INCREMENTAL");

			ENABLE_LOCAL_MESSAGE = Config.getInstance().getBooleanProperty("ENABLE_LOCAL_MESSAGE");

			QUERY_X_LABEL = Config.getInstance().getIntProperty("QUERY_X_LABEL");

			QUERY_X_FILTER = Config.getInstance().getIntProperty("QUERY_X_FILTER");

			QUERY_Y_LABEL = Config.getInstance().getIntProperty("QUERY_Y_LABEL");

			EDGE_X_Y_ATTRIBUTE = Config.getInstance().getIntProperty("EDGE_X_Y_ATTRIBUTE");

			PERSON_LABEL = Config.getInstance().getIntProperty("PERSON_LABEL");

			PARAMETER_B = Config.getInstance().getIntProperty("B");
			PARAMETER_K = Config.getInstance().getIntProperty("K");
			PARAMETER_N = Config.getInstance().getIntProperty("N");
			PARAMETER_ETA = Config.getInstance().getIntProperty("ETA");
			PARAMETER_LAMBDA = Config.getInstance().getDoubleProperty("LAMBDA");

			ENABLE_OPT = Config.getInstance().getBooleanProperty("OPT");
			ENABLE_FILTERX = Config.getInstance().getBooleanProperty("FILTER");
			EXPEND_WIDTH = Config.getInstance().getIntProperty("WIDTH");

			LEAST_MESSAGE = Config.getInstance().getIntProperty("LEAST_MESSAGE");
			FREQUENT_EDGE = Config.getInstance().getStringProperty("GRAPH_FREQUENT_FILE");

			// TODO:validate configuration, some combination are not valid.

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
