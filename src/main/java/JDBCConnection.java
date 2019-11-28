/**
 * @author tangd-a
 * @date 2019/11/28 17:27
 */
public class JDBCConnection {
	public static String igniteCacheName = "data";
	public static String igniteUrl = "jdbc:ignite:cfg://cache=" + igniteCacheName + "@file:///./config/example-save-to-ignite.xml";
	public static String igniteTableName = "data";
}