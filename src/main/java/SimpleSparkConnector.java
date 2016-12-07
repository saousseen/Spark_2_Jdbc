
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BinaryType;

import scala.reflect.ClassManifestFactory$;

public class SimpleSparkConnector {

	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/testBlob";
	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "root";

	private static final SparkSession sparkSession = SparkSession.builder().master("local").appName("test")
			.getOrCreate();

	public static void main(String[] args) {

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", MYSQL_USERNAME);
		// connectionProperties.put("password", MYSQL_PWD);

		final String req = "(select file from Test where name = 'name1') as test_alias ";

		Dataset<Row> jdbcDF = sparkSession.read().jdbc(MYSQL_CONNECTION_URL, req, "file", 1, 2, 10,
				connectionProperties);

		// display the schema of the return rows
		jdbcDF.printSchema();

		List<Row> testRows = jdbcDF.collectAsList();

		for (Row r : testRows) {

			System.out.println(" total columns per row " + r.length());

			byte[] fileContent = (byte[]) r.get(0);

			// display the content of file by looping fileContent Array
//			for (int i = 0; i < fileContent.length; i++) {
//				System.out.print((char) fileContent[i]);
//			}

			System.out.println(r.get(0));
		}

	}

}
