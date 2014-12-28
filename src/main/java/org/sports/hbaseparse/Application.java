package org.sports.hbaseparse;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.sports.hbaseparse.parserUtils.ContentParser;

public class Application {

	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException {
		final String serverFQDN = "localhost";

		Configuration conf = HBaseConfiguration.create();

		conf.clear();
		conf.set("hbase.zookeeper.quorum", serverFQDN);
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		HBaseAdmin admin = new HBaseAdmin(conf);
		
		try {
			HTable hTable = new HTable(conf, "webpage");
			
			try {

				Scan scan = new Scan();
				scan.addColumn("f".getBytes(), "cnt".getBytes());
				scan.setBatch(2000);
				int count = 0;
				ResultScanner scanner = hTable.getScanner(scan);
				Iterator<Result> resultsIter = scanner.iterator();
				while (resultsIter.hasNext()) {

					Result result = resultsIter.next();

					List<KeyValue> values = result.list();
					for (KeyValue value : values) {
						System.out.println(value.getKey());
						System.out.println(new String(value.getQualifier()));
						String str = new String(value.getValue(), "UTF-8");
//						System.out.println(++count + "." + str);
						
						ContentParser cp = new ContentParser(str);
						System.out.println("-----------------------------------------------");
						System.out.println(cp.getContent("div#news_content"));
						System.out.println("-----------------------------------------------");
					}
				}
			} finally {
				if (hTable != null) {
					hTable.close();
				}
			}
		} catch (Exception ex) {
			System.out.println("Error caught.");
			ex.printStackTrace();
		}

		System.out.println("End.");
	}
}