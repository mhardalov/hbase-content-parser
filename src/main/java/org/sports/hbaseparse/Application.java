package org.sports.hbaseparse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.sports.hbaseparse.parserUtils.ContentParser;
import org.sports.hbaseparse.repository.SolrUpdater;

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

				SolrUpdater solrUpd = new SolrUpdater();
				ResultScanner scanner = hTable.getScanner(scan);
				Iterator<Result> resultsIter = scanner.iterator();
				while (resultsIter.hasNext()) {

					Result result = resultsIter.next();
					String key = Bytes.toString(result.getRow());

					List<KeyValue> values = result.list();
					for (KeyValue value : values) {

						String pureHtml = new String(value.getValue(), "UTF-8");

						ContentParser cp = new ContentParser(pureHtml);
						String category = cp
								.getCategory("h1.printing_large_text_toolbar > strong");
						String content = cp.getContent("div#news_content");

						Map<String, Object> updValues = new HashMap<String, Object>();
						updValues.put("id", key);
						updValues.put("content", content);
						updValues.put("category", category);
						
						solrUpd.updateEntry(updValues);
						break;
					}
				}
				
				solrUpd.commitDocuments();
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