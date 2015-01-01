package org.sports.hbaseparse;

import java.net.URL;
import java.text.SimpleDateFormat; 
import java.util.Date;
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
import org.sports.hbaseparse.interfaces.IHtmlValuesParser;
import org.sports.hbaseparse.parserUtils.BaseValuesParser;
import org.sports.hbaseparse.parserUtils.ContentParser;
import org.sports.hbaseparse.parserUtils.CustomDateTimeParser;
import org.sports.hbaseparse.repository.SolrUpdater;
import org.sports.hbaseparse.repository.SportalValuesParser;

public class Application {

	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException {
		final String serverFQDN = "localhost";
		final int commitCount = 250;

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
				scan.addColumn("f".getBytes(), "bas".getBytes());
				scan.setBatch(2000);
				scan.setCaching(2000);

				SolrUpdater solrUpd = new SolrUpdater(commitCount);
				ResultScanner scanner = hTable.getScanner(scan);
				Iterator<Result> resultsIter = scanner.iterator();
				while (resultsIter.hasNext()) {

					Result result = resultsIter.next();
					String key = Bytes.toString(result.getRow());
					String urlString = Bytes.toString(result.getValue("f".getBytes(),
							"bas".getBytes()));
					URL url = new URL(urlString);
					byte[] html = result.getValue("f".getBytes(),
							"cnt".getBytes());

					String pureHtml = new String(html, "UTF-8");
					IHtmlValuesParser valuesExtracter = new SportalValuesParser(
							pureHtml);

					Map<String, Object> parsedValues = valuesExtracter
							.parseHtml(pureHtml);

					if (parsedValues != null) {
						Map<String, Object> updValues = new HashMap<String, Object>();
						updValues.putAll(parsedValues);
						updValues.put("url", url.toString());
						updValues.put("id", key);
						updValues.put("host", url.getHost());
						
						solrUpd.updateEntry(updValues);
					} else {
						System.out.println("Skipping " + url);
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