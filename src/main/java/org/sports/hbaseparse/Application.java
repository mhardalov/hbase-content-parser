package org.sports.hbaseparse;

import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.sports.gate.GateSportsApplication;
import org.sports.hbaseparse.interfaces.IHtmlValuesParser;
import org.sports.hbaseparse.repository.SolrUpdater;
import org.sports.hbaseparse.repository.SportalValuesParser;
import org.sports.ontology.model.DocumentModel;

public class Application {

	private static List<DocumentModel> documents = new ArrayList<DocumentModel>();

	private static void addDocument(String key, URL url,
			Map<String, Object> parsedValues) throws ParseException, Exception {
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		df.setTimeZone(tz);
		Date docDate = df.parse(parsedValues.get("tstamp").toString());

		DocumentModel docModel = new DocumentModel();
		docModel.setKey(key);
		docModel.setUrl(url.toString());
		docModel.setContent(parsedValues.get("content").toString());
		docModel.setDate(docDate);

		documents.add(docModel);
	}

	private static void annotate() {
		try {
			GateSportsApplication.annotate(documents);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		documents.clear();
	}

	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException {
		final String serverFQDN = "localhost";
		final int commitCount = 250;

		Configuration conf = HBaseConfiguration.create();

		conf.clear();
		conf.set("hbase.zookeeper.quorum", serverFQDN);
		conf.set("hbase.zookeeper.property.clientPort", "2181");

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
					String urlString = Bytes.toString(result.getValue(
							"f".getBytes(), "bas".getBytes()));
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
						addDocument(key, url, updValues);

						if (solrUpd.doCommit()) {
							annotate();
						}
					} else {
						System.out.println("Skipping " + url);
					}
				}

				solrUpd.commitDocuments();
				annotate();
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