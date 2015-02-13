package org.sports.hbaseparse.repository;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.sports.gate.GateSportsApplication;
import org.sports.ontology.model.DocumentModel;

public class GateAnnotationsBuilder {

	private final String urlSolr = "http://localhost:8983/solr/sports";

	List<DocumentModel> documents;
	private SolrServer server;

	public GateAnnotationsBuilder() {
		this.documents = new ArrayList<DocumentModel>();
		this.server = this.getSolrServer();
	}

	private SolrServer getSolrServer() {
		HttpSolrServer server = new HttpSolrServer(urlSolr);
		server.setMaxRetries(1); // defaults to 0. > 1 not recommended.
		server.setConnectionTimeout(5000); // 5 seconds to establish TCP
		// Setting the XML response parser is only required for cross
		// version compatibility and only when one side is 1.4.1 or
		// earlier and the other side is 3.1 or later.
		server.setParser(new XMLResponseParser()); // binary parser is used by
													// default
		// The following settings are provided here for completeness.
		// They will not normally be required, and should only be used
		// after consulting javadocs to know whether they are truly required.
		server.setSoTimeout(5000); // socket read timeout
		server.setDefaultMaxConnectionsPerHost(100);
		server.setMaxTotalConnections(100);
		server.setFollowRedirects(false); // defaults to false
		// allowCompression defaults to false.
		// Server side must support gzip or deflate for this to have any effect.
		server.setAllowCompression(true);

		return server;
	}

	private void addDocument(SolrDocument doc) throws ParseException,
			Exception {
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		df.setTimeZone(tz);
		Date docDate = df.parse(doc.getFieldValue("tstamp").toString());

		String url = doc.getFieldValue("url").toString();		
		String content = doc.getFieldValue("content").toString();		
		
		DocumentModel docModel = new DocumentModel();
		docModel.setKey(url);
		docModel.setUrl(url);
		docModel.setContent(content);
		docModel.setDate(docDate);

		documents.add(docModel);
	}

	private void annotate() throws Exception {		
		GateSportsApplication.annotate(documents);
		documents.clear();
	}

	public long getCount() throws SolrServerException {		
		SolrQuery solrQuery = new SolrQuery();

		solrQuery.setQuery("*:*");
		solrQuery.set("fl", "content,url,tstamp");
		QueryResponse rsp = this.server.query(solrQuery);

		SolrDocumentList docs = rsp.getResults();
		return docs.getNumFound();
	}

	public void query(int commitCount) throws ParseException, Exception {
		long maxCount = this.getCount();		
		SolrQuery solrQuery;
		int round = 0;
		int start;

		do {
			solrQuery = new SolrQuery();

			solrQuery.setQuery("*:*");
			solrQuery.set("fl", "content,url,tstamp,id");
			start = round * commitCount;

			// Avoiding paging with invalid values < 1
			solrQuery.set("start", start);

			solrQuery.set("rows", commitCount);
			QueryResponse rsp = this.server.query(solrQuery);
			SolrDocumentList docs = rsp.getResults();

			for (SolrDocument doc : docs) {
				this.addDocument(doc);
			}
			
			this.annotate();
			
			round++;
			System.out.printf("Annoteted %d documents.\n", round * commitCount);
			
			
		} while (maxCount > start);
	}

}
