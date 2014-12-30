package org.sports.hbaseparse.repository;

import java.io.IOException; 
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrInputDocument;

public class SolrUpdater {
	final String urlSolr = "http://localhost:8983/solr/sports";

	private Collection<SolrInputDocument> documents;
	SolrServer server;
	
	public SolrUpdater() {
		this.documents = new ArrayList<SolrInputDocument>();
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
	
	public void commitDocuments() throws SolrServerException, IOException {
		int docCount = documents.size();
		if (docCount == 0) {
			return;
		}
		
		server.add(documents);
		server.commit();
		
		documents.clear();
		
		System.out.println("Commited " + docCount + " documents");
	}

	public void updateEntry(Map<String, Object> values)
			throws SolrServerException, IOException {

		SolrInputDocument doc = new SolrInputDocument();

		for (Entry<String, Object> entry : values.entrySet()) {
			doc.addField(entry.getKey(), entry.getValue());
		}
		
		this.documents.add(doc);

		if (documents.size() % 250 == 0) {
			this.commitDocuments();
		}
	}

}
