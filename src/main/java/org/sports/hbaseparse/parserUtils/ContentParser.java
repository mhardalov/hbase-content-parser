package org.sports.hbaseparse.parserUtils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class ContentParser {

	private Document doc;

	private String ClearHtml(String content) {
		//
		content = content.replaceAll("br2n", "\n");

		return content;
	}

	private String getElementContent(String expression) {
		Element element = doc.select(expression).first();
		String result = "";
		if (element != null)
			result = element.text();

		return result;
	}

	// //div[@id='news_content']
	public String getContent(String expression) {
		String content = this.getElementContent(expression);
		
		return this.ClearHtml(content);

	}

	public String getCategory(String expression) {
		String content = this.getElementContent(expression);
		return this.ClearHtml(content);

	}

	public ContentParser(String html) {
		html = html.replaceAll("(?i)<br[^>]*>", "br2n");
		this.doc = Jsoup.parse(html);
	}

}
