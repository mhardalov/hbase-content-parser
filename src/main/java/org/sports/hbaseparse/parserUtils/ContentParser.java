package org.sports.hbaseparse.parserUtils;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.xml.sax.SAXException;

public class ContentParser {
	
	private Document doc;
	
	private String ClearHtml(String content) {
		//
		content = content.replaceAll("br2n", "\n");
		
		return content;
	}
	
	////div[@id='news_content']
	public String getContent(String expression) {
		Element element = doc.select(expression).first();
		
		if (element == null)
			return "";
		
		return this.ClearHtml(element.text());
		
	}
	
	public ContentParser(String html) {
		html = html.replaceAll("(?i)<br[^>]*>", "br2n");
		this.doc = Jsoup.parse(html);
	}

}
