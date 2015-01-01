package org.sports.hbaseparse.interfaces;

import java.util.Map;

public interface IHtmlValuesParser {
	public Map<String, Object> parseHtml(String pureHtml);
}
