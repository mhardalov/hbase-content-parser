package org.sports.hbaseparse.parserUtils;

import java.util.HashMap;
import java.util.Map;

import org.sports.hbaseparse.interfaces.IHtmlValuesParser;

public abstract class BaseValuesParser implements IHtmlValuesParser {
	protected Map<String, Object> resultMap;
	protected ContentParser contentParser;

	public BaseValuesParser(String pureHtml) {
		this.contentParser = new ContentParser(pureHtml);
		this.resultMap = new HashMap<String, Object>();
	}
}
