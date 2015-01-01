package org.sports.hbaseparse.repository;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.sports.hbaseparse.parserUtils.BaseValuesParser;
import org.sports.hbaseparse.parserUtils.CustomDateTimeParser;

public class SportalValuesParser extends BaseValuesParser {

	public SportalValuesParser(String pureHtml) {
		super(pureHtml);
	}

	@Override
	public Map<String, Object> parseHtml(String pureHtml) {

		String category = contentParser
				.getCategory("h1.printing_large_text_toolbar > strong");
		String content = contentParser.getContent("div#news_content");
		String title = contentParser.getContent("#news_heading > h1");
		String dateStr = contentParser
				.getContent("#news_heading > span.dark_text");

		// Cannot proceed when one of the values is null
		// The page isnot in the correct format
		if (dateStr == "" || content == "" || category == "" || title == "") {
			return null;
		}

		Date date = CustomDateTimeParser.parse(dateStr);
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		df.setTimeZone(tz);
		String dateAsISO = df.format(date);

		this.resultMap.put("title", title);
		this.resultMap.put("content", content);
		this.resultMap.put("category", category);
		this.resultMap.put("tstamp", dateAsISO);

		return this.resultMap;
	}

}
