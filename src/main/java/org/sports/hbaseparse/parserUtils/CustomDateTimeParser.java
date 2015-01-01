package org.sports.hbaseparse.parserUtils;

 import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CustomDateTimeParser {

	private static int getMonth(String bgMonthString) {
		int result = -1;
		switch (bgMonthString.toLowerCase()) {
		case "януари":
			result =  1;
			break;
		case "февруари":
			result =  2;
			break;
		case "март":
			result =  3;
			break;
		case "април":
			result =  4;
			break;
		case "май":
			result =  5;
			break;
		case "юни":
			result =  6;
			break;
		case "юли":
			result =  7;
			break;
		case "август":
			result =  8;
			break;
		case "септември":
			result =  9;
			break;
		case "октомври":
			result =  10;
			break;
		case "ноември":
			result =  11;
			break;
		case "декември":
			result =  12;
			break;
		default:
			throw new IllegalArgumentException("Invalid month string.");
		}
		
		return result;
	}

	/**
	 * Transforms string encoded date into DateTime format
	 * 
	 * @param input
	 *            String encoded date in format dd MonthName YYYY | HH:MM
	 * @return Parsed date and time in proper format
	 */
	public static Date parse(String input) {
		if (input == "") {
			return null;
		}

		String regex = "(\\d+)\\s+(.*)\\s+(\\d+)[|\\s]+(\\d+):(\\d+)";
		Pattern pattern = Pattern.compile(regex);

		Matcher m = pattern.matcher(input);

		if (m.matches()) {
			int date = Integer.parseInt(m.group(1));
			int month = getMonth(m.group(2)) - 1;
			int year = Integer.parseInt(m.group(3));
			int hourOfDay = Integer.parseInt(m.group(4));
			int minute = Integer.parseInt(m.group(5));
			Calendar cal = Calendar.getInstance();
			cal.set(year, month, date, hourOfDay, minute, 0);

			return cal.getTime();
		} else {
			throw new IllegalArgumentException(
					"Not valid date string format. Must be dd MonthName YYYY | HH:MM.");
		}
	}
}
