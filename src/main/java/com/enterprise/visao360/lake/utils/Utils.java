package com.enterprise.visao360.lake.utils;


import com.enterprise.visao360.lake.enums.TableDateFormatEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Utils {
	
	final static Logger LOGGER = Logger.getLogger(Utils.class);
	
	public static void main(String[] args) {
		String date = "20022018";
		
		try {
			System.out.println(dateFormart(date, TableDateFormatEnum.PEDT001, TableDateFormatEnum.FORMAT_INPUT));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param separator
	 * @param keys
	 * @return the String
	 */
	public static String generateKey(final String separator, String... keys) {
		String rowKey = "";
		for (String key : keys) 
			rowKey += key.concat(separator);
		
		return rowKey.substring(0, rowKey.length() - 1);
	}
	
	/**
	 * @param date
	 * @param format
	 * @param formatDefault
	 * @throws ParseException
	 * @return the String
	 */
	public static String dateFormart(final String date, 
									 final TableDateFormatEnum format, 
									 final TableDateFormatEnum formatInput) throws ParseException {
		
		String dateFormated = "";

		try {
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat(formatInput.getDescription());
			SimpleDateFormat sdfNew = new SimpleDateFormat(format.getDescription());
			cal.setTime(sdf.parse(date));
			
			dateFormated = sdfNew.format(cal.getTime());
			
		} catch (ParseException e) {
			LOGGER.error(e.getMessage(), e);
			throw new ParseException(e.getMessage(), 1);
		}
		
		return dateFormated;
	}
	
	/**
	 * @param numero
	 * @return the String
	 */
	public static String leftPad(final String numero) {
		
		if(StringUtils.isNotEmpty(numero)) {
			return String.format("%03d", Integer.parseInt(numero));
		}
		
		return "";
		
	}
}
