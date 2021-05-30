package com.stackroute.datamunger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*There are total 5 DataMungertest files:
 * 
 * 1)DataMungerTestTask1.java file is for testing following 3 methods
 * a)getSplitStrings()  b) getFileName()  c) getBaseQuery()
 * 
 * Once you implement the above 3 methods,run DataMungerTestTask1.java
 * 
 * 2)DataMungerTestTask2.java file is for testing following 3 methods
 * a)getFields() b) getConditionsPartQuery() c) getConditions()
 * 
 * Once you implement the above 3 methods,run DataMungerTestTask2.java
 * 
 * 3)DataMungerTestTask3.java file is for testing following 2 methods
 * a)getLogicalOperators() b) getOrderByFields()
 * 
 * Once you implement the above 2 methods,run DataMungerTestTask3.java
 * 
 * 4)DataMungerTestTask4.java file is for testing following 2 methods
 * a)getGroupByFields()  b) getAggregateFunctions()
 * 
 * Once you implement the above 2 methods,run DataMungerTestTask4.java
 * 
 * Once you implement all the methods run DataMungerTest.java.This test case consist of all
 * the test cases together.
 */

public class DataMunger {

	/*
	 * This method will split the query string based on space into an array of words
	 * and display it on console
	 */

	public String[] getSplitStrings(String queryString) {
		return queryString.toLowerCase().split(" ");
	}

	/*
	 * Extract the name of the file from the query. File name can be found after a
	 * space after "from" clause. Note: ----- CSV file can contain a field that
	 * contains from as a part of the column name. For eg: from_date,from_hrs etc.
	 * 
	 * Please consider this while extracting the file name in this method.
	 */

	public String getFileName(String queryString) {
		String subString="";
		String fileName="";
		int fromIndex = queryString.indexOf(" from ")+1;
		//substring starting from 'from' 
		subString = queryString.substring(fromIndex);
		if(subString.indexOf(" ")>0) {
			//substring starting from next word of 'from'
			subString = subString.substring(subString.indexOf(" ")+1);
		}
		if(subString.indexOf(" ")>0) {//contains other clause after from clause
			fileName = subString.substring(0, subString.indexOf(" "));
		}else {
			fileName = subString;
		}
		return fileName;
	}

	/*
	 * This method is used to extract the baseQuery from the query string. BaseQuery
	 * contains from the beginning of the query till the where clause
	 * 
	 * Note: ------- 1. The query might not contain where clause but contain order
	 * by or group by clause 2. The query might not contain where, order by or group
	 * by clause 3. The query might not contain where, but can contain both group by
	 * and order by clause
	 */
	
	public String getBaseQuery(String queryString) {
		String baseQuery="";
		int endIndex;
		if(queryString.contains(" where ")) {
			endIndex = queryString.indexOf(" where ");
		}else {
			if(queryString.contains(" group by ")) {
				endIndex = queryString.indexOf(" group by ");
			}else if(queryString.contains(" order by ")){
				endIndex = queryString.indexOf(" order by ");
			}else {
				endIndex= -1;
			}
		}
		if(endIndex == -1) {
			baseQuery = queryString;
		}else {
			baseQuery = queryString.substring(0,endIndex);
		}
		return baseQuery;
	}

	/*
	 * This method will extract the fields to be selected from the query string. The
	 * query string can have multiple fields separated by comma. The extracted
	 * fields will be stored in a String array which is to be printed in console as
	 * well as to be returned by the method
	 * 
	 * Note: 1. The field name or value in the condition can contain keywords
	 * as a substring. For eg: from_city,job_order_no,group_no etc. 2. The field
	 * name can contain '*'
	 * 
	 */
	
	public String[] getFields(String queryString) {
		String fieldsPart = queryString.substring(7, queryString.indexOf(" from "));
		return fieldsPart.split(",");
	}

	/*
	 * This method is used to extract the conditions part from the query string. The
	 * conditions part contains starting from where keyword till the next keyword,
	 * which is either group by or order by clause. In case of absence of both group
	 * by and order by clause, it will contain till the end of the query string.
	 * Note:  1. The field name or value in the condition can contain keywords
	 * as a substring. For eg: from_city,job_order_no,group_no etc. 2. The query
	 * might not contain where clause at all.
	 */
	
	public String getConditionsPartQuery(String queryString) {
		queryString = queryString.toLowerCase();
		String subString="";
		String conditionClause = null;
		int endIndex;
		if(queryString.contains(" where ")) {
			subString = queryString.substring(queryString.indexOf(" where ")+7);
			if(subString.contains(" group by ")) {
				endIndex = subString.indexOf(" group by ");
			}else if(subString.contains(" order by ")){
				endIndex = subString.indexOf(" order by ");
			}else {
				endIndex= -1;
			}
			if(endIndex == -1) {
				conditionClause = subString;
			}else {
				conditionClause =subString.substring(0, endIndex);
			}
		}
		return conditionClause;
	}

	/*
	 * This method will extract condition(s) from the query string. The query can
	 * contain one or multiple conditions. In case of multiple conditions, the
	 * conditions will be separated by AND/OR keywords. for eg: Input: select
	 * city,winner,player_match from ipl.csv where season > 2014 and city
	 * ='Bangalore'
	 * 
	 * This method will return a string array ["season > 2014","city ='bangalore'"]
	 * and print the array
	 * 
	 * Note: ----- 1. The field name or value in the condition can contain keywords
	 * as a substring. For eg: from_city,job_order_no,group_no etc. 2. The query
	 * might not contain where clause at all.
	 */

	public String[] getConditions(String queryString) {
		String conditional_clause = getConditionsPartQuery(queryString);
		String[] splitUsingAND = null; 
		String[] splitUsingOR = null;
		String result[][] = new String[2][];
		String[] conditions = null;
		int conitionCounter = 0;
		int totalConditions =0;
		if(conditional_clause!=null && !conditional_clause.equals("")) {
			if(conditional_clause.contains(" and ")) {
				splitUsingAND = conditional_clause.split(" and ");
			}else {
				splitUsingAND = new String[1];
				splitUsingAND[0] = conditional_clause;
			}
			for(int i=0;i<splitUsingAND.length;i++) {
				if(splitUsingAND[i].contains(" or ")) {
					splitUsingOR = null;
					splitUsingOR = splitUsingAND[i].split(" or ");
				}else {
					splitUsingOR = new String[1];
					splitUsingOR[0] = splitUsingAND[i];
				}
				result[i] = splitUsingOR;
				totalConditions = totalConditions+result[i].length;
			}
			conditions = new String[totalConditions];
			
			for(int i=0;i<result.length;i++) {
				if(result[i]!=null) {
					for(int j=0;j<result[i].length;j++) {
						conditions[conitionCounter++] = result[i][j];
					}
				}
			}
		}
		return conditions;
	}
	/*
	 * This method will extract logical operators(AND/OR) from the query string. The
	 * extracted logical operators will be stored in a String array which will be
	 * returned by the method and the same will be printed Note:  1. AND/OR
	 * keyword will exist in the query only if where conditions exists and it
	 * contains multiple conditions. 2. AND/OR can exist as a substring in the
	 * conditions as well. For eg: name='Alexander',color='Red' etc. Please consider
	 * these as well when extracting the logical operators.
	 * 
	 */

	public String[] getLogicalOperators(String queryString) {
		String conditionsClause = getConditionsPartQuery(queryString);
		boolean isAND = false;
		boolean isOR = false;
		String[] conditionalOperator = null;
		if(conditionsClause!=null && !conditionsClause.equals("")) {
			if(conditionsClause.contains(" and ")){
				isAND = true;
			} 
			if(conditionsClause.contains(" or ")){
				isOR = true;
			}
		}
		if(isAND && isOR ) {
			conditionalOperator = new String[2];
			conditionalOperator[0]="and";
			conditionalOperator[1]="or";
		}else if(isAND){
			conditionalOperator = new String[1];
			conditionalOperator[0]="and";
		}else if(isOR) {
			conditionalOperator = new String[1];
			conditionalOperator[0]="or";
		}
		return conditionalOperator;
	}

	/*
	 * This method extracts the order by fields from the query string. Note: 
	 * 1. The query string can contain more than one order by fields. 2. The query
	 * string might not contain order by clause at all. 3. The field names,condition
	 * values might contain "order" as a substring. For eg:order_number,job_order
	 * Consider this while extracting the order by fields
	 */

	public String[] getOrderByFields(String queryString) {
		String[] orderByFields = null;
		String orderByClause ="";
		if(queryString.contains(" order by ")) {
			//get String after order by keyword
			orderByClause = queryString.substring(queryString.indexOf(" order by ")+10);
			orderByFields = orderByClause.split(",");
		}
		return orderByFields;
	}

	/*
	 * This method extracts the group by fields from the query string. Note:
	 * 1. The query string can contain more than one group by fields. 2. The query
	 * string might not contain group by clause at all. 3. The field names,condition
	 * values might contain "group" as a substring. For eg: newsgroup_name
	 * 
	 * Consider this while extracting the group by fields
	 */

	public String[] getGroupByFields(String queryString) {
		String groupByClause ="";
		String[] groupByFields = null;
		int endIndex;
		if(queryString.contains(" group by ")) {
			//get String after group by keyword
			groupByClause = queryString.substring(queryString.indexOf(" group by ")+10);
			//if also have order by clause
			if(groupByClause.contains(" order by ")) {
				endIndex = groupByClause.indexOf(" order by ");
				groupByClause = groupByClause.substring(0, endIndex);
			}
			groupByFields = groupByClause.split(",");
		}
		return groupByFields;
	}

	/*
	 * This method extracts the aggregate functions from the query string. Note:
	 *  1. aggregate functions will start with "sum"/"count"/"min"/"max"/"avg"
	 * followed by "(" 2. The field names might
	 * contain"sum"/"count"/"min"/"max"/"avg" as a substring. For eg:
	 * account_number,consumed_qty,nominee_name
	 * 
	 * Consider this while extracting the aggregate functions
	 */

	public String[] getAggregateFunctions(String queryString) {
		String[] fields = getFields(queryString);
		
		int aggrFunctionCounter = 0;
		String [] aggregateFunctions= null;
		if(!(fields[0].contains("*"))) {
			for(int i=0;i<fields.length;i++) {
				Matcher matcher = Pattern.compile("\\(([^)]+)\\)").matcher(fields[i]);
				if(matcher.find()) { //checking for aggregate functions
					aggrFunctionCounter++; // counting number of aggregate functions
				}
			}
			aggregateFunctions = new String[aggrFunctionCounter];
			aggrFunctionCounter = 0;
			for(int i=0;i<fields.length;i++) {
				Matcher matcher = Pattern.compile("\\(([^)]+)\\)").matcher(fields[i]);
				if(matcher.find()) { 
					aggregateFunctions[aggrFunctionCounter++] = fields[i];
				}
			}
		}
		return aggregateFunctions;
	}

}