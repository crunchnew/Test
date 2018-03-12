/**
 * Info about this package doing something for package-info.java file.
 */
package com.TestOlap;
/**
 * test
 */


import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DecimalFormat;

import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.metadata.Member;

/**
 * @author Z0064FP6
 *
 */

public class TestOlap {

  /**
   *@param value some decimal
   *@return value
    */
  public static String foo(final double value) {
    DecimalFormat formatter;

     if (value - (int) value > 0.0) {
      formatter = new DecimalFormat("0.00");
	 } else {
       formatter = new DecimalFormat("0");
     }

     return formatter.format(value);
  }


  /**
   *@param parentMember some decimal
   *@throws OlapException ble ble
   */
  public static void collectionOlap(final Member parentMember) throws OlapException {

    for (Member member : parentMember.getChildMembers()) {
      System.out.println("Child Row Member [" + member.getName() + "]");
    }

  }

  /**
   * @param args ble ble
   * @throws Exception ble ble
   * 
   */
  public static void main(String[] args) throws Exception {

	    String str;
	    String str_lnz;
	    Boolean connectLnz =  true;

	    OlapConnection olapConnection;
	    OlapStatement statement ;
	    CellSet cellSet;
	    
	    str = "SELECT {[Measures].[Internet Order Quantity],[Measures].[Sales Amount Forecast]} DIMENSION PROPERTIES PARENT_UNIQUE_NAME,"
		  		+ "HIERARCHY_UNIQUE_NAME ON COLUMNS , NON EMPTY CrossJoin(Hierarchize({[Employee].[Employees].[CEO].AllMembers}),"
		  		+ " Hierarchize(DrilldownMember({{DrilldownLevel({[Product].[Product by Category].[All]},,,INCLUDE_CALC_MEMBERS)}},"
		  		+ " {[Product].[Product by Category].[Category].&[1],[Product].[Product by Category].[Category].&[2],[Product]."
		  		+ "[Product by Category].[Category].&[3],[Product].[Product by Category].[Category].&[4]},,,INCLUDE_CALC_MEMBERS))) "
		  		+ "DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME,[Product].[Product by Category].[Subcategory]."
		  		+ "[Category],[Employee].[Employees].[Employee Sort],[Employee].[Employees].[Employees] "
		  		+ "ON ROWS  FROM [AdventureWorks] CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS";

	    str_lnz = "SELECT {[Measures].[AlarmCode (AVG)],[Measures].[AlarmError Count],[Measures].[AlarmOK Count],[Measures].[AlarmUndefined Count]}"
	    		+ " DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS , "
	    		+ "NON EMPTY Hierarchize({DrilldownLevel({[Dim MaterialWeight].[MaterialWeight Hierarchy].[All]},,,INCLUDE_CALC_MEMBERS)}) "
	    		+ "DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS  FROM [AS_BIDWStar_auto_cube] CELL PROPERTIES VALUE, "
	    		+ "FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS" ;

	    
	    str_lnz = " SELECT {" +
	      "[Measures].[KPI_CCM_10_SlabInternalQuality [%]]], " +
	      "[Measures].[KPI_CCM_10_SlabInternalQuality [-]] (CNT)], " +
	      "[Measures].[KPI_CCM_12_SlabOverallQuality [%]]], " +
	      "[Measures].[KPI_CCM_12_SlabOverallQuality [-]] (CNT)]} " +  
	      "ON COLUMNS , " +
	      "{" + 
	      "[Dim Grade].[Grade Hierarchy].[All]" +
	      "} " +
	      "ON ROWS " +  
	      "FROM [AS_BIDWStar_auto_cube]" ;
	    
	    str_lnz = 
	        " SELECT NON EMPTY                                          " +                          
	            " {                                                     " +
	            "    [Measures].[KPI_CCM_06_Breakout [1/y]]]            " +
	            "  , [Measures].[KPI_CCM_10_SlabInternalQuality [%]]]   " +
	            " }                                                     " +
	            " ON 0,                                                 " +
	            " NON EMPTY                                             " +
	            " {                                                     " +
	            "   [Dim Grade].[Grade Hierarchy].[Grade].&[16MnAl],    " +
	            "   [Dim Grade].[Grade Hierarchy].[Grade].&[22MnB5]     " +
	            " }                                                     " +
	            "                                                       " +
	            " ON 1                                                  " +
	            " FROM [AS_BIDWStar_auto_cube]                          " +
	            " WHERE {                                               " +
	            "  [Dim Date].[Date Hierarchy].[Quarter].&[2017.Q1]     " +
	            " , [Dim Date].[Date Hierarchy].[Quarter].&[2017.Q2]    " +
	            "  }                                                    " ;
	    
	    
	    long startTime = System.currentTimeMillis();
	    long endTime = 0;
	    long endTime_2 = 0;

	      Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");

	      Connection connection_lnz = DriverManager.getConnection(
	          "jdbc:xmla:Server=http://lnzvt-sql0008.metals-automation.net/TPQC_Dev/msmdpump.dll;Catalog=AS_BIDWStar_auto_db",
	    		  "CubeReader",
	    		  "CubeR1!");

	      Connection connection = DriverManager.getConnection(
	          "jdbc:xmla:Server=http://localhost/OLAP/msmdpump.dll;Catalog=AdventureWorks SSAS",
	    		  "OLAP",
	    		  "0815Pwd!");

	   try {
	      if ( connectLnz ) {
	    	  
	    	  olapConnection = connection_lnz.unwrap(OlapConnection.class);
	    	  statement = olapConnection.createStatement();
	    	  cellSet = statement.executeOlapQuery(str_lnz);
	      }
	      else {  
	    	  olapConnection = connection.unwrap(OlapConnection.class);
	    	  
	    	  statement = olapConnection.createStatement();
	    	  cellSet = statement.executeOlapQuery(str); 
	      }
	      
	      endTime = System.currentTimeMillis() - startTime;
	      startTime = System.currentTimeMillis();
	      
		  for (Position row : cellSet.getAxes().get(1)) {
		    
		    for (Position column : cellSet.getAxes().get(0)) {

		      for (Member member : row.getMembers()) {
		  	  	System.out.println("Row Member " + member.getUniqueName());
		  	    System.out.println("Row Member [" + member.getName() + "]");
		  	  //System.out.println(member.);

		  	  if (member.getChildMemberCount() > 0) {
		  	    collectionOlap(member);
		  	  }
		  	}
		      
		  	for (Member member : column.getMembers()) {
		  	   System.out.println("Column Member" + member.getUniqueName());
		  	   System.out.println("Column Member [" + member.getName() + "]");
		  	}
		  	final Cell cell = cellSet.getCell(column, row);
		  	// System.out.println("Value ["+ cell.getForm attedValue()+ "]");
		  	// System.out.println("Value ["+ cell.getValue().toString() + "]");
		  	System.out.println("Value [" + foo(cell.getDoubleValue()) + "]");
	          System.out.println();
		    }
		  }
	    } catch (Exception e) {
		  		System.out.println(e.getMessage());
	    }
	    
      endTime_2 = System.currentTimeMillis() - startTime;
      System.out.println(" FIRST [" + endTime + "] SECOND [" + endTime_2 + "]");
	  }
}
