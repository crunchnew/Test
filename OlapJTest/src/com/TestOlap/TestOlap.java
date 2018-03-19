/**
 * Info about this package doing something for package-info.java file.
 */
package com.TestOlap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.OlapWrapper;
import org.olap4j.Position;
import org.olap4j.metadata.Member;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


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

	    int connectLnz =  4;

	    OlapConnection olapConnection;
	    OlapStatement statement = null ;
	    CellSet cellSet = null;
	    
	    long startTime = System.currentTimeMillis();
	    long endTime = 0;
	    long endTime_2 = 0;

      Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");

      Connection connection_lnz ;
      Connection connection ;
      ComboPooledDataSource cpds;
      HikariConfig config ;
	      
	   try {
	     
	     switch (connectLnz) {
	       case 1:
	         
	         connection_lnz = DriverManager.getConnection(
	             "jdbc:xmla:Server=http://lnzvt-sql0008.metals-automation.net/TPQC_Dev/msmdpump.dll;Catalog=AS_BIDWStar_auto_db",
	             "CubeReader",
	             "CubeR1!");
	         
	          olapConnection = connection_lnz.unwrap(OlapConnection.class);
	          statement = olapConnection.createStatement();
	          cellSet = statement.executeOlapQuery(str_lnz);        
	       break;

	       case 2:
	         
	         connection = DriverManager.getConnection(
	             "jdbc:xmla:Server=http://localhost/OLAP2/msmdpump.dll;Catalog=AdventureWorks SSAS",
	             "OLAP",
	             "0815Pwd!");
	         
	          olapConnection = connection.unwrap(OlapConnection.class);	          
	          statement = olapConnection.createStatement();
	          cellSet = statement.executeOlapQuery(str); 
	       break;
        
	       case 3:

	         cpds = new ComboPooledDataSource();
	         cpds.setDriverClass("org.olap4j.driver.xmla.XmlaOlap4jDriver"); //loads the jdbc driver
	         
	         cpds.setJdbcUrl("jdbc:xmla:Server=http://localhost/OLAP2/msmdpump.dll;Catalog=AdventureWorks SSAS");
	         cpds.setUser("OLAP");
	         cpds.setPassword("0815Pwd!");
	         
	         cpds.setMinPoolSize(1);
	         cpds.setAcquireIncrement(2);
	         cpds.setMaxPoolSize(7);

	         connection = cpds.getConnection();
	         OlapWrapper wrapper = (OlapWrapper) connection;
	         olapConnection = wrapper.unwrap(OlapConnection.class);
	         statement = olapConnection.createStatement();
	         cellSet = statement.executeOlapQuery(str);
	       break;
	       
	       case 4:
	         config = new HikariConfig();
	         config.setJdbcUrl("jdbc:xmla:Server=http://localhost/OLAP2/msmdpump.dll;Catalog=AdventureWorks SSAS");
           config.setUsername("OLAP");
           config.setPassword("0815Pwd!");

           config.setMinimumIdle(2);
           config.setMaximumPoolSize(3);
           config.setAutoCommit(false);
           
           DataSource dataSource = new HikariDataSource(config);
           connection = dataSource.getConnection();
          
           olapConnection = connection.unwrap(OlapConnection.class);
           statement = olapConnection.createStatement();
           cellSet = statement.executeOlapQuery(str);
           
           config.addDataSourceProperty("cachePrepStmts", "true");
           config.addDataSourceProperty("prepStmtCacheSize", "250");
           config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
	       break;
	       
	       
	       case 5:
	         GenericObjectPool connectionPool =
	         new GenericObjectPool(null);
	     ConnectionFactory connectionFactory =
	         new DriverManagerConnectionFactory(
	             "jdbc:xmla:Server=http://localhost/OLAP2/msmdpump.dll;Catalog=AdventureWorks SSAS"
	             ,new Properties());
	     PoolableConnectionFactory poolableConnectionFactory =
	         new PoolableConnectionFactory(
	             connectionFactory, connectionPool, null, null, false, true);
	     DataSource dataSource1 =
	         new PoolingDataSource(connectionPool);


	     connection = dataSource1.getConnection();
	     olapConnection = connection.unwrap(OlapConnection.class);
	     statement = olapConnection.createStatement();
	     cellSet = statement.executeOlapQuery(str);
	       break;
	       
	       default:
         break;
      }
	     	      
	    endTime = System.currentTimeMillis() - startTime;
	    startTime = System.currentTimeMillis();
	      
if (2 == 2) {  
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
}		  
	    

	   
	   } catch (Exception e) {
		  		System.out.println(e.getMessage());
	    }
	    
      endTime_2 = System.currentTimeMillis() - startTime;
      System.out.println(" FIRST [" + endTime + "] SECOND [" + endTime_2 + "]");
      
      
	  }
}


