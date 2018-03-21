/**
 * Info about this package doing something for package-info.java file.
 */
package com.TestOlap;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.olap4j.Cell;
import org.olap4j.CellSet;
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

     if (value - (int) value > 0.0) { formatter = new DecimalFormat("0.00");
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

	     //rowery
	     str =
	         //"SELECT NON EMPTY CrossJoin(Hierarchize({DrilldownLevel({[Date].[Fiscal Date].[All]},,,INCLUDE_CALC_MEMBERS)}), {[Measures].[Internet Order Quantity],[Measures].[Internet Sales Amount]}) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS , NON EMPTY Hierarchize(DrilldownMember({{DrilldownMember({{DrilldownLevel({[Product].[Product by Category].[All]},,,INCLUDE_CALC_MEMBERS)}}, {[Product].[Product by Category].[Category].&[1],[Product].[Product by Category].[Category].&[3],[Product].[Product by Category].[Category].&[4]},,,INCLUDE_CALC_MEMBERS)}}, {[Product].[Product by Category].[Subcategory].&[1],[Product].[Product by Category].[Subcategory].&[26],[Product].[Product by Category].[Subcategory].&[27],[Product].[Product by Category].[Subcategory].&[28],[Product].[Product by Category].[Subcategory].&[29],[Product].[Product by Category].[Subcategory].&[30],[Product].[Product by Category].[Subcategory].&[31],[Product].[Product by Category].[Subcategory].&[32],[Product].[Product by Category].[Subcategory].&[37]},,,INCLUDE_CALC_MEMBERS)) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME,[Product].[Product by Category].[Subcategory].[Category],[Product].[Product by Category].[Product].[Subcategory] ON ROWS  FROM [AdventureWorks] CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"
	         //"SELECT NON EMPTY CrossJoin(CrossJoin(CrossJoin(CrossJoin(CrossJoin(CrossJoin(CrossJoin(Hierarchize({DrilldownLevel({[Date].[Fiscal Date].[All]},,,INCLUDE_CALC_MEMBERS)}), Hierarchize({DrilldownLevel({[Ship Date].[Calendar Date].[All]},,,INCLUDE_CALC_MEMBERS)})), Hierarchize({DrilldownLevel({[Ship Date].[Fiscal Date].[All]},,,INCLUDE_CALC_MEMBERS)})), Hierarchize({DrilldownLevel({[Date].[Calendar Date].[All]},,,INCLUDE_CALC_MEMBERS)})), Hierarchize({DrilldownLevel({[Ship Date].[Month].[All]},,,INCLUDE_CALC_MEMBERS)})), Hierarchize({DrilldownLevel({[Ship Date].[Calendar Quarter].[All]},,,INCLUDE_CALC_MEMBERS)})), Hierarchize({DrilldownLevel({[Due Date].[Calendar Date].[All]},,,INCLUDE_CALC_MEMBERS)})), {[Measures].[Internet Order Quantity],[Measures].[Internet Sales Amount],[Measures].[Sales Amount Forecast]}) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME,[Ship Date].[Month].[Month].[Calendar Quarter],[Ship Date].[Month].[Month].[Fiscal Quarter],[Ship Date].[Calendar Quarter].[Calendar Quarter].[Calendar Year] ON COLUMNS , NON EMPTY CrossJoin(Hierarchize(DrilldownMember({{DrilldownMember({{DrilldownLevel({[Product].[Product by Category].[All]},,,INCLUDE_CALC_MEMBERS)}}, {[Product].[Product by Category].[Category].&[1],[Product].[Product by Category].[Category].&[3],[Product].[Product by Category].[Category].&[4]},,,INCLUDE_CALC_MEMBERS)}}, {[Product].[Product by Category].[Subcategory].&[1],[Product].[Product by Category].[Subcategory].&[26],[Product].[Product by Category].[Subcategory].&[27],[Product].[Product by Category].[Subcategory].&[28],[Product].[Product by Category].[Subcategory].&[29],[Product].[Product by Category].[Subcategory].&[30],[Product].[Product by Category].[Subcategory].&[31],[Product].[Product by Category].[Subcategory].&[32],[Product].[Product by Category].[Subcategory].&[37]},,,INCLUDE_CALC_MEMBERS)), Hierarchize(DrilldownMember({{DrilldownLevel({[Product].[Size by Color].[All]},,,INCLUDE_CALC_MEMBERS)}}, {[Product].[Size by Color].[Color].&[Black]},,,INCLUDE_CALC_MEMBERS))) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME,[Product].[Product by Category].[Subcategory].[Category],[Product].[Product by Category].[Product].[Subcategory] ON ROWS  FROM [AdventureWorks] CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"
	         "SELECT NON EMPTY Hierarchize({DrilldownLevel({[Due Date].[Calendar Date].[All]},,,INCLUDE_CALC_MEMBERS)}) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS , NON EMPTY Hierarchize({DrilldownLevel({[Product].[Product by Category].[All]},,,INCLUDE_CALC_MEMBERS)}) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS  FROM [AdventureWorks] WHERE ([Measures].[Internet Order Quantity]) CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"
	         ;
	     
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
	       
	       case 4: //  Best perfomence 
	         
	         config = new HikariConfig();
	         config.setJdbcUrl("jdbc:xmla:Server=http://localhost/OLAP2/msmdpump.dll;Catalog=AdventureWorks SSAS");
           config.setUsername("OLAP");
           config.setPassword("0815Pwd!");

           config.setMinimumIdle(1);
           config.setMaximumPoolSize(2);
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
	         GenericObjectPool connectionPool = new GenericObjectPool(null);
	         ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
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
	    List<CubeCell> cubeCells = new ArrayList<>();   
	    
if (2 == 2) {  
  
     
      
		  for (Position row : cellSet.getAxes().get(1)) {
		    
		    for (Position column : cellSet.getAxes().get(0)) {

		      List<String> rows = new ArrayList<>();
		      List<String> columns = new ArrayList<>();
		      CubeCell cubeCell = new CubeCell();
		      
		      for (Member member : row.getMembers()) {
		  	  	//System.out.println("Row Member " + member.getUniqueName());
            rows.add(member.getUniqueName());  

		  	    //System.out.println("Row Member [" + member.getName() + "]");
		  	    rows.add(member.getName());  
		  	  }
		      
		  	  for (Member member : column.getMembers()) {
		  	     //System.out.println("Column Member" + member.getUniqueName());
		  	     columns.add(member.getUniqueName());
		  	     //System.out.println("Column Member [" + member.getName() + "]");
		  	     columns.add(member.getName());
		  	  }
		  	
		  	  final Cell cell = cellSet.getCell(column, row);
		  	  if ( cell.getValue() != null) {
		  	    //System.out.println("Value [" + foo(cell.getDoubleValue()) + "]" );
		  	    cubeCell.setValue(foo(cell.getDoubleValue()));
		  	  } else {
		  	    //System.out.println("Value [ EMPTY ] " );
            cubeCell.setValue("EMPTY");		  	    
		  	  }
		  	  //System.out.println("============= END OF ROW ============= [" + row.getOrdinal() + "," + column.getOrdinal() + "]"  );
		  	  cubeCell.setColumns(columns);
		  	  cubeCell.setRow(rows);
		  	  cubeCell.setRowId(row.getOrdinal());
		  	  cubeCell.setColumnId(column.getOrdinal());
		  	  
		  	  cubeCells.add(cubeCell);
		    } 
		   
		  }
}		  
	    
  //======================= JSON
  
   ObjectMapper mapper = new ObjectMapper();

   //ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
   
   mapper.setVisibility(JsonMethod.FIELD, Visibility.ANY);
   
   for(CubeCell cubeCell : cubeCells) {
     
     String jsonInString = mapper.writeValueAsString(cubeCell);
     System.out.println(jsonInString);
   }
  
   JsonToFile  jsonFile = new JsonToFile();
   jsonFile.setCubeCells(cubeCells);
   jsonFile.jsonToFile();
      
  //======================= JSON	   
	   } catch (Exception e) {
		  		System.out.println(e.getMessage());
	    }
	   
      endTime_2 = System.currentTimeMillis() - startTime;
      System.out.println(" FIRST [" + endTime + "] SECOND [" + endTime_2 + "]");
      
	  }
}


class CubeCell {
  
  private int rowId;
  private int columnId;
  private String value;
  private List<String> columns;
  private List<String> row;
  
  protected int getRowId() {
    return rowId;
  }
  protected void setRowId(int rowId) {
    this.rowId = rowId;
  }
  protected int getColumnId() {
    return columnId;
  }
  protected void setColumnId(int columnId) {
    this.columnId = columnId;
  }
  protected String getValue() {
    return value;
  }
  protected void setValue(String value) {
    this.value = value;
  }
  protected List<String> getColumns() {
    return columns;
  }
  protected void setColumns(List<String> columns) {
    this.columns = columns;
  }
  protected List<String> getRow() {
    return row;
  }
  protected void setRow(List<String> row) {
    this.row = row;
  }
 
}

