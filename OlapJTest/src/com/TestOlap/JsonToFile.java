/**
 * 
 */
package com.TestOlap;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author Primetals 2017 -- Wieslaw Czuber
 *
 */
public class JsonToFile {
     private List<CubeCell> cubeCells;

    /**
     * @return the cubeCells
     */
    public List<CubeCell> getCubeCells() {
      return cubeCells;
    }

    /**
     * @param cubeCells the cubeCells to set
     */
    public void setCubeCells(List<CubeCell> cubeCells) {
      this.cubeCells = cubeCells;
    }
     
     public void jsonToFile() throws IOException {
       ObjectMapper objectMapper = new ObjectMapper();
       //Set pretty printing of json
       
       objectMapper.setVisibility(JsonMethod.FIELD, Visibility.ANY);
       
       objectMapper.writeValue(new File("d:\\Jaspersoft\\temp\\cells.json"), cubeCells);
       String arrayToJson = objectMapper.writeValueAsString(cubeCells);
       System.out.println("1. Convert List of person objects to JSON :");
       System.out.println(arrayToJson);
     }
     
     
}
