//importing the packages
import java.io.IOException; 
import org.apache.pig.EvalFunc; 
import org.apache.pig.data.Tuple; 

public class FilterBplCards extends EvalFunc<String> // extending the class EvalFunc
{ 

   public String exec(Tuple input) throws IOException  // overriding exec method of class EvalFunc
   {   
	  // condition to check if input is null or input size is 0
      if (input == null || input.size() == 0) 
      {
      return null;  // return null
      }
      String str = (String)input.get(0); // reading the entire line of input
      
      String[] str1 = str.split(","); // split the input string on the basis of delimiter
      String district = str1[0]; 
      String objectiveBpl = str1[1];
      String performanceBpl = str1[2];
      
      // condition to check the 80% objective of BPL cards
      if (Double.parseDouble(performanceBpl)/Double.parseDouble(objectiveBpl) >= 0.8)
      {
      return district;  // return the district names
      }
	  return null; // return null

   } 
}