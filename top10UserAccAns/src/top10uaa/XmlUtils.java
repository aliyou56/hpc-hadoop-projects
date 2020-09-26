
package top10uaa;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XmlUtils {

  static final Pattern pattern_keyvalue= Pattern.compile("([a-zA-Z_]+)=(\\\"[^(\\\")]*\\\")");
  
  /**
   * Parse an XML string and extract the (first) value of a given attribute
   * @param key attribute key 
   * @param xml the string
   * @return the value of the attribute key or null if not found
   */
  public static String getAttributeValue(String key, String xml) {
    
    Matcher match = pattern_keyvalue.matcher(xml);
    while (match.find()) {
      try {
        if (! match.group(1).equals(key))
          continue;
      } catch (Exception e) {
        continue;
      }
      try {
        String value = match.group(2);
        return value.substring(1, value.length()-1);
      } catch (Exception e) {
        return "";
      }
    }
    return null;
  }
  /**
   * Parse an XML string and extract a map of all attributes
   * @param xml the string
   * @return the map(key,value) of all attributes contained in xml
   */
  public static Map<String,String> getAttributesMap(String xml) {
    
    Map<String, String> map= new HashMap<String, String>();
    Matcher match = pattern_keyvalue.matcher(xml);
    while (match.find()) {
      String key= match.group(1);
      String value = match.group(2);
      map.put(key, value.substring(1, value.length()-1));
    }
    return map;
  }
    
  /**
   * Suppress the < and > around a list of tags
   * @param s a XML containing tags
   * @return the list of tags without < >
   */
  public static String formatTags(String s){
    
    return s.replaceAll("&lt;","").replaceAll("&gt;",",");
  }
  
  private static HashMap<String, String> htmlEscape = new HashMap<>();
  static{
    htmlEscape.put("&lt;" , "<");
    htmlEscape.put("&gt;" , ">");
    htmlEscape.put("&amp;" , "&");
    htmlEscape.put("&quot;" , "\"");
    htmlEscape.put("&nbsp;" , " ");
    htmlEscape.put("&apos;" , "'");
    htmlEscape.put("&#xA;" , "\n");
  }

 /**
  * Unescape XML strings
  * @param source an HTML escaped XML string
  * @return the string without escaped characters
 */
 public static final String unescapeHTML(String source) {
   StringBuilder sb= new StringBuilder(source);
   int sb_start= 0, entity_start= 0, entity_end= 0; // index on sb
   boolean continueLoop;
   do {
      continueLoop = false;
      entity_start = sb.indexOf("&", sb_start); // search for "&" on sb from sb_start
      if (entity_start > -1) { // "&" found at index entity_start
        entity_end = sb.indexOf(";", entity_start+1); // index of entity_end
        if (entity_end > entity_start) { // full entity found
          String html_entity = sb.substring(entity_start, entity_end + 1); // entity string
          String html_value= htmlEscape.get(html_entity);
          if (html_value != null) { // an html string has been found
            sb.replace(entity_start,entity_end+1,html_value); // replace the html entity by its html value
            sb_start= entity_start+html_value.length(); // next escaped string research will start after the html value inserted
          }else {
            System.err.println(html_entity+" not found");
            sb.delete(entity_start,entity_end+1); // delete the html entity
            sb_start= entity_start; 
          }
          continueLoop= true; // continue to traverse the source string
        }
      }
   } while (continueLoop);
   return sb.toString();
 }

 /**
  * Extract the first occurrence of a Wikipedia URL inside a given string
  * @param text the string
  * @return the Wikipedia URL
  */
  public static String getWikipediaURL(String text) {

    int idx = text.indexOf("\"http://en.wikipedia.org");
    if (idx == -1) {
        return null;
    }
    int idx_end = text.indexOf('"', idx + 1);

    if (idx_end == -1) {
        return null;
    }

    int idx_hash = text.indexOf('#', idx + 1);

    if (idx_hash != -1 && idx_hash < idx_end) {
        return text.substring(idx + 1, idx_hash);
    } else {
        return text.substring(idx + 1, idx_end);
    }

  }
  // test
  public static void main(String[] args) {

    if (args.length==1) { 
      
    }else {
      String xml_line="<row Id=\"6857\" "
          + "PostTypeId=\"2\" "
          + "ParentId=\"6847\" "
          + "CreationDate=\"2008-08-09T17:47:10.223\" "
          + "Score=\"0\" "
          + "Body=\"&lt;p&gt;Well, there is a certain set of best practices for security. At a minimum, for database applications, you need to watch out for SQL Injection.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Other stuff like hashing passwords, encrypting connection strings, etc. are also a standard.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;From here on, it depends on the actual application.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Luckily, if you are working with frameworks such as .Net, a lot of security protection comes built-in.&lt;/p&gt;&#xA;\" "
          + "OwnerUserId=\"380\" "
          + "LastActivityDate=\"2008-08-09T17:47:10.223\" "
          + "CommentCount=\"0\" />";
      System.out.println("Id value="+getAttributeValue("Id", xml_line));
      
      Map<String, String> parsed= getAttributesMap(xml_line);
      for (String att:parsed.keySet()){
        System.out.print(att+"=");
        String value= getAttributeValue(att,xml_line);
        switch(att){
        case "Tags":
          System.out.println(formatTags(value));
          break;
        case "Body":
          System.out.println(unescapeHTML(value));
          break;
        default:
          System.out.println(value);
        }
      }
    }
  }
}