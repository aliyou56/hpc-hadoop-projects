/**
 * Classe pour faciliter la manipulation de Tuple par des programmes MapReduce
 * @author raimbaul
 */
package pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
 
/**
 * Tuple lu ou écrit par des taches Map et Reduce. 
 * Chaque membre du tuple est une String
 */
public class TupleWritable implements WritableComparable<TupleWritable>{

  private String[] values; 

  public TupleWritable(){
    values= null;
  }
  
  public TupleWritable(int size) {
    values= new String[size];
  }

  public TupleWritable(String... values){
    this(values.length);
    for (int i=0; i<values.length; i++){
      this.values[i]= values[i];
    }
  }
  
  public TupleWritable(LinkedList<String> values){
    this(values.size());
    int i=0; 
    for (String value:values){
      this.values[i++]= value;
    }
  }
  
  public int size(){
    return (values == null) ? 0 : values.length;
  }
  
  public String get(int index){
    return (index >= size()) ? null : values[index];
  }
  
  public String[] get(){
    return (values==null) ? null : values;
  }
  
  public void set(int index, String value){
    if (index >= size()) return;
    values[index]= value;
  }

  /**
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int length= in.readInt();
    if (length == 0){
      values= null;
    }else{
      values= new String[length];
      for(int i=0; i<length; i++){
        values[i]= Text.readString(in);
      }
    }
  }

  /**
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    if (values == null){
      out.writeInt(0);
    }else{
      out.writeInt(values.length);
      for(String value:values){
        Text.writeString(out, (value==null) ? "" : value);      
      }
    }
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TupleWritable)) {
      return false;
    }
    TupleWritable that = (TupleWritable) obj;
    if (this.values.length != that.values.length){
      return false;
    }
    for(int i=0; i<this.values.length; i++){
      if (! this.values[i].equals(that.values[i])){
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(TupleWritable that) {
    if (this.values.length != that.values.length){
      return 0;
    }
    for(int i=0; i<this.values.length; i++){
      if (this.values[i].compareTo(that.values[i])==1){
        return 1;
      }else if (this.values[i].compareTo(that.values[i])==-1){
        return -1;
      }
    }
    return 0;
  }
  
  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuffer buf= new StringBuffer();
    for(String value:values){
      buf.append(value).append(',');      
    }
    if (buf.length()>0) buf.deleteCharAt(buf.length()-1);
    return buf.toString();
  }


  /**page_links
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    
    int hash= 0;
    for(String value:values){
      hash += value.hashCode();
    }
    return hash;
  }

}
