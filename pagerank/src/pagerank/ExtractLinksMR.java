/**
 * Construction de la liste des liens sortants des articles de Wikipédia.
 * 
 * Format du fichier d'entrée : XML de wikipédia
 * On utilise le RecordReader fournit par {@link XmlPageInputFormat} 
 * qui produit des couples ("titre","contenu de la page")
 * 
 * Les liens sont extraits des balises [[linkname|text]] trouvées avec l'expression régulière 
 * obtenue par la méthode Pattern.compile("\\[\\[[^\\]]+\\]\\]"); 
 * 
 * Format du fichier de sortie (SequenceFile) : 
 *  ("pagename1",tuple(1,"linkname1","linkname2"...))   
 *  ("pagename2",tuple(1,"linkname1","linkname2"...))
 *  
 *  ou les tuple() sont des instances de {@link TupleWritable}
 *   
 * NE PAS MODIFIER (sauf pour les paramètres d'exécution)
 * 
 * @author F. Raimbault
 */

package pagerank;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import pagerank.ExtractLinksMapper.MapCounters;

public class ExtractLinksMR {
  
  // a executer par : yarn jar pagerank.jar pagerank.ExtractLinksMR
  // suivi de manière optionnelle par les chemins d'entrées et de sortie

  public static void main(String[] args) throws IOException, URISyntaxException{
    
    if ((args == null) || args.length==0){ // on prend des chemins par défaut
      args= new String[]{
          "/data/wikipedia/frwiki-pages-10",     //          10 pages
          "frwiki-links-10-articles",            //        1748 links
          // "/data/wikipedia/frwiki-pages-100",    //         100 pages
          // "frwiki-links-100-articles",           //      20 147 links
          // "/data/wikipedia/frwiki-pages-1000",   //       1 000 pages
          // "frwiki-links-1000-articles",          //     204 331 links
          // "/data/wikipedia/frwiki-pages-10000",  //      10 000 pages
          // "frwiki-links-10000-articles",         //   1 770 142 links
          // "/data/wikipedia/frwiki-pages-100000", //     100 000 pages
          // "frwiki-links-100000-articles",        //   7 595 791 links
          // "/data/wikipedia/frwiki-pages",        //     3744189 pages 
          // "frwiki-links-articles",               //  96 007 177 links
      };
    }else if (args.length != 2) {
      System.err.println("usage: hadoop -jar pagerank.ExtractLinksMR input_filename output_filename");
      System.exit(1);
    }
    
    Configuration conf = new Configuration();

    // parametres d'exécution des taches
    conf.setInt("mapreduce.job.reduces",10); // 10 taches reduce
    conf.setInt("mapreduce.map.maxattempts",1); // pas de relance d'une tache map qui échoue 
    conf.setInt("mapreduce.reduce.maxattempts",1); // pas de relance d'une tache reduce qui échoue 
    conf.setBoolean("mapreduce.map.speculative",true); // exécution spéculative des taches map
    conf.setBoolean("mapreduce.reduce.speculative",true); // exécution spéculative des taches reduce
    
    // suppression des fichiers de sortie
    FileSystem hdfs= FileSystem.get(new URI("hdfs://hnn:9000"),conf); 
    hdfs.delete(new Path(args[1]), true);
    
    // paramètres du Job
    Job job = Job.getInstance(conf,"ExtractLinks");
    job.setJarByClass(ExtractLinksMR.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(ExtractLinksMapper.class);
    job.setInputFormatClass(XmlPageInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TupleWritable.class);
    job.setReducerClass(Reducer.class); // identity
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TupleWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    try {
      job.waitForCompletion(true);
      Counter pages_counter= job.getCounters().findCounter(MapCounters.PAGE_CNT);
      Counter links_counter= job.getCounters().findCounter(MapCounters.LINK_CNT);
      if ((pages_counter != null) && (links_counter != null)){
        System.out.println(pages_counter.getValue()+" page names and "+links_counter.getValue()+" links ref found in "+args[0]);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
  
  
}
