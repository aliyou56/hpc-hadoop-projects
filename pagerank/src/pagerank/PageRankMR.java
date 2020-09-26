/**
 * Programme de calcul du PageRank sur les données issues de {@link ExtractlinkMR}
 * 
 * NE PAS MODIFIER (sauf pour les paramètres d'exécution)
 * 
 * @author raimbaul
 */
package pagerank;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import pagerank.PageRankMapper.MapCounters;
import pagerank.PageRankReducer.ReduceCounters;

/**
 * 
 */
public class PageRankMR {
  
  public static final double D_FACTOR = 0.85; // constante pour l'algo du pagerank
  private static final double EPSILON = 0.01; // variation minimale du page rank moyen entre 2 itérations

  //a executer par : hadoop jar pagerank.jar pagerank.PageRankMR
  //suivi de manière optionnelle par les chemins d'entrées et de sortie

  public static void main(String[] args) {

    if ((args == null) || args.length==0){
      args = new String[] { 
        // "frwiki-links-10-articles",     // loop 3 : pagerank average= 0.1627
        // "frwiki-ranks-10-articles",     
        // "frwiki-links-100-articles",    // loop 3 : pagerank average= 0.15388    | 0.15385 
        // "frwiki-ranks-100-articles",    
        // "frwiki-links-1000-articles",   // loop 3 : pagerank average= 0.156449   | 0.15639
        // "frwiki-ranks-1000-articles",   
        // "frwiki-links-10000-articles",  // loop 4 : pagerank average= 0.1829261  | 0.18278613584075223 
        // "frwiki-ranks-10000-articles",  
        // "frwiki-links-100000-articles", // loop 5 : pagerank average= 0.21256295 | 0.21298833651428398
        // "frwiki-ranks-100000-articles", 
        "frwiki-links-articles",        // loop 9 : pagerank average= 0.46
        "frwiki-ranks-articles",
      }; 
    } else if (args.length != 2) {
      System.err.println("usage: hadoop -jar pagerank.PageRankMR input_filename output_dirname");
      System.exit(1);
    }
     
    int iter_cnt= 0; // nombre d'itérations
    String output_dir= args[0]; // initialisé avec le répertoire d'entrée
    
    double oldrank= Float.MAX_VALUE; // pagerank moyen précédent
    try {
      while(true){ // a repeter jusqu'au point fixe du PAGE_RANK 
        
        Configuration conf = new Configuration();  
        conf.setInt("mapred.reduce.tasks",10);
        Job job= Job.getInstance(conf,"PageRank");
        job.setJarByClass(PageRankMR.class);

        // le répertoire de sortie de l'itération précédente devient le répertoire d'entrée
        FileInputFormat.setInputPaths(job, new Path(output_dir));
        // on crée un nouveau nom de répertoire de sortie à chaque itération
        output_dir= args[1]+"_"+iter_cnt%2;       

        // on supprime le répertoire de sortie
        FileSystem hdfs= FileSystem.get(new URI("hdfs://hnn:9000"),conf); // pour execution sur le cluster de l'IRISA
        hdfs.delete(new Path(output_dir), true);
        
        FileOutputFormat.setOutputPath(job, new Path(output_dir));
    
        job.setMapperClass(PageRankMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TupleWritable.class);
        
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TupleWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.waitForCompletion(true);
        
        // on calcule de pagerank moyen en récupérant les compteurs
        Counter pagerank_counter=  job.getCounters().findCounter(ReduceCounters.PAGE_RANK);
        if (pagerank_counter == null){
          System.err.println("PAGE_RANK counter not found");
          System.exit(3);
        }else{
          System.out.println("PAGE_RANK counter= "+pagerank_counter.getValue());
        }
        Counter pagecnt_counter=  job.getCounters().findCounter(MapCounters.PAGE_CNT);
        if (pagecnt_counter == null){
          System.err.println("PAGE_CNT counter not found");
          System.exit(3);
        }else{
          System.out.println("PAGE_CNT counter= "+pagecnt_counter.getValue());
        }
        double newrank= (double) pagerank_counter.getValue() / (1000 * pagecnt_counter.getValue());
        // le PAGE_RANK est un réél qui a été multiplié par 1000 puis tronqué pour tenir dans un long
        iter_cnt += 1;
        System.out.println("loop "+iter_cnt+" : pagerank average= "+newrank);
        if (Math.abs(newrank-oldrank) < EPSILON){  // faible variation du PAGE_RANK
          // on supprime le répertoire de sortie qui ne sert à rien et on quitte
          output_dir= args[1]+"_"+iter_cnt%2;
          hdfs.delete(new Path(output_dir), true);
          break; // on arrête le calcul du PageRank
        }
        oldrank= newrank;
      } 
      System.out.println("PageRank computed on "+args[0]+ " completed in "+iter_cnt+" loops");
    } catch (IOException e1) {
      System.err.println(e1);
      System.exit(1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
