/**
 * Lecteur d'articles de wikipédia au format XML
 * produit des couples (nom de la page, contenu de la page)
 * NE PAS MODIFIER
 * (pourrait être remplacé par StreamInputFormat et StreamXMLRecordReader, cf HDG page 237)
 * @author F. Raimbault
 */
package pagerank;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Reads XML records that are delimited by a <page> </page> tag.
 */
public class XmlPageInputFormat extends FileInputFormat<Text, Text> {

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new XmlRecordReader();
  }

  /**
   * XMLRecordReader class to read through a given xml document to output xml blocks as records as
   * specified by the start tag and end tag
   * 
   */
  static final byte[] startPageTag = "<page>".getBytes();

  static final byte[] endPageTag = "</page>".getBytes();;

  static final byte[] startTitleTag = "<title>".getBytes();

  static final byte[] endTitleTag = "</title>".getBytes();;

  public static class XmlRecordReader extends RecordReader<Text, Text> {
    
    long start;

    long end;

    FSDataInputStream fsin;

    final DataOutputBuffer data_buffer = new DataOutputBuffer();

    final DataOutputBuffer key_buffer = new DataOutputBuffer();

    @Override
    public void initialize(InputSplit input, TaskAttemptContext context) throws IOException,
        InterruptedException {
      FileSplit split = (FileSplit) input;
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(context.getConfiguration());
      fsin = fs.open(split.getPath());
      fsin.seek(start);
    }

    private final Text next_key = new Text();

    private final Text next_value = new Text();

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return next_key;
    }
 
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return next_value;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (fsin.getPos() < end) {
        if (readUntilMatch(startPageTag, false)) { // <page> trouvée
          // log.info("<page> trouve");
          try {
            data_buffer.write(startPageTag);
            if (readUntilMatch(endPageTag, true)) { // </page> trouvée
              // log.info("</page> trouve");
              if (key_buffer.size() > 0) { // <title> </title> trouvée
                next_key.set(key_buffer.getData(), 0, key_buffer.getLength());
              } else {
                next_key.set("NO PAGE TITLE FOUND from " + fsin.getPos());
              }
              next_value.set(data_buffer.getData(), 0, data_buffer.getLength());
              return true;
            }
          } catch (IOException e) {
            // skip
          } finally {
            data_buffer.reset();
            key_buffer.reset();
          }
        }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }

    boolean readUntilMatch(byte[] tag, boolean withinPage) throws IOException {
      int page_tag_index = 0; // pour le <page> ou </page> tag
      int title_tag_index = 0; // pour le <title> ou </title> tag
      while (true) {
        int byte_read = fsin.read();
        if (byte_read == -1) {// end of file
          return false;
        }
        if (withinPage) { // we are inside a page
          data_buffer.write(byte_read);
          if (byte_read == startTitleTag[title_tag_index]) { // check if we're matching title tag
            // log.info("<title> trouve");
            title_tag_index++;
            if (title_tag_index >= startTitleTag.length) { // we are inside a title
              title_tag_index = 0;
              while (true) {
                byte_read = fsin.read();
                if (byte_read == -1) {// end of file
                  return false;
                }
                data_buffer.write(byte_read);
                if (byte_read == endTitleTag[title_tag_index]) {
                  title_tag_index++;
                  if (title_tag_index >= endTitleTag.length) { // end of page title
                    // log.info("</title> trouve");
                    // log.info("title= "+new String(buffer2.getData()));
                    title_tag_index = 0;
                    break;
                  }
                } else {
                  key_buffer.write(byte_read);
                  title_tag_index = 0;
                }
              }
            }
          } else {
            title_tag_index = 0;
          }
        }
        // check if we're matching page tag
        if (byte_read == tag[page_tag_index]) {
          page_tag_index++;
          if (page_tag_index >= tag.length) {
            return true;
          }
        } else {
          page_tag_index = 0;
        }
        // see if we've passed the stop point:
        if (!withinPage && page_tag_index == 0 && fsin.getPos() >= end) {
          return false;
        }
      }
    }
  }

}
