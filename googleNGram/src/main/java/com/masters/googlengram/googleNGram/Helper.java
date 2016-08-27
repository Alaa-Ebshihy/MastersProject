package com.masters.googlengram.googleNGram;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class Helper {

  public static void main(String[] args) throws IOException {
    for (int i = 0; i < 26; i++) {
      char x = (char) ('a' + i);
      System.out.println("wget "
          + "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-1gram-20120701-"
          + x
          + ".gz");
    }
    
//    File infile = new File("/home/alaaebshihy/Desktop/data/googlebooks-eng-all-1gram-20120701-0.gz");
//    unGzip(infile, true);

  }
  
  public static File unGzip(File infile, boolean deleteGzipfileOnSuccess) throws IOException {
    GZIPInputStream gin = new GZIPInputStream(new FileInputStream(infile));
    FileOutputStream fos = null;
    try {
        File outFile = new File(infile.getParent(), infile.getName().replaceAll("\\.gz$", ""));
        fos = new FileOutputStream(outFile);
        byte[] buf = new byte[100000];
        int len;
        while ((len = gin.read(buf)) > 0) {
            fos.write(buf, 0, len);
        }

        fos.close();
        if (deleteGzipfileOnSuccess) {
            infile.delete();
        }
        return outFile; 
    } finally {
        if (gin != null) {
            gin.close();    
        }
        if (fos != null) {
            fos.close();    
        }
    }       
}

}
