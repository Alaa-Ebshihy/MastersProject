package com.masters.googlengram.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.google.code.externalsorting.ExternalSort;
import com.masters.googlengram.googleNGram.NGramHelper;
import com.masters.googlengram.googleNGram.NGramHelper.NGram;
import com.masters.googlengram.utils.StringUtils;

public class PreprocessingHelper {

  private static PreprocessingHelper instance;
  
  private static final String PATH__STOP_WORDS = "stopwords_list_long.txt";

  private PreprocessingHelper() {
  }

  public static PreprocessingHelper getInstance() {
    if (instance == null) {
      instance = new PreprocessingHelper();
    }

    return instance;
  }
  
  public void analyzeNGramDirectoryZipped(String directoryPath) throws IOException {

    File dir = new File(directoryPath);
    File[] directoryListing = dir.listFiles();
    
    if (directoryListing != null) {
      GoogleNgramAnalyzer analyzer = new GoogleNgramAnalyzer(new FileReader(new File(PATH__STOP_WORDS)));

      for (File file : directoryListing) {
        if (file.getPath().endsWith(".gz")) {
          File unGzippedFile = NGramHelper.unGzip(file, true);
          analyzeNGramFile(unGzippedFile, analyzer);
          unGzippedFile.delete();
        }
      }
      
      analyzer.close();
    }
  }

  public void analyzeNGramFile(File filePath, GoogleNgramAnalyzer analyzer) throws IOException {

    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
    String input;
    BufferedWriter bw = new BufferedWriter
      (new OutputStreamWriter(new FileOutputStream(new File(filePath + "_normalized"))));
    
    int count = 0;
    while ((input = br.readLine()) != null) {
      Map<String, Object> recordMap = NGramHelper.getBaseRecordMap(input);
      String analyzedNGram = analyzeNGram(analyzer, 
                                          (String) recordMap.get(NGramHelper.ATTRIBUTE__NGRAM));
      
      if(StringUtils.isEmptyString(analyzedNGram))
        continue;

      bw.append(NGramHelper.getNgramRecordFromMap(recordMap, analyzedNGram));
      
      if((++count) >= 1000000) {
        bw.flush();
        count = 0;
      }
    }
    
    bw.flush();
    br.close();
    bw.close();
  }
  
  public void sortAndMergeDirectory(String directoryPath) throws IOException {
    File dir = new File(directoryPath);
    File[] directoryListing = dir.listFiles();
    
    if (directoryListing != null) {

      for (File file : directoryListing) {
        if (file.getName().equals(".DS_Store"))
          continue;
        File outSortedFile = new File(file + "_sorted_temp");
        File outMergedFile = new File(file + "_sorted");
        ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(file), outSortedFile);
        file.delete();
        mergeSimilarNgrams(outSortedFile, outMergedFile);
        outSortedFile.delete();
      }
      
    }
  }

  public void mergeSimilarNgrams(File inputFile, File outputFile) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
    
    NGram curNGram = null;
    String input = br.readLine();
    
    if(input != null)
      curNGram = NGramHelper.createNGram(input);
    
    int count = 0;
    while((input = br.readLine()) != null) {
      NGram tempNGram = NGramHelper.createNGram(input);
      
      if(tempNGram.equals(curNGram)) {
        NGramHelper.megeTwoNgrams(curNGram, tempNGram);
      }
      else {
        bw.append(NGramHelper.getNGramRecord(curNGram));
        curNGram = tempNGram;
        
        if((++count) >= 1000000) {
          bw.flush();
          count = 0;
        }  
      }
    }
    
    if(curNGram != null)
      bw.append(NGramHelper.getNGramRecord(curNGram));
    
    bw.flush();
    bw.close();
    br.close();
  }

  private String analyzeNGram(GoogleNgramAnalyzer analyzer, String nGram) throws IOException {
    TokenStream stream = analyzer.tokenStream("field", nGram);
    PorterStemFilter stemFilter = new PorterStemFilter(stream);
    return getStringFromTokens(stemFilter);
  }

  private String getStringFromTokens(TokenStream stream) throws IOException {
    // get the CharTermAttribute from the TokenStream
    CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);

    StringBuffer tokenBuffer = new StringBuffer();
    try {
      stream.reset();
      while (stream.incrementToken()) {
        tokenBuffer.append(termAtt.toString().split("_")[0] + " ");
      }
      stream.end();
    } finally {
      stream.close();
    }
    return tokenBuffer.toString().trim();
  }
  
  public static void main(String[] args) throws FileNotFoundException, IOException {
    GoogleNgramAnalyzer analyzer = new GoogleNgramAnalyzer(new FileReader(new File("stopwords_list_long.txt")));
//    PreprocessingHelper.getInstance().analyzeNGramFile(new File("/home/alaaebshihy/Master_Thesis/Data/sample_test"), analyzer);
//    PreprocessingHelper.getInstance().analyzeNGramDirectory("/home/alaaebshihy/Master_Thesis/Data/Test/");
    
    

//    ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(new File("/home/alaaebshihy/Master_Thesis/Data/Test/googlebooks-eng-all-1gram-20120701-o_normalized")), new File("/home/alaaebshihy/Master_Thesis/Data/Test/sample_test_out"));
  PreprocessingHelper.getInstance().mergeSimilarNgrams(new File("/home/alaaebshihy/Master_Thesis/Data/Test/sample_test_out"), new File("/home/alaaebshihy/Master_Thesis/Data/Test/sample_test_out_2"));

  }
}
