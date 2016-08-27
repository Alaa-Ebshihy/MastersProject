package com.masters.googlengram.test;

import java.io.IOException;

import com.masters.googlengram.googleNGram.NGramBaseline;
import com.masters.googlengram.googleNGram.NGramFrequencyChange;
import com.masters.googlengram.googleNGram.NGramStorage;
import com.masters.googlengram.preprocessing.PreprocessingHelper;

public class Main {
  
  public static void start (String directoryPath, int nGramStorageType, String indexName, String docTypeName, 
                            int initialId, int startCount, int endCount) 
    throws IOException {
    
    //step1: analyze all files in directory
    PreprocessingHelper.getInstance().analyzeNGramDirectoryZipped(directoryPath);
    
    //step2: sort and merge analyzed files
    PreprocessingHelper.getInstance().sortAndMergeDirectory(directoryPath);
    
    //step3: indexing process
    indexingProcess(directoryPath, nGramStorageType, indexName, docTypeName, initialId, startCount, endCount);
  }

  private static void indexingProcess(String directoryPath, int nGramStorageType, String indexName, String docTypeName,
      int initialId, int startCount, int endCount) throws IOException {
    NGramStorage nGramStorage = null;

    switch (nGramStorageType) {
    case 2:
      nGramStorage = new NGramFrequencyChange();
      break;

    default:
      nGramStorage = new NGramBaseline();
      break;
    }

    nGramStorage = new NGramFrequencyChange();
    nGramStorage.indexContentsOfDirectory(directoryPath, indexName, docTypeName, initialId, startCount, endCount);
  }
  
  /**
   * 
   * args[0] -> path to the data file
   * args[1] -> index name
   * args[2] -> doc type name
   * args[3] -> storage type
   * args[4] -> initial id
   * args[5] -> start ngram from input file 
   * args[6] -> end ngram from input file
   * 
   */
  public static void main(String[] args) throws IOException {
    args = new String[] {"/Users/alaa-elebshihy/Masters/Thesis/Data/Test", "freq_test", "nGram", "2", "1", "1", "100"};
    
    int nGramStorageType = Integer.parseInt(args[3]);
    int initialId = Integer.parseInt(args[4]);
    int startCount = Integer.parseInt(args[5]);
    int endCount = Integer.parseInt(args[6]);
    
    Main.start(args[0], nGramStorageType, args[1], args[2], initialId, startCount, endCount);
    
  }

}
