package com.masters.googlengram.googleNGram;

import java.io.IOException;

public class NGramTest {

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
//    int nGramStorageType = Integer.parseInt(args[3]);
//    int initialId = Integer.parseInt(args[4]);
//    int startCount = Integer.parseInt(args[5]);
//    int endCount = Integer.parseInt(args[6]);
    
    NGramStorage nGramStorage = null;
    
//    switch (nGramStorageType) {
//    case 2:
//      nGramStorage = new NGramFrequencyChange();
//      break;
//
//    default:
//      nGramStorage = new NGramBaseline();
//      break;
//    }

    nGramStorage = new NGramFrequencyChange();
//    nGramStorage.indexContentsOfDirectory(args[0], args[1], args[2], initialId, startCount, endCount);
    
    nGramStorage.indexContentsOfDirectory("/home/alaaebshihy/Master_Thesis/Data/Test", 
                                          "freq_test", "nGram", 1, 1, 100);
//    nGramStorage.indexContentsOfFile("freq_test", "nGram", 1, client, 1, 1000, inputFile)
        
  }

}
