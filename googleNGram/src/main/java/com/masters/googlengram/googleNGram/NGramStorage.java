package com.masters.googlengram.googleNGram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import com.masters.googlengram.utils.StringUtils;

public abstract class NGramStorage {
  
  protected String curNGram = "";
  protected List<Integer> yearList;
  protected List<Integer> matchCountList;
  protected List<Integer> volumeCountList;

  
  /**
   * 
   * @param directoryPath
   * @param indexName
   * @param docTypeName
   * @param initialId
   * @param startCount : n gram to start adding at
   * @param endCount: n gram to end adding at
   * @throws IOException
   */
  public void indexContentsOfDirectory(String directoryPath, String indexName, String docTypeName, 
                                       int initialId, int startCount, int endCount) 
      throws IOException {
    
    File dir = new File(directoryPath);
    File[] directoryListing = dir.listFiles();
    
    if (directoryListing != null) {
      Client client = NGramHelper.getClient();
      
      setNGramIndexMapper(client, indexName, docTypeName);

      int id = initialId;
      int count1 = startCount;

      for (File file : directoryListing) {
        if (file.getName().equals(".DS_Store"))
          continue;
        
//        int count2 = 0; //TODO: Remove
        id = indexContentsOfFile(indexName, docTypeName, startCount, client, id, count1, file);
      }
    }
  }

  public int indexContentsOfFile(String indexName, String docTypeName, int startCount, 
                                  Client client, int id, int count1, File inputFile) 
    throws FileNotFoundException, IOException {
    
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
    String input;
    
    while(count1 < startCount) br.readLine();
    
    BulkRequestBuilder bulkRequest = client.prepareBulk();

    while ((input = br.readLine()) != null) {
      
      id = addIndexRequest(indexName, docTypeName, client, bulkRequest, id, input);

      if (bulkRequest.numberOfActions() >= 1000000) {

        System.out.println(bulkRequest.numberOfActions());
        BulkResponse bulkResponse = bulkRequest.get();

        if (bulkResponse.hasFailures()) {
          // process failures by iterating through each bulk response
          // item
          System.out.println(bulkResponse.buildFailureMessage());
        }

        bulkRequest = client.prepareBulk();
//            if ((++count2) == 100) {
//              break;
//            }
      }
    }
    
    // index the last ngram in the file 
    id = addIndexRequestToBulk(indexName, docTypeName, client, bulkRequest, id);

    br.close();

//    inputFile.delete();
    
    if (bulkRequest.numberOfActions() > 0) {
      BulkResponse bulkResponse = bulkRequest.get();

      if (bulkResponse.hasFailures()) {
        // process failures by iterating through each bulk response
        // item
        System.out.println(bulkResponse.buildFailureMessage());
      }
    }
    return id;
  }
  
  
  public int addIndexRequest(String indexName, String docTypeName, Client client, BulkRequestBuilder bulkRequest,
      int id, String nGramEntry) throws IOException {

    Map<String, Object> recordMap = getRecordMap(nGramEntry);
    
    if(! curNGram.equals((String) recordMap.get(NGramHelper.ATTRIBUTE__NGRAM))) {
      if (!StringUtils.isEmptyString(curNGram)) {
        id = addIndexRequestToBulk(indexName, docTypeName, client, bulkRequest, id);
      }
      initLists();

    }
    addToLists(recordMap);
    updateCurrentValues(recordMap);
    // TODO: add log here
    
    return id;
  }
  
  public void initLists() {
    yearList = new ArrayList<Integer>();
    matchCountList = new ArrayList<Integer>();
    volumeCountList = new ArrayList<Integer>();
  }
  
  public void addToLists(Map<String, Object> recordMap) {
    yearList.add((Integer) recordMap.get(NGramHelper.ATTRIBUTE__YEAR));
    matchCountList.add((Integer) recordMap.get(NGramHelper.ATTRIBUTE__MATCH_COUNT));
    volumeCountList.add((Integer) recordMap.get(NGramHelper.ATTRIBUTE__VOLUME_COUNT));
  }

  public void updateCurrentValues(Map<String, Object> recordMap) {
    curNGram = (String) recordMap.get(NGramHelper.ATTRIBUTE__NGRAM);
  }
  
  public abstract Map<String, Object> getRecordMap(String nGramEntry);
  
  public abstract void setNGramIndexMapper(Client client, String indexName, String docTypeName) 
      throws IOException;
  
  public abstract int addIndexRequestToBulk(String indexName, String docTypeName, Client client, 
      BulkRequestBuilder bulkRequest, int id) throws IOException;
}
