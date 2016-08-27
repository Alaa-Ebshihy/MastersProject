package com.masters.googlengram.googleNGram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

public class NGramFrequencyChange extends NGramStorage {

  private int curMatchCount = 0;
  private int curYear = 0;
  private List<Integer> freqChangeList;
  
  public int addIndexRequestToBulk(String indexName, String docTypeName, Client client, 
                                   BulkRequestBuilder bulkRequest, int id) 
   throws IOException {
    
    Map<String, Object> nGramDocMap = new HashMap<String, Object>();
    nGramDocMap.put(NGramHelper.ATTRIBUTE__NGRAM, curNGram);
    nGramDocMap.put(NGramHelper.ATTRIBUTE__YEAR, yearList);
    nGramDocMap.put(NGramHelper.ATTRIBUTE__MATCH_COUNT, matchCountList);
    nGramDocMap.put(NGramHelper.ATTRIBUTE__VOLUME_COUNT, volumeCountList);
    nGramDocMap.put(NGramHelper.ATTRIBUTE__FREQ_CHANGE, freqChangeList);

    IndexRequestBuilder indexRequest = client.prepareIndex(indexName, docTypeName, "" + id++)
        .setSource(NGramHelper.getBuilder(nGramDocMap));
    bulkRequest.add(indexRequest);
    return id;
  }

  //TODO: should change to float
  public Map<String, Object> getRecordMap(String nGramEntry) {
    
    Map<String, Object> recordMap = NGramHelper.getBaseRecordMap(nGramEntry);
    
    int freqChange = 0;
    
    if(curNGram.equals((String) recordMap.get(NGramHelper.ATTRIBUTE__NGRAM))) {
      freqChange = (Integer)(recordMap.get(NGramHelper.ATTRIBUTE__MATCH_COUNT)) - curMatchCount;
    }

    recordMap.put(NGramHelper.ATTRIBUTE__FREQ_CHANGE, freqChange);
    
    return recordMap;
  }

  //TODO: the freq change is integer for now should be changed later to float
  public void setNGramIndexMapper(Client client, String indexName, String docTypeName) throws IOException {
    
    Map<String, Object> attributePropertiesMap = NGramHelper.getBaseAttributePropertiesMap();
    
    Map<String, String> typeFloatMap = new HashMap<String, String>();
    typeFloatMap.put(NGramHelper.ATTRIBUTE__TYPE, NGramHelper.ATTRIBUTE__TYPE_INTEGER);
    
    attributePropertiesMap.put(NGramHelper.ATTRIBUTE__FREQ_CHANGE, typeFloatMap);
    
    NGramHelper.createNGramIndexMapper(client, indexName, docTypeName, attributePropertiesMap);
  }

  public void initLists() {
    super.initLists();
    
    freqChangeList = new ArrayList<Integer>();
  }
  
  public void addToLists(Map<String, Object> recordMap) {
    super.addToLists(recordMap);
    
    freqChangeList.add((Integer) recordMap.get(NGramHelper.ATTRIBUTE__FREQ_CHANGE));
  }

  public void updateCurrentValues(Map<String, Object> recordMap) {
    super.updateCurrentValues(recordMap);
    
    curMatchCount = (Integer) (recordMap.get(NGramHelper.ATTRIBUTE__MATCH_COUNT));
    curYear = (Integer)(recordMap.get(NGramHelper.ATTRIBUTE__YEAR));
  }
}
