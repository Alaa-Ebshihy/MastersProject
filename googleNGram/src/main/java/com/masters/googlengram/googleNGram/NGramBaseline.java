package com.masters.googlengram.googleNGram;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

public class NGramBaseline extends NGramStorage{

  public Map<String, Object> getRecordMap(String nGramEntry) {
    return NGramHelper.getBaseRecordMap(nGramEntry);
  }
  
  public void setNGramIndexMapper(Client client, String indexName, String docTypeName) throws IOException {

    NGramHelper.createNGramIndexMapper(client, indexName, docTypeName, 
                                       NGramHelper.getBaseAttributePropertiesMap());
  }

  @Override
  public int addIndexRequestToBulk(String indexName, String docTypeName, Client client, BulkRequestBuilder bulkRequest,
      int id) throws IOException {
    
    Map<String, Object> nGramDocMap = new HashMap<String, Object>();
    nGramDocMap.put(NGramHelper.ATTRIBUTE__NGRAM, curNGram);
    nGramDocMap.put(NGramHelper.ATTRIBUTE__YEAR, yearList);
    nGramDocMap.put(NGramHelper.ATTRIBUTE__MATCH_COUNT, matchCountList);
    nGramDocMap.put(NGramHelper.ATTRIBUTE__VOLUME_COUNT, volumeCountList);
    
    IndexRequestBuilder indexRequest = client.prepareIndex(indexName, docTypeName, "" + id++)
        .setSource(NGramHelper.getBuilder(nGramDocMap));
    bulkRequest.add(indexRequest);

    return id;
  }

}
