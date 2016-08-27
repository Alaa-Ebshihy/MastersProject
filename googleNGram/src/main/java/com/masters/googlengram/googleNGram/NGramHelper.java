package com.masters.googlengram.googleNGram;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class NGramHelper {

  private static final String CLUSTER__NAME_NGRAM = "nGram_Cluster";
  
  public static final String ATTRIBUTE__NGRAM = "ngram";
  public static final String ATTRIBUTE__YEAR = "year";
  public static final String ATTRIBUTE__MATCH_COUNT = "match_count";
  public static final String ATTRIBUTE__VOLUME_COUNT = "volume_count";
  public static final String ATTRIBUTE__FREQ_CHANGE = "freq_change";
  
  public static final String ATTRIBUTE__TYPE = "type";
  public static final String ATTRIBUTE__TYPE_INTEGER = "integer";
  public static final String ATTRIBUTE__TYPE_STRING = "string";
  public static final String ATTRIBUTE__TYPE_FLOAT = "float";
  public static final String ATTRIBUTE__PROPERTIES = "properties";
  
  public static final String HOST = "52.11.51.32";
  public static final String HOST__LOCAL = "localhost";
  
  /**
   * 
   * @return
   * @throws UnknownHostException
   */
  public static Client getClient() throws UnknownHostException {
    Settings settings = Settings.settingsBuilder().put("cluster.name", CLUSTER__NAME_NGRAM).build();
    Client client = TransportClient.builder().settings(settings).build()
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST__LOCAL),
                                                            9300));
    return client;
  }
  
  public static Map<String, Object> getBaseAttributePropertiesMap() {
    Map<String, Object> baseAttributePropertiesMap = new HashMap<String, Object>();
    
    Map<String, String> typeIntegerMap = new HashMap<String, String>();
    typeIntegerMap.put(ATTRIBUTE__TYPE, ATTRIBUTE__TYPE_INTEGER);
    Map<String, String> typeStringMap = new HashMap<String, String>();
    typeStringMap.put(ATTRIBUTE__TYPE, ATTRIBUTE__TYPE_STRING);
    
    baseAttributePropertiesMap.put(ATTRIBUTE__NGRAM, typeStringMap);
    baseAttributePropertiesMap.put(ATTRIBUTE__YEAR, typeIntegerMap);
    baseAttributePropertiesMap.put(ATTRIBUTE__MATCH_COUNT, typeIntegerMap);
    baseAttributePropertiesMap.put(ATTRIBUTE__VOLUME_COUNT, typeIntegerMap);

    return baseAttributePropertiesMap;
  }
  
  /**
  * to create an index with special mapping --> mapping to set the attribute types
  *   
  * @param client
  * @param indexName
  * @throws IOException
  */
 public static void createNGramIndexMapper(Client client, String indexName, String docTypeName,
                                           Map<String, Object> attibutePropertiesMap) 
     throws IOException {
   
   IndicesExistsResponse indexExistResponse = client.admin().indices().prepareExists(indexName).execute().actionGet();
   
   if(!indexExistResponse.isExists()){
     CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
     // create the mapping for object 
     XContentBuilder mappingBuilder = jsonBuilder()
         .startObject()
           .startObject(docTypeName)
             .field(ATTRIBUTE__PROPERTIES, attibutePropertiesMap)
           .endObject()
         .endObject();
     
     System.out.println(mappingBuilder.string());
     createIndexRequestBuilder.addMapping(docTypeName, mappingBuilder);
 
     // MAPPING DONE
     createIndexRequestBuilder.execute().actionGet();
   }
 }
  
  /**
   * this method creates json builder given a map of json object key and value
   * @param recordMap: map of json object key and value
   * @return json object of the given map
   */
  public static XContentBuilder getBuilder(Map<String, Object> recordMap)
      throws IOException {

    XContentBuilder builder = jsonBuilder().startObject();
    for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
      builder.field(entry.getKey(), entry.getValue());
    }
    builder.endObject();

    return builder;
  }

  public static NGram createNGram(String ngramEntry) {
    Map<String, Object> nGramMap = getBaseRecordMap(ngramEntry);
    String nGram = (String) nGramMap.get(ATTRIBUTE__NGRAM);
    int year = (Integer) nGramMap.get(ATTRIBUTE__YEAR);
    int matchCount = (Integer) nGramMap.get(ATTRIBUTE__MATCH_COUNT);
    int volumeCount = (Integer) nGramMap.get(ATTRIBUTE__VOLUME_COUNT);

    return new NGram(nGram, year, matchCount, volumeCount);
  }
  
  public static Map<String, Object> getBaseRecordMap(String ngramEntry) {
    StringTokenizer st = new StringTokenizer(ngramEntry, "\t");
    
    Map<String, Object> baseStorageMap = new HashMap<String, Object>();
    baseStorageMap.put(ATTRIBUTE__NGRAM, st.nextToken());
    baseStorageMap.put(ATTRIBUTE__YEAR, Integer.parseInt(st.nextToken()));
    baseStorageMap.put(ATTRIBUTE__MATCH_COUNT, Integer.parseInt(st.nextToken()));
    baseStorageMap.put(ATTRIBUTE__VOLUME_COUNT, Integer.parseInt(st.nextToken()));

    return baseStorageMap;
  }
  
  public static String getNGramRecord(NGram ngram) {
    StringBuffer strBuf = new StringBuffer();
    strBuf.append(ngram.getnGram());
    strBuf.append("\t");
    strBuf.append(ngram.getYear());
    strBuf.append("\t");
    strBuf.append(ngram.getMatchCount());
    strBuf.append("\t");
    strBuf.append(ngram.getVolumeCount());
    strBuf.append("\r\n");
    
    return strBuf.toString();
  }
  
  public static String getNgramRecordFromMap(Map<String, Object> recordMap, String analyzedNGram) {
    StringBuffer strBuf = new StringBuffer();
    strBuf.append(analyzedNGram);
    strBuf.append("\t");
    strBuf.append(recordMap.get(NGramHelper.ATTRIBUTE__YEAR));
    strBuf.append("\t");
    strBuf.append(recordMap.get(NGramHelper.ATTRIBUTE__MATCH_COUNT));
    strBuf.append("\t");
    strBuf.append(recordMap.get(NGramHelper.ATTRIBUTE__VOLUME_COUNT));
    strBuf.append("\r\n");
    
    return strBuf.toString();
  }
  
  /**
   * merges the second ngram into the first one such that the frequency is combined for both
   * @param firstNGram
   * @param secondNGram
   */
  public static void megeTwoNgrams(NGram firstNGram, NGram secondNGram) {
    firstNGram.setMatchCount(firstNGram.getMatchCount() + secondNGram.getMatchCount());
    firstNGram.setVolumeCount(firstNGram.getVolumeCount() + secondNGram.getVolumeCount());
  }
  
  public static long getMilliSecondsSinceEpoc(String year){
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.clear();
    calendar.set(Integer.parseInt(year), Calendar.JANUARY, 1);
    
    return calendar.getTimeInMillis();  
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

  //
  //nested classes
  //
  public static final class NGram {
    private String nGram;
    private int year;
    private int matchCount;
    private int volumeCount;
    
    public NGram(String nGram, int year, int matchCount, int volumeCount) {
      this.nGram = nGram;
      this.year = year;
      this.matchCount = matchCount;
      this.volumeCount = volumeCount;
    }
    
    public String getnGram() {
      return nGram;
    }
    public void setnGram(String nGram) {
      this.nGram = nGram;
    }
    public int getYear() {
      return year;
    }
    public void setYear(int year) {
      this.year = year;
    }
    public int getMatchCount() {
      return matchCount;
    }
    public void setMatchCount(int matchCount) {
      this.matchCount = matchCount;
    }
    public int getVolumeCount() {
      return volumeCount;
    }
    public void setVolumeCount(int volumeCount) {
      this.volumeCount = volumeCount;
    }

    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + matchCount;
      result = prime * result + ((nGram == null) ? 0 : nGram.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      NGram other = (NGram) obj;
      if (nGram == null) {
        if (other.nGram != null)
          return false;
      } else if (!nGram.equals(other.nGram))
        return false;
      if (year != other.year)
        return false;
      return true;
    }
  }
}
