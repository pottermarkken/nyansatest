import java.text.SimpleDateFormat;
import java.util.concurrent.*;
import java.util.*;

import java.io.IOException; 
import java.util.logging.Level; 
import java.util.logging.Logger; 
import java.util.logging.*; 


/*
 *  URL Hit Counter class 
 *  Online Assessment
 * 
 * 
 * */
public class URLHitCounter {

	 /**
     * @param scanner for read stdin
     */
	private static Scanner scanner;
		
    public static boolean hasNextLine() {
        return scanner.hasNextLine();
    }
    
    public static String readLine() {
        String line;
        try {
            line = scanner.nextLine();
        }
        catch (NoSuchElementException e) {
            line = null;
        }
        return line;
    }
    
	//! process input
    public static String[] readAllLines() {
        ArrayList<String> lines = new ArrayList<String>();
        while (hasNextLine()) {
        	String readLine = readLine();
            lines.add(readLine);
            
            //System.out.print(lines.get(lines.size() - 1 ));
        }
        return lines.toArray(new String[lines.size()]);
    }
    
    
    /**
     *  Class Column 
     *  stores data <String URL, int count> 
     */
    static public class Column
    {
    	public String URL;
    	public int count;
    	
    	public Column() {}
    	public Column(String url, int count)
    	{
    		this.URL = url;
    		this.count = count;
    	}
    	
    	
    } // end of Column 
    
    /**
     *  Mini Cassandra 
     *  process data stream store 
     *  process data fetching for query request 
     *  
     */
    
    
    public static class MiniCassandra
    {
    	public ConcurrentHashMap<String, HashMap<String, ArrayList>> map;
    	PriorityQueue<DailyNode> displayQueue;
    	
    	//constructor 
    	public MiniCassandra(){
    		map = new ConcurrentHashMap<String, HashMap<String, ArrayList>>();
    		displayQueue = new PriorityQueue<DailyNode>(new ComparetorByNumber2() );
    	}
    	
    	
    	 /**
         * @param timestamp a string
         * @param dateString a string 
         * @param url a string
         * @param update current hashmap for stored data of url, timestamp and date 
         */
    	public void insert(String timestamp, String dateString, String url)
    	{
    		HashMap<String, ArrayList> dailySummary;
    		ArrayList<String> tsList; 
    		// update timestamp into mini cassandra database
    	    if (!map.containsKey(dateString))
    	    {
    	    	// not found daily time format in database
    	        dailySummary = new HashMap<String, ArrayList>();    
    	    }
    	    else
    	    {
    	    	dailySummary = map.get(dateString);
    	    }
    	    if (!dailySummary.containsKey(url))
    	    {
    	    	tsList = new ArrayList<String>();// store timestamp list
    	    }
    	    else
    	    {
    	    	tsList = dailySummary.get(url);
    	    }
    	    
    	    // O(1) operation for data update in current map
    	    tsList.add(timestamp);
    	    dailySummary.put(url, tsList);
    	    map.put(dateString, dailySummary);
    	    
    	}
    	
    	//! new comparator for Priority Queue for daily <url, count> sorting
    	public static class ComparetorByNumber implements Comparator{
    		
    		public int compare(Object o1, Object o2)
    		{
    			Column s1 = (Column)o1;
    			Column s2 = (Column)o2;
    			
    			int result = s1.count > s2.count ? 1: (s1.count == s2.count) ? 0 : -1;
    		     if (result == 0)
    		     {
    		    	    int compareQuotes = s1.URL.compareTo(s2.URL);
    		    		if(compareQuotes>0)
    						result= - 1;
    					else if(compareQuotes==0)
    						result= 0;
    					else
    						result= 1;
    		    		
    		     }
    		    	  
    		     return (-result);
    		}
    		
    	}
    	
    	//! new comparator for Priority Queue for daily date sorting <mm/dd/yyyy>
        public static class ComparetorByNumber2 implements Comparator{
    		
        	// Override compare rule. 
    		public int compare(Object o1, Object o2)
    		{
    			DailyNode s1 = (DailyNode)o1;
    			DailyNode s2 = (DailyNode)o2;
    			
    			// deal with yyyy > mm > dd， 08/09/2014 GMT
    			
    			String[] o1String = s1.dateString.split("\\s+");
    			String[] o2String = s2.dateString.split("\\s+");
    			
    			String[] o1t = o1String[0].split("\\/");
    			String[] o2t = o2String[0].split("\\/");
    			//! compare yyyy
    			int result =  o1t[2].compareTo(o2t[2]);
    			if (result == 0)
    			{
    				//! compare mm
    				result = o1t[1].compareTo(o2t[1]);
    				if (result == 0)
    				{
    					//! compare dd
    					result =  o1t[0].compareTo(o2t[0]);
    				}
    			}
    			return result;
    	
    		}
    		   
     }
    	
        /**
         * @param  url a string
         * @param  dateString a string
         * @return number of URL hit 
         */
        
    	public int hitCountOnUrl(String url, String dateString)
    	{
    		// return hit count number for specified url in one day 
    		if (!map.containsKey(dateString))
    		{
    			// not found specified date
    			return -1;
    		}
    		if (! map.get(dateString).containsKey(url))
    		{
    			return 0;
    		}
    		// return the size of timestamp
    		// O(1) for map query
    		return map.get(dateString).get(url).size();
    	}
    	
        /**
         * @param dateString a string
         * @return a list of Columns after sorting in priority queue
         * example: [<www.facebook.com 3>, <www.google.com 2> ]
         */
    	public List<Column> fetchDailyReport(String dateString)
    	{
    		 // heap sorting based on url hit count.
    		 PriorityQueue<Column> pq = new PriorityQueue<Column>(new ComparetorByNumber() );
    		 HashMap<String, ArrayList> dailySummary;
    		 List<Column> res = new ArrayList<Column>();
    		 
    		 if (!map.containsKey(dateString))
    		 {
    			 // not found daily report
    			 return null;
    		 }
    		 dailySummary = map.get(dateString);
    		 for (Map.Entry mapElement : dailySummary.entrySet()) 
    		 {
    			 String key = (String)mapElement.getKey(); 
    			 int count = this.hitCountOnUrl(key, dateString);
    			 pq.add(new Column(key, count));
    			 
    		 }
    		 while (!pq.isEmpty())
    		 {
    			 res.add(pq.poll());
    			 //System.out.println("current Column is: " + res.get(res.size() - 1).URL + " " + res.get(res.size() - 1).count);;
    		 }
    		 
    		return res;
    	}
    	
 
        /**
         * @return a list of date stored in map
         */  	
    	public Set<String> listAllDate()
    	{
    		Set<String> keys = map.keySet();
    		return keys;
    	}
    	
        /**
         * local test for fetch daily report 
         * @return a list of Columns after sorting in priority queue
         */
    	public void parallelDisplayReport()
    	{
    		  Set<String> keys  = this.listAllDate();
              // Sorting HashSet using List 
              List<String> list = new ArrayList<String>(keys); 
              Collections.sort(list); 
              
              // multithread programming
              for (String key : list)
              {
              	System.out.println("key:" + key);
              	this.fetchDailyReport(key);
              }
    	}
    	
    	public List<String> getAllDate()
    	{
    		 Set<String> keys  = this.listAllDate();
             // Sorting HashSet using List 
             List<String> list = new ArrayList<String>(keys); 
             Collections.sort(list); 
             return list;
    	}
    } // end of cassandra 
    
    
    /**
     * Data Input Stream Processing class
     * read "timestamp|URL"
     * 
     * store tsUnix
     * store URL
     * store processed date converted from timestamp UNIX time
     * 
     */
    public static class DataStreamProcessing
    {
        public String tsUnix;
        public String URL;
        public String dateString;
        
    	public DataStreamProcessing() {
    		
    		this.tsUnix = null;
    		this.URL = null;
    		this.dateString = null;
    		
    	}
    	public DataStreamProcessing(String dataStream) {
    		
    	     // read data and store in member varibale 
    		 readData(dataStream);
    	}
    	
    	public void readData(String dataStream)
    	{
    		// dataStream format 
    		// eg. 1407564301|www.nba.com
    		
    		String[] data_input = dataStream.split("\\|");
    		this.tsUnix = data_input[0];
    		this.URL = data_input[1];
    		this.dateString = convertUnixToDate(tsUnix);
    	}
    	
    	
        /**
         * @param unixtime2 a string
         * @return a string with yyyy/mm/dd formart 
         */
    	
    	public String convertUnixToDate(String unixTime2)
    	{
        	//! process timestamp
        	long unixTime = Long.parseLong(unixTime2);
        	
        	Date date = new Date(unixTime*1000L); // convert seconds to milliseconds
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy z");
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

            String dateString = sdf.format(date);
        	//System.out.println(dateString); 
    	    return dateString;
    	}
    	
    	
    	public String getURL()
    	{
           return this.URL; 	    	
    	}
    	public String getDateString()
    	{
    		return this.dateString;
    	}
    	public String gettsUnix()
    	{
    		return this.tsUnix;
    	}
    	
    	
    } // end of Data Stream 
   
    public static class DailyNode
    {
    	public String dateString;
    	public List<Column> res;
    	
    	public DailyNode(String dateString, List<Column> res)
    	{
    		this.dateString = dateString;
    		this.res = res;
    	}
    }
    
    /**
     * Parallel Thread ThreadInsertDatabase
     * for Data Stream processing
     * 
     * */
    public static class ThreadInsertDatabase extends Thread
    {
        
        public MiniCassandra miniDatabase;
        public int startIndex = 0;
        public int num_threads = 0; 
        public String[] input_list;
        
    	public ThreadInsertDatabase() {
           
        }
    	        
    	public ThreadInsertDatabase(MiniCassandra miniDatabase, int startIndex, int num_threads, String[] input_list)
    	{
    		this.miniDatabase = miniDatabase;
    		this.startIndex = startIndex;
    		this.num_threads = num_threads; 		
    	    this.input_list = input_list;
    	} 
     
        @Override
        public void run() {
            // System.out.println("current thread is + " + Thread.currentThread().getId());
        	
            // multithread programming
          synchronized(miniDatabase.map) {
             for (int i = this.startIndex; i < this.input_list.length; i = i + num_threads)
             {
            	 DataStreamProcessing dateStream = new DataStreamProcessing();
            	 dateStream.readData(input_list[i]);
                 miniDatabase.insert(dateStream.tsUnix, dateStream.dateString, dateStream.URL);
                
             } 
          } // sync up
       }
    }
    
    
    /**
     * Parallel Thread ThreadUrlHitCount
     * for mini Database query processing
     * 
     * */
    public static class ThreadUrlHitCount extends Thread
    {
        
        public MiniCassandra miniDatabase;
        public int startIndex = 0;
        public int num_threads = 0; 
        public  List<String> list = null;
        public List<Column> res = null;
     
    	public ThreadUrlHitCount() {
           
        }
    	   
    	public ThreadUrlHitCount(MiniCassandra miniDatabase, int startIndex, int num_threads, List<String> list)
    	{
    		this.miniDatabase = miniDatabase;
    		this.startIndex = startIndex;
    		this.num_threads = num_threads; 
    		this.list = list; // get all date in mini database 
    		res = new ArrayList<Column>();
    	
    	} 
     
        @Override
        public void run() {
       	
            // multithread programming
          synchronized(miniDatabase.displayQueue) {
             for (int i = this.startIndex; i < list.size(); i = i + num_threads)
             {
            	//System.out.println("key:" + list.get(i));
            	res = this.miniDatabase.fetchDailyReport(list.get(i));
            	this.miniDatabase.displayQueue.add(new DailyNode(list.get(i), res));
             } 
          } // sync up
       }
    }
    
        
    public static void testModule() throws InterruptedException
    {
    	// scanner = new Scanner(System.in);  
    	// String[] res = readAllLines();
        
        // unit test for local debugging
        String[] input_list = {"1407564301|www.nba.com",
                               "1407478021|www.facebook.com", 
                                "1407478021|www.facebook.com",
                                "1407481200|news.ycombinator.com",
        			  "1407478028|www.google.com",  
        			  "1407564301|sports.yahoo.com", 
        			  "1407564300|www.cnn.com", 
        			  "1407564300|www.nba.com",
        			  "1407564300|www.nba.com",
        			  "1407564301|sports.yahoo.com", 
        			  "1407478022|www.google.com", 
        			  "1407648022|www.twitter.com"
        			};
        	
        // process input stream
    	// DataStreamProcessing dateStream = new DataStreamProcessing();
     
        // start to process concurrent hashmap 
        MiniCassandra miniDatabase = new MiniCassandra();
        int num_threads = 2;
        
        
        
        	//Multithread Programming
           ThreadInsertDatabase[] workerRead = new  ThreadInsertDatabase[num_threads]; 
        	for (int i = 0; i < num_threads; i++)
        	{
        		 workerRead[i] = new ThreadInsertDatabase(miniDatabase, i, num_threads, input_list) ;
        		 workerRead[i].start();		
        	}
            for( int i = 0 ; i < num_threads; i++)
            {
           	 	
            	 workerRead[i].join();
            		 
            }          
        	List<String> list = miniDatabase.getAllDate(); 
        	
            //test output 
            // miniDatabase.parallelDisplayReport();
             
            ThreadUrlHitCount[] workerThread = new ThreadUrlHitCount[num_threads];
            for( int i = 0 ; i < num_threads; i++)
            {
            	
            	 workerThread[i] = new ThreadUrlHitCount(miniDatabase, i, num_threads, list);
            	 workerThread[i].start();           	      	 
            }
            
            for( int i = 0 ; i < num_threads; i++)
            {
            	 workerThread[i].join();		 
            }
            
            
            while(!miniDatabase.displayQueue.isEmpty())
            {
            	DailyNode node = miniDatabase.displayQueue.poll();
            	System.out.println(node.dateString);
            	for( int i = 0; i < node.res.size(); i++)
            	{
            		System.out.println(node.res.get(i).URL + " " + node.res.get(i).count);
            	}
            }
    }
    
    public static void main(String[] args) {
    	try {

    			URLHitCounter.testModule();
    		
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}   
    } // end of main
    
}



