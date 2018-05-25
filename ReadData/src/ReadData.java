
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Connection;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class ReadData {
	
	static class  WordURL{
        private String word =null;
        private String urls =null;
        
        WordURL(){
        }
        
        WordURL(String word, String urls) {
            this.word = word;
            this.urls= urls;
        }
        
        public void setWord(String word) {
        	
        	this.word =word;
    //    	System.out.println(this.word + "'");
        }
        public void setUrls(String urls) {
        	this.urls = urls;
        }
        
        public String getWord() {
        	return this.word;
        }
        
        public String getUrls() {
        	return this.urls;
        }
        
        public void PrintWord() {
        	System.out.println(this.word + " : ");
        }
        
        public void  PrintUrls() {
        	StringTokenizer iter = new StringTokenizer(this.urls);
    		while(iter.hasMoreTokens()) {
    			System.out.println(iter.nextToken());
    		}
        }
}
	
	private static Connection getConn() {
	    String driver = "com.mysql.jdbc.Driver";
	    String url = "jdbc:mysql://116.56.129.192:3306/lab";
	    String username = "cooper";
	    String password = "";
	    Connection conn = null;
	    try {
	        Class.forName(driver); 
	        conn = (Connection) DriverManager.getConnection(url, username, password);
	    } catch (ClassNotFoundException e) {
	        e.printStackTrace();
	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
	    return conn;
	}
	
	private static int insert(WordURL wordUrl) {
	    Connection conn = getConn();
	    int i = 0;
	    String sql = "insert into Inverted_link(word,urls) values(?,?)";
	    PreparedStatement pstmt;
	    try {
	        pstmt = (PreparedStatement) conn.prepareStatement(sql);
	        pstmt.setString(1, wordUrl.getWord());
	        pstmt.setString(2, wordUrl.getUrls());
	        i = pstmt.executeUpdate();
	        pstmt.close();
	        conn.close();
	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
	    
	    
	    return i;
	}
	
	private static void printURL(String urls) {
		StringTokenizer iter = new StringTokenizer(urls);
		while(iter.hasMoreTokens()) {
			System.out.println(iter.nextToken());
		}
	}
	
	private static ArrayList<WordURL> select(ArrayList<String> words) {
	    Connection conn = getConn();
	    ArrayList<WordURL> result = new ArrayList<WordURL>();
	    
	    String sql = "select word, urls from Inverted_link where word in (";
	    int length = words.size();
	    
	    if(length == 0)
	    	return result;
	    
	    for(int i=0;i<length;i++) {
	    	if(i!=length-1) {
	    		sql += ("'"+words.get(i) + "',");
	    	}else {
	    		sql += ("'"+words.get(i) + "');");
	    	}
	    }
	    System.out.println(sql);
	    PreparedStatement pstmt;
	    try {
	        pstmt = (PreparedStatement)conn.prepareStatement(sql);
	        ResultSet rs = pstmt.executeQuery();
	        while (rs.next()) {
	        	WordURL pointer = new WordURL();
	            pointer.setWord(rs.getString(1));
	            String urls = rs.getString(2);
	            pointer.setUrls(urls);
	            
	            //printURL(urls);
	            result.add(pointer);
	        }
	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
	    return result;
	}
	

	public static void getSB(String filePath) {  
        Reader reader = null;  
        BufferedReader br = null;  
        WordURL wordUrls = null;
        
        try {  
            reader = new FileReader(filePath);  
            br = new BufferedReader(reader);  
            String data = null;  
            while ((data = br.readLine()) != null) {
            	wordUrls = new WordURL();
                StringTokenizer iter = new StringTokenizer(data, "{");
                String word = null;
                String urls = null;
                if(iter.hasMoreTokens()) {
                	word = iter.nextToken();
                	word = word.trim();
                	//System.out.println(word + "'");
                	wordUrls.setWord(word);
                }
                if(iter.hasMoreTokens()) {
                	urls = iter.nextToken();
                	wordUrls.setUrls(urls);
                }
                
                if(word == null)
                	continue;
               insert(wordUrls);
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {  
            try {  
                reader.close();  
                br.close();  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
        return; 
    }
	
	public static void main(String args[]) throws Exception {

	   ArrayList<String> querylist = new ArrayList<String>();
	    querylist.add("brain");
	    querylist.add("science");
	    
	    ArrayList<WordURL> result = new ArrayList<WordURL>();
	    result = ReadData.select(querylist);
	    if(result.size()==0)
	    	System.out.println("There is no corresponding match!");
	    else {
	    	for(int i=0;i<result.size();i++) {
	    		result.get(i).PrintWord();
	    		result.get(i).PrintUrls();
	    	}
	    }
		//getSB(".\\output-url-2.txt");
	
	    
		System.out.println("It's done!");
	    
	}
}
