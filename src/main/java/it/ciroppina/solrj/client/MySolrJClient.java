package it.ciroppina.solrj.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class MySolrJClient {

	public static void main(String[] args) {

		File json = new File("src/main/resources/spq.json");
		byte[] bytes = null;
		try {
			bytes = new byte[new FileInputStream(json).available()];
			new BufferedInputStream(new FileInputStream(json))
			.read(bytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String aJson = new String(bytes);
		
		new MySolrJClient("localhost", 8983, "referti").indexFromJson(aJson);
	}

	public void indexFromJson(String aJson) {
		String aJson1 = aJson.substring(aJson.indexOf("\"") );
		String aJson2 = aJson1.substring(0, aJson1.lastIndexOf('}') );
		
		String[] documents = aJson2.split("\\},[\n\r\\.?\\s]*\\{");
		System.out.println(documents.length + " documenti Json");
		
		Long i = System.currentTimeMillis();
		for (String jdoc : documents) {
			SolrInputDocument document = new SolrInputDocument();
			//System.out.println(jdoc.trim());
			//System.out.println("___");
			
			String[] fields = jdoc.split(",\r\n");
			for (String field : fields) {
				//System.out.println(field.trim());
				String fieldName = field.substring(1, field.indexOf(":") - 1)
						.replace('"', ' ').trim();
				String value = field.substring(field.indexOf(":") + 1)
						.replace('"', ' ').trim();
				document.addField(fieldName, value);
			}
			//loop json docs
			try {
				UpdateResponse response = _solr.add(document);
				System.out.println("adding document took (ms): " 
					+ response.getElapsedTime() );
				// Remember to commit your changes!
				//_solr.commit();
			} catch (SolrServerException | IOException e) {
				System.out.println("cor cavolo che indicizza");
				e.printStackTrace();
			}
		}
		Long e = System.currentTimeMillis();
		System.out.println("overall indexing (ms) took: " + (e - i));
		// Remember to commit your changes!
		try {
			_solr.commit();
		} catch (SolrServerException | IOException ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * useful Constructors
	 * */
	private String _urlString = "";
	SolrClient _solr = null;
	public MySolrJClient(String host, int port, String schema) {
		_urlString = "http://" + host + ":" + port + "/solr/" + schema.trim();
		_solr = new HttpSolrClient.Builder(_urlString).build();
	}
	public MySolrJClient(String proto, String host, int port, String schema) {
		_urlString = proto + "://" + host + ":" + port + "/solr/" + schema.trim();
		_solr = new HttpSolrClient.Builder(_urlString).build();
	}

}
