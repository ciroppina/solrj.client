package it.ciroppina.solrj.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.noggit.JSONParser;
import org.noggit.JSONUtil;

public class MySolrJClient {

	public static void main(String[] args) {
		new MySolrJClient("localhost", 8983, "referti").run();
		File json = new File("src/main/resources/spq.json");
		byte[] bytes = null;
		try {
			bytes = new byte[new FileInputStream(json).available()];
			new BufferedInputStream(new FileInputStream(json))
			.read(bytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String aJson = new String(bytes);
		
		new MySolrJClient("localhost", 8983, "referti").indexFromJson(aJson);
	}

	private void run() {
		this._urlString = "http://localhost:8983/solr/referti";
		SolrClient solr = new HttpSolrClient.Builder(_urlString).build();
		
		//indexing a doc
		SolrInputDocument document = new SolrInputDocument();
		document.addField("DocumentoID", "552200_2015022569_201508171209_01_0030");
		document.addField("MPI", "141402");
		document.addField("Nosologico", "2015022599");
		document.addField("DataInizio", "2015-08-17 12:09");
		document.addField("DataFine", "2015-08-20 11:53");
		document.addField("ErogatoreID", "12");
		document.addField("Erogatore", "RICOVERI ORDINARI");
		document.addField("PrestazioneID", "30");
		document.addField("Prestazione", "GASTROENTERITE DA SALMONELLA");
		document.addField("Referto", "Anim'e chi t'astra");
		try {
			UpdateResponse response = solr.add(document);
			// Remember to commit your changes!
			solr.commit();
		} catch (SolrServerException | IOException e) {
			System.out.println("cor cavolo che indicizza");
			e.printStackTrace();
		}
	}
	
	public void indexFromJson(String aJson) {
		String aJson1 = aJson.substring(aJson.indexOf("\"") );
		String aJson2 = aJson1.substring(0, aJson1.lastIndexOf('}') );
		
		String[] documents = aJson2.split("\\},[\n\r\\.?\\s]*\\{");
		System.out.println(documents.length + " documenti Json");
		
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
				// Remember to commit your changes!
				_solr.commit();
			} catch (SolrServerException | IOException e) {
				System.out.println("cor cavolo che indicizza");
				e.printStackTrace();
			}
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
