package qengine.program;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.Statement;

public class Dictionnaire {
	
	private Map<Integer, String> dict;
	private List<Statement> statementList;
	
	public Dictionnaire(List<Statement> statementList) {
		this.dict = new HashMap<>();
		this.statementList = new ArrayList<>(statementList);
	}
	
	public void creationDictonnaire() {
		for (Statement st : statementList) {
			if(!dict.containsValue(st.getSubject().toString())) 
                dict.put(dict.size(), st.getSubject().toString());
            if(!dict.containsValue(st.getPredicate().toString()))
                dict.put(dict.size(),st.getPredicate().toString());
            if(!dict.containsValue(st.getObject().toString())) 
                dict.put(dict.size(), st.getObject().toString());
        }
	}
	
	public String toString() {
		String resultat = "";
		for(Map.Entry<Integer, String> ligne : dict.entrySet()) {
		    Integer cle = ligne.getKey();
		    String valeur = ligne.getValue();
		    resultat = resultat + ("Clé : " + cle + ", Valeur : " + valeur+"\n");
		}
		return resultat;
	}

	public Map<Integer, String> getDict() {
		return dict;
	}
	
	public int getCle(String valeur) {
		Integer cle = -1;
        for (Map.Entry<Integer, String> ligne : dict.entrySet()) {
            if (ligne.getValue().equals(valeur)) {
                cle = ligne.getKey();
                break;
            }
        }
		return cle;
	}
	
	public String getValeur(int cle) {
		return dict.get(cle);
	}
	
	public int getSize() {
		return dict.size();
	}
}