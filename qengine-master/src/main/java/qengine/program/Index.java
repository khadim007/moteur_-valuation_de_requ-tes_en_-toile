package qengine.program;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.Statement;

public class Index {
	
	private List<Statement> statementList;
	private Map<Integer, String> dict;
	private HashMap<Integer, HashMap<Integer, List<Integer>>> spo;
	private HashMap<Integer, HashMap<Integer, List<Integer>>> sop;
	private HashMap<Integer, HashMap<Integer, List<Integer>>> ops;
	private HashMap<Integer, HashMap<Integer, List<Integer>>> osp;
	private HashMap<Integer, HashMap<Integer, List<Integer>>> pso;
	private HashMap<Integer, HashMap<Integer, List<Integer>>> pos;
	

	
	public Index(List<Statement> statementList, Map<Integer, String> dict) {
		this.spo = new HashMap<Integer, HashMap<Integer, List<Integer>>>();
		this.sop = new HashMap<Integer, HashMap<Integer, List<Integer>>>();
		this.ops = new HashMap<Integer, HashMap<Integer, List<Integer>>>();
		this.osp = new HashMap<Integer, HashMap<Integer, List<Integer>>>();
		this.pso = new HashMap<Integer, HashMap<Integer, List<Integer>>>();
		this.pos = new HashMap<Integer, HashMap<Integer, List<Integer>>>();
		this.statementList = new ArrayList<>(statementList);
		this.dict = new HashMap<>(dict);
		
	}
	
	public void creationIndexHexastore() {
		List<List<Integer>> Lspo = new ArrayList<>();
		List<List<Integer>> Lsop = new ArrayList<>();
		List<List<Integer>> Lops = new ArrayList<>();
		List<List<Integer>> Losp = new ArrayList<>();
		List<List<Integer>> Lpso = new ArrayList<>();
		List<List<Integer>> Lpos = new ArrayList<>();
		
		for (Statement st : statementList) {
			String subject = st.getSubject().toString();
			String predicate = st.getPredicate().toString();
			String object = st.getObject().toString();
			int keySub = -1;
			int keyPred = -1;
			int keyObj = -1;
			
			for(Map.Entry<Integer, String> entry : dict.entrySet()) {
				if(entry.getValue().equals(subject)) {
					keySub = entry.getKey();
				}
				if(entry.getValue().equals(predicate)) {
					keyPred = entry.getKey();
				}
				if(entry.getValue().equals(object)) {
					keyObj = entry.getKey();
				}
			}
			
			Lspo.add(Arrays.asList(keySub, keyPred, keyObj));
			Lsop.add(Arrays.asList(keySub, keyObj, keyPred));
			Lops.add(Arrays.asList(keyObj, keyPred, keySub));
			Losp.add(Arrays.asList(keyObj, keySub, keyPred));
			Lpso.add(Arrays.asList(keyPred, keySub, keyObj));
			Lpos.add(Arrays.asList(keyPred, keyObj, keySub));
		}
		
		for (List<Integer> row : Lspo) {
            int key1 = row.get(0);
            int key2 = row.get(1);
            int value = row.get(2);
            if (!spo.containsKey(key1)) {
                spo.put(key1, new HashMap<>());
            }
            Map<Integer, List<Integer>> innerMap = spo.get(key1);
            if (!innerMap.containsKey(key2)) {
                innerMap.put(key2, new ArrayList<>());
            }
            innerMap.get(key2).add(value);
        }
		for (List<Integer> row : Lsop) {
            int key1 = row.get(0);
            int key2 = row.get(1);
            int value = row.get(2);
            if (!sop.containsKey(key1)) {
            	sop.put(key1, new HashMap<>());
            }
            Map<Integer, List<Integer>> innerMap = sop.get(key1);
            if (!innerMap.containsKey(key2)) {
                innerMap.put(key2, new ArrayList<>());
            }
            innerMap.get(key2).add(value);
        }
		for (List<Integer> row : Lops) {
            int key1 = row.get(0);
            int key2 = row.get(1);
            int value = row.get(2);
            if (!ops.containsKey(key1)) {
            	ops.put(key1, new HashMap<>());
            }
            Map<Integer, List<Integer>> innerMap = ops.get(key1);
            if (!innerMap.containsKey(key2)) {
                innerMap.put(key2, new ArrayList<>());
            }
            innerMap.get(key2).add(value);
        }
		for (List<Integer> row : Losp) {
            int key1 = row.get(0);
            int key2 = row.get(1);
            int value = row.get(2);
            if (!osp.containsKey(key1)) {
            	osp.put(key1, new HashMap<>());
            }
            Map<Integer, List<Integer>> innerMap = osp.get(key1);
            if (!innerMap.containsKey(key2)) {
                innerMap.put(key2, new ArrayList<>());
            }
            innerMap.get(key2).add(value);
        }
		for (List<Integer> row : Lpso) {
            int key1 = row.get(0);
            int key2 = row.get(1);
            int value = row.get(2);
            if (!pso.containsKey(key1)) {
            	pso.put(key1, new HashMap<>());
            }
            Map<Integer, List<Integer>> innerMap = pso.get(key1);
            if (!innerMap.containsKey(key2)) {
                innerMap.put(key2, new ArrayList<>());
            }
            innerMap.get(key2).add(value);
        }
		for (List<Integer> row : Lpos) {
            int key1 = row.get(0);
            int key2 = row.get(1);
            int value = row.get(2);
            if (!pos.containsKey(key1)) {
            	pos.put(key1, new HashMap<>());
            }
            Map<Integer, List<Integer>> innerMap = pos.get(key1);
            if (!innerMap.containsKey(key2)) {
                innerMap.put(key2, new ArrayList<>());
            }
            innerMap.get(key2).add(value);
        }
	}
	
	public String toString(HashMap<Integer, HashMap<Integer, List<Integer>>> list) {
		String resultat = "";
		for (Map.Entry<Integer, HashMap<Integer, List<Integer>>> entry1 : list.entrySet()) {
            int key1 = entry1.getKey();
            HashMap<Integer, List<Integer>> innerMap = entry1.getValue();
            resultat += "Clé map 1: { " + key1+", ";
            for (Map.Entry<Integer, List<Integer>> entry2 : innerMap.entrySet()) {
                int key2 = entry2.getKey();
                List<Integer> values = entry2.getValue();
                resultat += "(Clé map 2: { " + key2+", ";
                resultat += "List: " + values+" }) ";
            }
            resultat += " }\n";
        }
		return resultat;
	}
	
	public List<Integer> getSubject(String hex, int predicate, int object) {
		List<Integer> res = new ArrayList<>();		
		if(hex.equals("ops")) {
			HashMap<Integer, List<Integer>> map1 = ops.get(object);
	        if (map1 != null && map1.containsKey(predicate)) {
	        	res.addAll(map1.get(predicate));
	        }else {
	        	return null;
	        }
		}
		return res;
	}

	public HashMap<Integer, HashMap<Integer, List<Integer>>> getSpo() {
		return spo;
	}

	public HashMap<Integer, HashMap<Integer, List<Integer>>> getSop() {
		return sop;
	}

	public HashMap<Integer, HashMap<Integer, List<Integer>>> getOps() {
		return ops;
	}

	public HashMap<Integer, HashMap<Integer, List<Integer>>> getOsp() {
		return osp;
	}

	public HashMap<Integer, HashMap<Integer, List<Integer>>> getPso() {
		return pso;
	}

	public HashMap<Integer, HashMap<Integer, List<Integer>>> getPos() {
		return pos;
	}
}
