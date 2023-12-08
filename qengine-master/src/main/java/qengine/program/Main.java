package qengine.program;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import com.opencsv.CSVWriter;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.eclipse.rdf4j.model.Statement;

final class Main {
	static final String baseURI = null;
	static final String workingDir = "data/";

//	static final String queryFile = workingDir + "sample_query.queryset";
//	static final String queryFile = workingDir + "test.queryset";
//	static final String queryFile = workingDir + "STAR_ALL_workload.queryset";

//	static final String dataFile = workingDir + "sample_data.nt";
//	static final String dataFile = workingDir + "test.nt";
//	static final String dataFile = workingDir + "100K.nt";
	
	static String queryFile;
	static String dataFile;
	static String outputFile;
	static boolean activerJena;
	static String pourcentageWarm;
	static boolean permutation;
	static boolean export_query_results;
	
	
	private static Map<Integer, List<String>> resultat_moi = new HashMap<>();
	private static Map<Integer, List<String>> resultat_jena = new HashMap<>();
	private static Map<Integer, String> csv_temps_requete = new HashMap<>();
	private static Map<String, String> csv_general = new HashMap<>();
	private static List<Statement> statementList;
	private static BufferedWriter writer1;
	private static BufferedWriter writer2;
	private static Dictionnaire dict;
	private static Index ind;
		

	
	/**
	 * Méthode utilisée ici lors du parsing de requête sparql pour agir sur l'objet obtenu.
	 * @throws IOException 
	 */
	public static void processAQuery(ParsedQuery query, int numero) throws IOException {
		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
		String predicate = null;
		String object = null;
		int keyPredicate = -1;
		int keyObject = -1;
		List<Integer> commun = new ArrayList<>();
		long startTime = System.currentTimeMillis();
		int i = 0;
		for (StatementPattern pattern : patterns) {
			predicate = pattern.getPredicateVar().getValue().toString();
			object = pattern.getObjectVar().getValue().toString();
			keyPredicate = dict.getCle(predicate);
			keyObject = dict.getCle(object);
			List<Integer> res = ind.getSubject("ops", keyPredicate, keyObject);
			if(keyPredicate == -1 || keyObject == -1 || res == null) {
				csv_temps_requete.put(numero, String.valueOf(System.currentTimeMillis() - startTime));
				return;
			}
			if(i == 0) 
				commun.addAll(res);
			else 
				commun.retainAll(res);
			i+=1;
        }
		commun = commun.stream().distinct().collect(Collectors.toList());
		csv_temps_requete.put(numero, String.valueOf(System.currentTimeMillis() - startTime));
		for(int cle : commun) {
			ajouterElementDictionnaire(resultat_moi, numero, dict.getValeur(cle), "moi");
		}
	}
	
	
	public static void executerRequeteJena() throws IOException {
		Model model = ModelFactory.createDefaultModel();
        model.read(dataFile);
        
        try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();
			
			int i = 1;
			while (lineIterator.hasNext()){  
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					String requeteSPARQL = queryString.toString();
		            try (QueryExecution qexec = QueryExecutionFactory.create(requeteSPARQL, model)) {
		                ResultSet resultSet = qexec.execSelect();
		                while (resultSet.hasNext()) {
		                    QuerySolution solution = resultSet.nextSolution();
		                    RDFNode v0Value = solution.get("v0");
		                    ajouterElementDictionnaire(resultat_jena, i, v0Value.toString(), "jena");
		                }
		            }
					queryString.setLength(0);
					i+=1;
				}
			}
		}
    }


	public static void main(String[] args) throws Exception {
		
		long startTime1 = System.currentTimeMillis();
		
		HashMap<String, String> options = parseArguments(args);
		queryFile = options.getOrDefault("-queries", "");
		dataFile = options.getOrDefault("-data", "");
        outputFile = options.getOrDefault("-output", "");
        activerJena = options.containsKey("-Jena");
        pourcentageWarm = options.getOrDefault("-warm", "");
        permutation = options.containsKey("-shuffle");
        export_query_results = options.containsKey("-export_query_results");
        verificationParDefaut();
		
		parseData();
		
		long startTime2 = System.currentTimeMillis();
		dict = new Dictionnaire(statementList);
		dict.creationDictonnaire();
		csv_general.put("tempsDico", String.valueOf(System.currentTimeMillis() - startTime2));
		
		long startTime3 = System.currentTimeMillis();
		ind = new Index(statementList, dict.getDict());
		ind.creationIndexHexastore();
		csv_general.put("tempsIndex", String.valueOf(System.currentTimeMillis() - startTime3));
		
		long startTime4 = System.currentTimeMillis();
		parseQueries();
		if(activerJena) {
			executerRequeteJena();
		}
		csv_general.put("tempsTotalWorkload", String.valueOf(System.currentTimeMillis() - startTime4));

//		System.out.println("------------------Dictionnaire------------------");
//		System.out.println(dict.toString());
//		System.out.println("------------------spo------------------");
//		System.out.println(ind.toString(ind.getSpo()));
//		System.out.println("------------------sop------------------");
//		System.out.println(ind.toString(ind.getSop()));
//		System.out.println("------------------ops------------------");
//		System.out.println(ind.toString(ind.getOps()));
//		System.out.println("------------------osp------------------");
//		System.out.println(ind.toString(ind.getOsp()));
//		System.out.println("------------------pso------------------");
//		System.out.println(ind.toString(ind.getPso()));
//		System.out.println("------------------pos------------------");
//		System.out.println(ind.toString(ind.getPos()));
		
		ajouterDansFichier(resultat_moi, "moi");
		if(activerJena) {
			ajouterDansFichier(resultat_jena, "jena");
			comparaison(resultat_moi, resultat_jena);
		}
		
		for (Map.Entry<Integer, String> entry : csv_temps_requete.entrySet()) {
            System.out.println("Temps pour la requete " + entry.getKey() + " : " + entry.getValue());
        }
		System.out.println("Temps total d’évaluation du workload : "+csv_general.get("tempsTotalWorkload"));
		
		csv_general.put("tempsTotal", String.valueOf(System.currentTimeMillis() - startTime1));
		ajouterDansCSV();
	}

	private static void parseQueries() throws FileNotFoundException, IOException {
		int compteur = 0;
		int total = 0;
		if(Integer.parseInt(pourcentageWarm) != 100 || permutation) {
			try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
				Iterator<String> lineIterator = lineStream.iterator();
				while (lineIterator.hasNext()){  
					String line = lineIterator.next();
					if (line.trim().endsWith("}")) {
						compteur++;
					}
				}
			}
			csv_general.put("nbr_requete", String.valueOf(compteur));
			total = compteur;
			compteur = (compteur * Integer.parseInt(pourcentageWarm)) / 100;
		}
		
		long startTime = System.currentTimeMillis();
		if(permutation) {
			List<Integer> aParcourir = new ArrayList<>();
			Random rand = new Random(); 
			while(aParcourir.size() < compteur) {
				int genere = rand.nextInt(total) + 1;
				if(!aParcourir.contains(genere)) {
					aParcourir.add(genere);
				}
			}
			
			try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
				SPARQLParser sparqlParser = new SPARQLParser();
				Iterator<String> lineIterator = lineStream.iterator();
				StringBuilder queryString = new StringBuilder();
				
				int i = 1;
				int j = 1;
				while (lineIterator.hasNext()){  
					String line = lineIterator.next();
					queryString.append(line);

					if (line.trim().endsWith("}")) {
						if(aParcourir.contains(i)) {
							ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);
							processAQuery(query, i);
							if(j == compteur) {
								break;
							}
							j+=1;
						}
						queryString.setLength(0);
						i+=1;
					}
				}
			}
		}else {
			try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
				SPARQLParser sparqlParser = new SPARQLParser();
				Iterator<String> lineIterator = lineStream.iterator();
				StringBuilder queryString = new StringBuilder();
				
				int i = 1;
				/* On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'On considère alors que c'est la fin d'une requête */
				while (lineIterator.hasNext()){  
					String line = lineIterator.next();
					queryString.append(line);

					if (line.trim().endsWith("}")) {
						ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);
						processAQuery(query, i);
						queryString.setLength(0);
						if(i == compteur) {
							System.out.println("Sortie "+i);
							break;
						}
						i+=1;
					}
				}
			}
		}
		csv_general.put("tempsLectureRequetes", String.valueOf(System.currentTimeMillis() - startTime));
	}
	
	private static void parseData() throws FileNotFoundException, IOException {
		long startTime = System.currentTimeMillis();
		try (Reader dataReader = new FileReader(dataFile)) {
			MainRDFHandler rdfHandler = new MainRDFHandler();
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
			rdfParser.setRDFHandler(rdfHandler);
			rdfParser.parse(dataReader, baseURI);
			statementList = rdfHandler.getStatementList();
		}
		csv_general.put("tempsLectureDonnees", String.valueOf(System.currentTimeMillis() - startTime));
	}
	
	private static void ajouterElementDictionnaire(Map<Integer, List<String>> d, int cle, String valeur, String choix) {
        if(choix.equals("jena")) {
        	if (!d.containsKey(cle)) 
	            d.put(cle, new ArrayList<>());
	        d.get(cle).add(valeur);
        }else if(choix.equals("moi")) {
        	if (!d.containsKey(cle)) 
	            d.put(cle, new ArrayList<>());
	        d.get(cle).add(valeur);
        }
    }
	
	private static void ajouterDansFichier(Map<Integer, List<String>> d, String nom) throws IOException {
		if(nom.equals("moi")) {
			writer1 = new BufferedWriter(new FileWriter(outputFile));
			for (Map.Entry<Integer, List<String>> entry : d.entrySet()) {
	            int key = entry.getKey();
	            List<String> values = entry.getValue();
	            writer1.write("Pour la requete "+key+":\n");
	    		if(values.size() == 0) {
					writer1.write("0 resultat trouve.\n");
				}else {
					for(String valeur : values) {
						writer1.write("Clé :"+dict.getCle(valeur)+", Valeur :"+valeur+"\n");
					}
				}
	    		writer1.write("--------------------\n");
	        }
			writer1.close();
		}else {
			writer2 = new BufferedWriter(new FileWriter(workingDir + "resultat_jena.txt"));
			for (Map.Entry<Integer, List<String>> entry : d.entrySet()) {
	            int key = entry.getKey();
	            List<String> values = entry.getValue();
	            writer2.write("Pour la requete "+key+":\n");
	    		if(values.size() == 0) {
					writer2.write("0 resultat trouve.\n");
				}else {
					for(String valeur : values) {
						writer2.write("Valeur :"+valeur+"\n");
					}
				}
	    		writer2.write("--------------------\n");
	        }
			writer2.close();
		}
    }
	
	private static void comparaison(Map<Integer, List<String>> d1, Map<Integer, List<String>> d2) {
        for (Map.Entry<Integer, List<String>> entry : d1.entrySet()) {
            Integer key = entry.getKey();
            if (!d2.containsKey(key)) {
                System.out.println("Pour la requete "+key+", les maps sont différentes");
                return;
            }
            List<String> list1 = entry.getValue();
            List<String> list2 = d2.get(key);

            if (list1.size() != list2.size()) {
                System.out.println("Les listes pour la requete " + key + " ont des tailles différentes");
                return;
            }
            if (!list1.containsAll(list2) || !list2.containsAll(list1)) {
                System.out.println("Les éléments pour la clé " + key + " sont différents");
                return;
            }
        }
        System.out.println("Les resultats sont identiques");
    }
	
	private static HashMap<String, String> parseArguments(String[] args) {
        HashMap<String, String> options = new HashMap<>();
        String currentOption = null;
        for (String arg : args) {
            if (arg.startsWith("-")) {
                currentOption = arg;
                options.put(currentOption, "");
            } else if (currentOption != null) {
                options.put(currentOption, arg);
                currentOption = null;
            }
        }
        return options;
    }
	
	private static void verificationParDefaut() {
        if(queryFile.equals("") || dataFile.equals("")) {
        	System.out.println("Vous n'avez pas saisi le chemin du fichier des donnes ou des requetes.");
        	System.exit(1);
        }
        if(outputFile.isEmpty()) 
        	outputFile = workingDir + "resultat_moi.txt";
        if(pourcentageWarm.isEmpty())
        	pourcentageWarm = "100";
        queryFile = workingDir+""+queryFile;
        dataFile = workingDir+""+dataFile;
    }
	
	private static void ajouterDansCSV() {
		String[] tab = {"Nom Fichier Données", "Nom Dossier Requetes", "Nombre Triplets RDF", "Nombre Requetes", "Temps Lecture Données", "Temps Lecture Requetes", "Temps Création Dico", "Nombre d’Index", "Temps Création Index", "Temps Total d’évaluation du workload", "Temps Total du Programme"};
		String filePath = outputFile.split("\\.")[0].concat(".csv");		
		
		try (CSVWriter writer = new CSVWriter(new FileWriter(filePath, true))) {
            if (new File(filePath).length() == 0) {
                writer.writeNext(tab);
            }
        	tab[0] = dataFile;
        	tab[1] = queryFile;
        	tab[2] = String.valueOf(statementList.size());
        	tab[3] = csv_general.get("nbr_requete");
        	tab[4] = csv_general.get("tempsLectureDonnees");
        	tab[5] = csv_general.get("tempsLectureRequetes");
        	tab[6] = csv_general.get("tempsDico");
        	tab[7] = "6";
        	tab[8] = csv_general.get("tempsIndex");
        	tab[9] = csv_general.get("tempsTotalWorkload");
        	tab[10] = csv_general.get("tempsTotal");
    		writer.writeNext(tab); 
        } catch (IOException e) {
            System.err.println("Erreur lors de la création du fichier CSV : " + e.getMessage());
        }
		
		if(export_query_results) {
			String[] tab2 = {"Numero requete", "Clé Dans Dic", "Valeur"};
			String[] tab3 = {"Numero requete", "Valeur"};
			String filePath1 = workingDir + "query_results_moi.csv";	
			String filePath2 = workingDir + "query_results_jena.csv";	
			
			try (CSVWriter writer = new CSVWriter(new FileWriter(filePath1))) {
	            writer.writeNext(tab2);
				for (Map.Entry<Integer, List<String>> entry : resultat_moi.entrySet()) {
		            int key = entry.getKey();
		            List<String> values = entry.getValue();
		            tab2[0] = String.valueOf(key);
		    		if(values.size() == 0) {
		    			tab2[1] = "0 resultat trouve.";
		    			tab2[2] = "0 resultat trouve.";
					}else {
						for(String valeur : values) {
							tab2[1] = String.valueOf(dict.getCle(valeur));
							tab2[2] = valeur;
							writer.writeNext(tab2);
						}
					} 
		        }
	        } catch (IOException e) {
	            System.err.println("Erreur lors de la création du fichier CSV : " + e.getMessage());
	        }
			
			if(activerJena) {
				try (CSVWriter writer = new CSVWriter(new FileWriter(filePath2))) {
		            writer.writeNext(tab3);
					for (Map.Entry<Integer, List<String>> entry : resultat_jena.entrySet()) {
			            int key = entry.getKey();
			            List<String> values = entry.getValue();
			            tab3[0] = String.valueOf(key);
			    		if(values.size() == 0) {
			    			tab3[1] = "0 resultat trouve.\n";
						}else {
							for(String valeur : values) {
								tab3[1] = valeur;
								writer.writeNext(tab3); 
							}
						}
			        }
		        } catch (IOException e) {
		            System.err.println("Erreur lors de la création du fichier CSV : " + e.getMessage());
		        }
			}
		}
    }
}