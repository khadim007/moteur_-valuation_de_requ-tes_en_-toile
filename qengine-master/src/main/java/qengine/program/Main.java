package qengine.program;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.eclipse.rdf4j.model.Statement;

/**
 * Programme simple lisant un fichier de requête et un fichier de données.
 * 
 * <p>
 * Les entrées sont données ici de manière statique,
 * à vous de programmer les entrées par passage d'arguments en ligne de commande comme demandé dans l'énoncé.
 * </p>
 * 
 * <p>
 * Le présent programme se contente de vous montrer la voie pour lire les triples et requêtes
 * depuis les fichiers ; ce sera à vous d'adapter/réécrire le code pour finalement utiliser les requêtes et interroger les données.
 * On ne s'attend pas forcémment à ce que vous gardiez la même structure de code, vous pouvez tout réécrire.
 * </p>
 * 
 * @author Olivier Rodriguez <olivier.rodriguez1@umontpellier.fr>
 */
final class Main {
	static final String baseURI = null;
	static final String workingDir = "data/";

//	static final String queryFile = workingDir + "sample_query.queryset";
	static final String queryFile = workingDir + "test.queryset";
//	static final String queryFile = workingDir + "STAR_ALL_workload.queryset";

//	static final String dataFile = workingDir + "sample_data.nt";
	static final String dataFile = workingDir + "test.nt";
//	static final String dataFile = workingDir + "100K.nt";
	
	private static List<Statement> statementList;
	private static Dictionnaire dict;
	private static Index ind;
	private static BufferedWriter writer;
	
	/**
	 * Méthode utilisée ici lors du parsing de requête sparql pour agir sur l'objet obtenu.
	 * @throws IOException 
	 */
	public static void processAQuery(ParsedQuery query) throws IOException {
		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
		String predicate = null;
		String object = null;
		int keyPredicate = -1;
		int keyObject = -1;
		List<Integer> commun = new ArrayList<>();
		int i = 0;
		for (StatementPattern pattern : patterns) {
			predicate = pattern.getPredicateVar().getValue().toString();
			object = pattern.getObjectVar().getValue().toString();
			keyPredicate = dict.getCle(predicate);
			keyObject = dict.getCle(object);
			List<Integer> res = ind.getSubject("ops", keyPredicate, keyObject);
			if(keyPredicate == -1 || keyObject == -1) {
				System.out.println("Predicate ou Object n'existe pas dans les donnes");
				writer.write("Predicate ou Object n'existe pas dans les donnes\n");
				System.out.println("--------------------");
				writer.write("--------------------\n");
				return;
			}
			if(res == null) {
				System.out.println("Combinaison entre Predicate et Object non trouve.");
				writer.write("Combinaison entre Predicate et Object non trouve.\n");
				System.out.println("--------------------");
				writer.write("--------------------\n");
				return;
			}
			if(i == 0) {
				commun.addAll(res);
			}else {
				commun.retainAll(res);
			}
			i+=1;
        }
		if(commun.size() == 0) {
			System.out.println("0 resutat trouve.");
			writer.write("0 resutat trouve.\n");
		}
		for(int cle : commun) {
			System.out.println("Clé :"+cle+", Valeur :"+dict.getValeur(cle));
			writer.write("Clé :"+cle+", Valeur :"+dict.getValeur(cle)+"\n");
			
		}
		System.out.println("--------------------");
		writer.write("--------------------\n");
	}
	
	
	public static void executerRequeteJena() throws IOException {
		Model model = ModelFactory.createDefaultModel();
        model.read(dataFile);
        
        try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			SPARQLParser sparqlParser = new SPARQLParser();
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();
			
			int i = 0;
//			System.out.println("Pour la requete "+i+":");
//			writer.write("Pour la requete "+i+":\n");
			while (lineIterator.hasNext()){  
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					String requeteSPARQL = queryString.toString();
		            System.out.println("Pour la requête " + (i + 1) + ":");
		            System.out.println(requeteSPARQL);

		            try (QueryExecution qexec = QueryExecutionFactory.create(requeteSPARQL, model)) {
		                ResultSet resultSet = qexec.execSelect();
		                ResultSetFormatter.out(resultSet);
		            }
					queryString.setLength(0);
					i+=1;
//					if(lineIterator.hasNext()) {
////						System.out.println("Pour la requete "+i+":");
////						writer.write("Pour la requete "+i+":\n");
//					}
				}
			}
		}
    }


	public static void main(String[] args) throws Exception {
		parseData();
		
		dict = new Dictionnaire(statementList);
		dict.creationDictonnaire();
		
		ind = new Index(statementList, dict.getDict());
		ind.creationIndexHexastore();

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
		
		parseQueries();
		
//		executerRequeteJena();
	}


	/**
	 * Traite chaque requête lue dans {@link #queryFile} avec {@link #processAQuery(ParsedQuery)}.
	 */
	private static void parseQueries() throws FileNotFoundException, IOException {
		/**
		 * Try-with-resources
		 * 
		 * @see <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">Try-with-resources</a>
		 */
		writer = new BufferedWriter(new FileWriter(workingDir + "resultat_moi.txt"));
		try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			SPARQLParser sparqlParser = new SPARQLParser();
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();
			
			int i = 1;
			System.out.println("Pour la requete "+i+":");
			writer.write("Pour la requete "+i+":\n");
			/* On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'On considère alors que c'est la fin d'une requête */
			while (lineIterator.hasNext()){  
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);
					processAQuery(query);
					queryString.setLength(0);
					i+=1;
					if(lineIterator.hasNext()) {
						System.out.println("Pour la requete "+i+":");
						writer.write("Pour la requete "+i+":\n");
					}
				}
			}
		}
		writer.close();
	}
	

	/**
	 * Traite chaque triple lu dans {@link #dataFile} avec {@link MainRDFHandler}.
	 */
	private static void parseData() throws FileNotFoundException, IOException {
		
		try (Reader dataReader = new FileReader(dataFile)) {
			MainRDFHandler rdfHandler = new MainRDFHandler();
			// On va parser des données au format ntriples
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);

			// On utilise notre implémentation de handler
			rdfParser.setRDFHandler(rdfHandler);

			// Parsing et traitement de chaque triple par le handler
			rdfParser.parse(dataReader, baseURI);
			statementList = rdfHandler.getStatementList();
		}
		// Récupérer la liste de Statement depuis MainRDFHandler  
	}
	
	private static String lireFichier(String cheminFichier) {
        try {
            return new String(Files.readAllBytes(Paths.get(cheminFichier)));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}