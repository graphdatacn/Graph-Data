package generator.generator;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;
import generator.generator.Constants;

public class ImportData {
	/**
	 * default is 1GB.
	 */
	public static long BATCHSIZE = 1 * 1024 * 1024 * 1024;
	public static long TXSIZE = 1000;
	public static String TITANCONFIFILE = null;
	private static Properties conf = new Properties();
	private static Logger logger = LoggerFactory.getLogger(ImportData.class);
	private static TitanGraph graph = null;
	public static final String type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	public static final String defaultType = "http://gcm.wdcm.org/ontology/gcmAnnotation/v1/DEFAULT";
	public static final String titanGraphId = "vertexID";
	public static final String titanDefaultLabel = "defaultLabel";
	static {
		loadConf();
		start();
	}

	/**
	 * 从文件中读取灵活配置的静态信息
	 */
	static void loadConf() {
		try {
			conf.load(new FileInputStream(new File("conf/import.properties")));
			BATCHSIZE = Long.parseLong(conf.getProperty(Constants.MAXBATCHSIZENAME, "1073741824"));
			TITANCONFIFILE = conf.getProperty(Constants.TITANCONFIFILENAME,"conf/titan-hbase-es.properties");
			TXSIZE =Long.parseLong(conf.getProperty(Constants.TXSIZENAME,"1000"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 关闭一下graph
	 */
	static void close() {
		graph.close();
	}
	/**
	 * 开启graph
	 */
	static void start() {
		long start = System.currentTimeMillis();
		graph = TitanFactory.open(TITANCONFIFILE);
		logger.info("connected titan takes time:" + (System.currentTimeMillis() - start) + "ms");
		System.out.println("connected titan takes time:" + (System.currentTimeMillis() - start) + "ms");
		System.out.println("connected!!");
	}

	private long loadSize = 0;
	private long batchTimes = 0;

	/**
	 * 从指定的文件或者文件夹中导入指定后缀名字的RDF文件
	 * 
	 * @param dirOrFileStr
	 * @param nameSuffixes
	 */
	public void importDataFromDir(String dirOrFileStr, final String[] nameSuffixes) {
		File dirOrFile = new File(dirOrFileStr);
		if (dirOrFile.isDirectory()) {
			File[] subDirOrFiles = dirOrFile.listFiles(new FileFilter() {
				public boolean accept(File pathname) {
					boolean result = false;
					for (String nameSufix : nameSuffixes) {
						if (result)
							break;
						result |= pathname.getName().endsWith(nameSufix);
					}
					return result;
				}
			});
			for (File subDirOrFile : subDirOrFiles) {
				importDataFromDir(subDirOrFile.getAbsolutePath(), nameSuffixes);
			}
		} else {
			setDataFile(dirOrFile.getAbsolutePath());
//////////////////////////////////////////////////////////////////////////////////
			try {
				parseDataFromFile();
				forceDataToDB();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				parsingLogger.error(dirOrFile.getAbsolutePath() + " is not exist!!!");
			}
//////////////////////////////////////////////////////////////////////////////////
		}
	}

	public static void main(String[] args) {
		try {
			ImportData importData = new ImportData();
			if (args.length == 0) {
				importData.buildSchemaByOWL("owl/aggregate_correct.owl");
				importData.importDataFromDir("tmpData/kegg1.n3", new String[] { ".n3" });
			} else {
				importData.buildSchemaByOWL(args[0]);
				importData.importDataFromDir(args[1], Arrays.copyOfRange(args, 2, args.length));
			}
		} finally {
			System.out.println("closing graph..");
			ImportData.close();
		}
	}

	class PO {
		String predict;
		String object;
	}

	private HashMap<String, ArrayList<PO>> properties = null;
	private HashMap<String, ArrayList<PO>> edges = null;
	private HashMap<String, String> classes = null;
	private OWLParser owlParser = new OWLParser();
	private Logger aboundentlogger = LoggerFactory.getLogger("aboundentlogger");
	private Logger parsingLogger = LoggerFactory.getLogger("parsingLogger");
	private String dataFile;

	/**
	 * 从文件中解析出类、属性、边相关信息。
	 * 
	 * @param dataFile
	 * @throws FileNotFoundException
	 */
	public void parseDataFromFile() throws FileNotFoundException {
		System.out.println("Parsing dataFile:" + getDataFile());
		logger.info("Parsing dataFile:" + getDataFile());
		int count = 0;
		properties = new HashMap<String,ArrayList<PO>>();
		edges = new HashMap<String,ArrayList<PO>>();
		classes = new HashMap<String,String>();
		Model model = ModelFactory.createDefaultModel();
		model.read(getDataFile());
		StmtIterator listStatements = model.listStatements();
		while (listStatements.hasNext()) {
			Statement next = listStatements.next();
			count++;
			String subject = next.getSubject().toString();
			if (!classes.containsKey(subject)) {
				Statement property = model.getProperty(next.getSubject(), model.getProperty(type));
				if (property == null) {
					parsingLogger.error("Parsing dataFile:" + getDataFile() + "--> " + subject + " no type!");
					classes.put(subject, defaultType);
				} else {
					classes.put(subject, property.getObject().toString().intern());
				}
			}
			if (next.getObject().isLiteral()) {
				PO tempPo = new PO();
				tempPo.predict = next.getPredicate().toString();
				tempPo.object = next.getObject().toString();
				if (properties.containsKey(subject)) {
					properties.get(subject).add(tempPo);
				} else {
					ArrayList<PO> tempList = new ArrayList<PO>();
					tempList.add(tempPo);
					properties.put(subject, tempList);
				}
			}
			if (next.getObject().isResource()) {
				if (!next.getPredicate().toString().equals(type)) {
					PO tempPo = new PO();
					tempPo.predict = next.getPredicate().toString();
					tempPo.object = next.getObject().toString();
					if (edges.containsKey(subject)) {
						edges.get(subject).add(tempPo);
					} else {
						ArrayList<PO> tempList = new ArrayList<PO>();
						tempList.add(tempPo);
						edges.put(subject, tempList);
					}
				}
			}
		}
		model.close();
		logger.info("Parsed file:" + getDataFile() + "--> class count:" + classes.size() + "; property count:"
				+ properties.size() + "; edge count:" + edges.size() + "; Triple count" + count);
	}

	/**
	 * 根据owl文件中定义的本体来创建schema
	 * 
	 * @param owlFile
	 */
	public void buildSchemaByOWL(String owlFile) {
		owlParser.parser(owlFile);
		// add a default type
		owlParser.getNamedClasses().add(defaultType);

		TitanManagement mgmt = graph.openManagement();

		System.out.println("building vertex schema......");
		if (!mgmt.containsVertexLabel(titanDefaultLabel)){
			mgmt.makeVertexLabel(titanDefaultLabel).make();
		}else{
			return;
		}
		// HashSet<String> classes = owlParser.getNamedClasses();
		// for (String tempClass : classes) {
		// if (!mgmt.containsVertexLabel(tempClass)) {
		// mgmt.makeVertexLabel(tempClass).make();
		// }
		// }
		System.out.println("building property schema......");
		HashSet<String> dataTypeProperties = owlParser.getDataTypeProperties();
		for (String tempdataTypeProperties : dataTypeProperties) {
			if (!mgmt.containsPropertyKey(tempdataTypeProperties)) {
				mgmt.makePropertyKey(tempdataTypeProperties).cardinality(Cardinality.SET).dataType(String.class).make();
			}
		}
		System.out.println("building edge schema......");
		HashSet<String> objectProperties = owlParser.getObjectProperties();
		for (String tempObjectProperties : objectProperties) {
			if (!mgmt.containsEdgeLabel(tempObjectProperties)) {
				mgmt.makeEdgeLabel(tempObjectProperties).multiplicity(Multiplicity.MULTI).make();
			}
		}
		PropertyKey graphIDProperty = mgmt.makePropertyKey(titanGraphId).dataType(String.class).make();
		// create a unique index
		String indexStr = "graphID";
		mgmt.buildIndex(indexStr, Vertex.class).addKey(graphIDProperty).unique().buildCompositeIndex();
		mgmt.commit();
		try {
			ManagementSystem.awaitGraphIndexStatus(graph, indexStr).status(SchemaStatus.ENABLED).call().getSucceeded();
			mgmt = graph.openManagement();
			mgmt.updateIndex(mgmt.getGraphIndex(indexStr), SchemaAction.REINDEX).get();
			mgmt.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("It is already built schema.");
	}

	/**
	 * with index:load data takes time:38838ms<br/>
	 * without index:load data takes time:141570ms
	 */
	public void forceDataToDB() {
		// 记录下在这次打开数据库后导入到数据库中的数据量的大小
		long loadCount = 0;
		File forceFile = new File(dataFile);
		loadSize += forceFile.length();
		System.out.println("force " + dataFile + " 's data to DB....");
		logger.info("force " + dataFile + " 's data to DB....");
		TitanTransaction tx = graph.newTransaction();
		GraphTraversalSource traversal = graph.traversal();
		int classCount = 0;
		int propertyCount = 0;
		int edgeCount = 0;
		try {
			long start = System.currentTimeMillis();
			// create vertices and set related properties
			for (String temp : classes.keySet()) {
				// Make sure that this class is contained in owl
				if (owlParser.getNamedClasses().contains(classes.get(temp))) {
					// Vertex tempVertex = traversal.V().has(titanGraphId,
					// temp).hasNext()
					// ? traversal.V().has(titanGraphId, temp).next()
					// : graph.addVertex(type, classes.get(temp), titanGraphId,
					// temp);
					loadCount++;
					if(loadCount>TXSIZE){
						tx.commit();
					}
					Vertex tempVertex = null;
					GraphTraversal<Vertex, Vertex> tempVertexTemp = traversal.V().has(titanGraphId, temp);
					if (tempVertexTemp.hasNext()) {
						tempVertex = tempVertexTemp.next();
					} else {
						tempVertex = graph.addVertex(type, classes.get(temp), titanGraphId, temp);
					}
					classCount++;
					ArrayList<PO> tempProperties = properties.get(temp);
					if (tempProperties != null) {
						for (PO tempproperty : tempProperties) {
							// Make sure that this datetypeProperty is contained
							// in owl
							if (owlParser.getDataTypeProperties().contains(tempproperty.predict)) {
								tempVertex.property(tempproperty.predict, tempproperty.object);
								propertyCount++;
							} else {
								aboundentlogger.error("forcing " + dataFile + "--> the datatype predict "
										+ tempproperty.predict + " of " + temp + " is not in owl");
							}
						}
					}
					// add edges
					ArrayList<PO> tempEdges = edges.get(temp);
					if (tempEdges != null) {
						for (PO tempEdge : tempEdges) {
							// Make sure that this objecttypeProperty is
							// contained in owl
							if (owlParser.getObjectProperties().contains(tempEdge.predict)) {
								Vertex target = null;
								// 修改了原来的三元符语句，减少一次查询
								GraphTraversal<Vertex, Vertex> targetTemp = traversal.V().has(titanGraphId,
										tempEdge.object);
								if (targetTemp.hasNext()) {
									target = targetTemp.next();
								} else {
									target = graph.addVertex(titanGraphId, tempEdge.object);
								}
								tempVertex.addEdge(tempEdge.predict, target);
								edgeCount++;
							} else {
								aboundentlogger.error("forcing " + dataFile + "--> the objecttype predict "
										+ tempEdge.predict + " of " + temp + " is not in owl");
							}
						}
					}
				} else {
					aboundentlogger.error("forcing " + dataFile + "--> the type of " + temp + " is not in owl");
				}
			}
			tx.commit();
			System.out.println(
					"forced " + dataFile + " 's data to DB;takes time:" + (System.currentTimeMillis() - start) + "ms");
			logger.info("forced " + dataFile + " 's data to DB; takes time:" + (System.currentTimeMillis() - start)
					+ "ms; forced class:" + classCount + ".forced datatype:" + propertyCount + ".forced objecttype:"
					+ edgeCount);
		} catch (Exception e) {
			System.out.println("error in forcing" + dataFile + ";  " + e.getMessage());
			logger.info("error in forcing" + dataFile + ";  " + e.getMessage());
			e.printStackTrace();
			tx.rollback();
		}
		if (loadSize > BATCHSIZE) {
			// 如果在这次打开数据库后导入的数据量超过额定值，关闭graph然后再打开。
			batchTimes++;
			System.out.println("batch " + batchTimes + " close graph and open graph..........");
			logger.info("batch " + batchTimes + " close graph..........");
			long start = System.currentTimeMillis();
			close();
			logger.info(
					"batch " + batchTimes + " close graph takes time:" + (System.currentTimeMillis() - start) + "ms");
			logger.info("batch " + batchTimes + " open graph..........");
			start = System.currentTimeMillis();
			start();
			logger.info(
					"batch " + batchTimes + " open graph takes time:" + (System.currentTimeMillis() - start) + "ms");
			loadSize = 0;
		}
	}

	public String getDataFile() {
		return dataFile;
	}

	public void setDataFile(String dataFile) {
		this.dataFile = dataFile;
	}
}

