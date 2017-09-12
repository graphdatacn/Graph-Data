package generator.generator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashSet;

import org.apache.jena.ontology.DatatypeProperty;
import org.apache.jena.ontology.ObjectProperty;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.iterator.ExtendedIterator;

public class OWLParser {
	private HashSet<String> namedClasses = new HashSet<String>();
	private HashSet<String> objectProperties = new HashSet<String>();
	private HashSet<String> dataTypeProperties = new HashSet<String>();

	public void parser(String owlFile) {
		PrintStream out = System.out;
		try {
			System.setOut(new PrintStream(new FileOutputStream(new File("parse.txt"))));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		OntModel model = ModelFactory.createOntologyModel();
		model.read(owlFile);
		int countclass = 0;
		ExtendedIterator<OntClass> listNamedClasses = model.listNamedClasses();
		while (listNamedClasses.hasNext()) {
			OntClass next = listNamedClasses.next();
			System.out.println(next.getURI());
			getNamedClasses().add(next.getURI());
			countclass++;
		}
		System.out.println(countclass);
		ExtendedIterator<ObjectProperty> listObjectProperties = model.listObjectProperties();
		while (listObjectProperties.hasNext()) {
			ObjectProperty property = listObjectProperties.next();
			System.out.println(property.getURI());
			getObjectProperties().add(property.getURI());
		}
		System.out.println("********************");
		ExtendedIterator<DatatypeProperty> listDatatypeProperties = model.listDatatypeProperties();
		while(listDatatypeProperties.hasNext()){
			DatatypeProperty property = listDatatypeProperties.next();
			System.out.println(property.getURI());
			getDataTypeProperties().add(property.getURI());
		}
		System.setOut(out);
	}

	public static void main(String[] args) {
		new OWLParser().parser("owl/aggregate_correct.owl");
	}

	public HashSet<String> getNamedClasses() {
		return namedClasses;
	}

	public void setNamedClasses(HashSet<String> namedClasses) {
		this.namedClasses = namedClasses;
	}

	public HashSet<String> getObjectProperties() {
		return objectProperties;
	}

	public void setObjectProperties(HashSet<String> objectProperties) {
		this.objectProperties = objectProperties;
	}

	public HashSet<String> getDataTypeProperties() {
		return dataTypeProperties;
	}

	public void setDataTypeProperties(HashSet<String> dataTypeProperties) {
		this.dataTypeProperties = dataTypeProperties;
	}
}

