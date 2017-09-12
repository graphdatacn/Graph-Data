package generator.generator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.*;
import java.util.Random;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.Statement;

class copyGenerator {
public List<String> fileList=new ArrayList<String> ();
	
	public void fileCreator(String directory,String fileName){
//		List<String> list=new ArrayList<String> ();
		int i=1;
		while(i<=9){
			String nm="new_"+fileName+"_"+i+".n3";
			fileList.add(nm);
			File file=new File(directory,nm);
			try{
				file.createNewFile();
			}catch(Exception e){
				e.printStackTrace();
			}
			i++;
		}
	}
	
	public void dataCreator(String directory,String dataTo,String dataFrom,int num){
		File wfile=new File(directory,dataTo);
		FileOutputStream fos=null;
		try{
			int countStatement=0;
			HashMap<String,String> sub=new HashMap<String,String>();
			fos=new FileOutputStream(wfile);
			Model model=ModelFactory.createDefaultModel();
			String road=directory+"/"+dataFrom;
			model.read(road);
			StmtIterator statements=model.listStatements();
			while(statements.hasNext()){
				Statement statement=statements.next();
				String subject=statement.getSubject().toString();
				String object=statement.getObject().toString();
				String predict="<"+statement.getPredicate().toString()+">";
				String newSubject;
				String newObject="";
				if(sub.containsKey(subject)){
					newSubject=sub.get(subject);
				}else{
					newSubject="<"+subject+"."+num+"."+countStatement+">";
					sub.put(subject,newSubject);
					countStatement++;
				}
				if(!object.startsWith("http://gcm.wdcm.org/ontology")){
					if(statement.getObject().isResource()){
						if(sub.containsKey(object)){
							newObject=sub.get(object);
						}else{
							newObject="<"+object+"."+num+"."+countStatement+">";
							sub.put(object,newObject);
							countStatement++;
						}
					}else if(statement.getObject().isLiteral()){
						if(object.indexOf("\\")>=0){
							newObject="\"this string contains special symbal\"";
						}
						else{
							newObject="\""+object+"\"";
						}
					}
				}else{
					newObject="<"+object+">";
				}
				String triple=newSubject+" "+predict+" "+newObject+" ."+"\n";
				byte[] bt=new byte[2048];
				bt=triple.getBytes();
				fos.write(bt,0,bt.length);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try{
				fos.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public void copyGenerate(String dir,String dataFile){
		fileCreator(dir,dataFile);
		for(int i=0;i<fileList.size();i++){
			dataCreator(dir,fileList.get(i),dataFile,i);
		}
		connectionCreator(dir);
	}
	
	public void connectionCreator(String directory){
		for(int i=0;i<fileList.size()-1;i++){
			String nm="connection_"+i+".n3";
			File fileX=new File(directory,nm);
			try{
				fileX.createNewFile();
			}catch(Exception e){
				e.printStackTrace();
			}
			Model modela=ModelFactory.createDefaultModel();
			Model modelb=ModelFactory.createDefaultModel();
			String namea=fileList.get(i);
			String nameb=fileList.get(i+1);
			String roada=directory+"/"+namea;
			String roadb=directory+"/"+nameb;
			modela.read(roada);
			modelb.read(roadb);
			StmtIterator statementsa=modela.listStatements();
			StmtIterator statementsb=modelb.listStatements();
			FileOutputStream fos=null;
			try{
				fos=new FileOutputStream(fileX);
			while(statementsa.hasNext()){
				Statement statementa=statementsa.next();
				String predicatea="<"+statementa.getPredicate().toString()+">";
				String subjecta="<"+statementa.getSubject().toString()+">";
				String objecta="<"+statementa.getObject().toString()+">";
				while(statementsb.hasNext()){
					Statement statementb=statementsb.next();
					String predicateb="<"+statementb.getPredicate().toString()+">";
					String subjectb="<"+statementb.getSubject().toString()+">";
					String objectb="<"+statementb.getObject().toString()+">";
					//create connection
					Random random=new Random();
					int c=random.nextInt(10);
					if(c>4){
						int a=random.nextInt(10);
						int b=random.nextInt(10);
//						FileOutputStream fos=null;
//						try{
//							fos=new FileOutputStream(fileX);
							String triple="";
							if(a>4 && b>4){
								triple=subjecta+" "+predicatea+" "+subjectb+" ."+"\n";
							}else if(a>4 && b<=4){
								triple=subjecta+" "+predicateb+" "+objectb+" ."+"\n";
							}else if(a<=4 && b>4){
								triple=objecta+" "+predicatea+" "+subjectb+" ."+"\n";
							}else if(a<=4 && b<=4){
								triple=objecta+" "+predicateb+" "+objectb+" ."+"\n";
							}
							byte[] bt=new byte[2048];
							bt=triple.getBytes();
							fos.write(bt,0,bt.length); 
						/*}catch(Exception e){
							e.printStackTrace();
						}finally{
							try{
								fos.close();
							}catch(Exception e){
								e.printStackTrace();
							}
						}*/
					}
				}
			}
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				try{
					fos.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}
	}
}
