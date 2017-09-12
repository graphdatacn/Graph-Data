package generator.generator;

import java.io.File;

public class App 
{
    public static void main( String[] args )
    {
    	copyGenerator cg=new copyGenerator();
		String dircty=args[0];
		File fileN=new File(dircty);
		File[] fileList=fileN.listFiles();
		for(File f:fileList){
			String fname=f.getName();
			cg.copyGenerate(dircty,fname);
		}
    }
}
