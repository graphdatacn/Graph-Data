package generator.generator;

import static org.junit.Assert.assertNotNull;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public void testApp()
    {
    	copyGenerator cg=new copyGenerator();
		cg.copyGenerate("E:/go", "301.n3");
		File file=new File("E:/go");
		File[] files=file.listFiles();
		assertNotNull("Data have been created",files);
    }
}
