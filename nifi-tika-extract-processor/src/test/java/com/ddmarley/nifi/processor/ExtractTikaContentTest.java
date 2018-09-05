package com.ddmarley.nifi.processor;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class ExtractTikaContentTest {
	
	private TestRunner runner;
	
	@Before
	public void initBefore() {
		this.runner = TestRunners.newTestRunner(new ExtractTikaContent());
	}
	
	@Test
	public void testNormal() {
		
		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.setProperty(ExtractTikaContent.EXTRACT_METADATA_ONLY, "false");
		this.runner.enqueue(this.getClass().getResourceAsStream("/Lucidworks_Enterprise_Search_in_2025.pdf"));
		
		this.runner.run();
		

		
		
	}

}
