package com.ddmarley.nifi.processor;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class FetchFilesTest {
	
	private TestRunner runner;

	@Before
	public void initBefore() {
		this.runner = TestRunners.newTestRunner(new FetchFiles());
	}

	@Test
	public void testNormalSingleDir() throws IOException {
		
		this.runner.setProperty(FetchFiles.FILEPATH, this.getClass().getResource("/single_folder").getPath());
		this.runner.enqueue("");
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(FetchFiles.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(FetchFiles.REL_SUCCESS).get(0).isContentEqual(IOUtils.toString(this.getClass().getResourceAsStream("/single_folder/test_document.pdf"), StandardCharsets.UTF_8)));;
	}
	
	@Test
	public void testNormalNestedDir() throws IOException {
		
		this.runner.setProperty(FetchFiles.FILEPATH, this.getClass().getResource("/nested_folder").getPath());
		this.runner.enqueue("");
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(FetchFiles.REL_SUCCESS, 2);

		assertTrue(this.runner.getFlowFilesForRelationship(FetchFiles.REL_SUCCESS).get(0).isContentEqual(IOUtils.toString(this.getClass().getResourceAsStream("/nested_folder/data/lvl1/test_document_v2.pdf"), StandardCharsets.UTF_8)));;
		assertTrue(this.runner.getFlowFilesForRelationship(FetchFiles.REL_SUCCESS).get(1).isContentEqual(IOUtils.toString(this.getClass().getResourceAsStream("/nested_folder/data/test_document.pdf"), StandardCharsets.UTF_8)));;
	}

}
