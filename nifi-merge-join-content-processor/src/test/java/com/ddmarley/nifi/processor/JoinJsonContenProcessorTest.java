package com.ddmarley.nifi.processor;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.processors.standard.MergeContent;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class JoinJsonContenProcessorTest {

	private TestRunner runner;

	@Before
	public void initBefore() {
		this.runner = TestRunners.newTestRunner(new JoinJsonContentProcessor());
	}
	
	@Test
	public void testNormalAll() {
		Map<String,String> att1 = new HashMap<>();
		att1.put("id", "1");
		Map<String,String> att2 = new HashMap<>();
		att1.put("id", "2");
		
		this.runner.enqueue("{\"id\":\"1\"}", att1);
		this.runner.enqueue("{\"id\":\"2\"}", att2);
		this.runner.enqueue("{\"foo\":\"1\"}", att1);
		this.runner.enqueue("{\"bar\":\"2\"}", att2);
		
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(MergeContent.REL_MERGED, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0).isContentEqual(
				"{\"id\":\"2\",\"foo\":\"1\",\"bar\":\"2\"}"));
		
	}

	@Test
	public void testNormalWithID() {
		this.runner.setProperty(JoinJsonContentProcessor.CORRELATION_ATTRIBUTE_NAME, "id");
		this.runner.setProperty(JoinJsonContentProcessor.MIN_ENTRIES, "2");
		
		Map<String,String> att1 = new HashMap<>();
		att1.put("id", "1");
		Map<String,String> att2 = new HashMap<>();
		att1.put("id", "2");
		
		this.runner.enqueue("{\"id\":\"1\"}", att1);
		this.runner.enqueue("{\"id\":\"2\"}", att2);
		this.runner.enqueue("{\"foo\":\"1\"}", att1);
		this.runner.enqueue("{\"bar\":\"2\"}", att2);
		
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(MergeContent.REL_MERGED, 2);

		assertTrue(this.runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0).isContentEqual(
				"{\"id\":\"2\",\"bar\":\"2\"}"));
		assertTrue(this.runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(1).isContentEqual(
				"{\"id\":\"1\",\"foo\":\"1\"}"));
	}
	
	@Test
	public void testNormalWithIDAnd5FF() {
		this.runner.setProperty(JoinJsonContentProcessor.CORRELATION_ATTRIBUTE_NAME, "id");
		this.runner.setProperty(JoinJsonContentProcessor.MIN_ENTRIES, "2");
		this.runner.setProperty(JoinJsonContentProcessor.MAX_ENTRIES, "2");
		
		Map<String,String> att1 = new HashMap<>();
		att1.put("id", "1");
		Map<String,String> att2 = new HashMap<>();
		att1.put("id", "2");
		
		this.runner.enqueue("{\"id\":\"1\"}", att1);
		this.runner.enqueue("{\"bla\":\"blub\"}", att1);
		this.runner.enqueue("{\"id\":\"2\"}", att2);
		this.runner.enqueue("{\"foo\":\"1\"}", att1);
		this.runner.enqueue("{\"bar\":\"2\"}", att2);
		
		this.runner.run();

		this.runner.assertQueueNotEmpty();
		this.runner.assertTransferCount(MergeContent.REL_MERGED, 2);

		assertTrue(this.runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0).isContentEqual(
				"{\"id\":\"2\",\"bar\":\"2\"}"));
		assertTrue(this.runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(1).isContentEqual(
				"{\"id\":\"1\",\"bla\":\"blub\"}"));
	}
}
