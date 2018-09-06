package com.ddmarley.nifi.processor;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
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
	public void testNormalAttributeDestAll() {
		
		this.runner.setProperty(ExtractTikaContent.EXTRACT_STRATEGY, ExtractTikaContent.STRATEGY_ALL);
		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.setProperty(ExtractTikaContent.ATTRIBUTE_NAME, "foobar");
		this.runner.setProperty(ExtractTikaContent.CONTENT_DEST, ExtractTikaContent.FLOW_FILE_ATTRIBUTE);
		this.runner.setProperty(ExtractTikaContent.FIELDNAME_FORMAT, ExtractTikaContent.FORMAT_LOWER);
		this.runner.setProperty(ExtractTikaContent.CONTENT_FIELDNAME, "COntENT");
		
		this.runner.enqueue(this.getClass().getResourceAsStream("/test_document.pdf"));
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0).isAttributeEqual(
				"tika_json",
				"{\"content\":\"\\nTest document… \\n\\n\\n\",\"date\":\"2018-09-06T13:51:27Z\",\"pdf_pdfversion\":\"1.5\",\"xmp_creatortool\":\"Microsoft® Word 2013\",\"access_permission_modify_annotations\":\"true\",\"access_permission_can_print_degraded\":\"true\",\"dc_creator\":\"Johannes Brucher\",\"dcterms_created\":\"2018-09-06T13:51:27Z\",\"last_modified\":\"2018-09-06T13:51:27Z\",\"dcterms_modified\":\"2018-09-06T13:51:27Z\",\"dc_format\":\"application/pdf; version=1.5\",\"last_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_docinfo_creator_tool\":\"Microsoft® Word 2013\",\"access_permission_fill_in_form\":\"true\",\"pdf_docinfo_modified\":\"2018-09-06T13:51:27Z\",\"meta_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_encrypted\":\"false\",\"modified\":\"2018-09-06T13:51:27Z\",\"content_type\":\"application/pdf\",\"pdf_docinfo_creator\":\"Johannes Brucher\",\"x_parsed_by\":[\"org.apache.tika.parser.DefaultParser\",\"org.apache.tika.parser.pdf.PDFParser\"],\"creator\":\"Johannes Brucher\",\"meta_author\":\"Johannes Brucher\",\"meta_creation_date\":\"2018-09-06T13:51:27Z\",\"created\":\"Thu Sep 06 15:51:27 CEST 2018\",\"access_permission_extract_for_accessibility\":\"true\",\"access_permission_assemble_document\":\"true\",\"xmptpg_npages\":\"1\",\"creation_date\":\"2018-09-06T13:51:27Z\",\"access_permission_extract_content\":\"true\",\"access_permission_can_print\":\"true\",\"author\":\"Johannes Brucher\",\"producer\":\"Microsoft® Word 2013\",\"access_permission_can_modify\":\"true\",\"pdf_docinfo_producer\":\"Microsoft® Word 2013\",\"pdf_docinfo_created\":\"2018-09-06T13:51:27Z\"}"));
	}

	@Test
	public void testNormalContentDestAll() {

		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.setProperty(ExtractTikaContent.EXTRACT_STRATEGY, ExtractTikaContent.STRATEGY_ALL);
		this.runner.setProperty(ExtractTikaContent.FIELDNAME_FORMAT, ExtractTikaContent.FORMAT_LOWER);
		this.runner.setProperty(ExtractTikaContent.CONTENT_DEST, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.enqueue(this.getClass().getResourceAsStream("/test_document.pdf"));
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0).isContentEqual(
				"{\"content\":\"\\nTest document… \\n\\n\\n\",\"date\":\"2018-09-06T13:51:27Z\",\"pdf_pdfversion\":\"1.5\",\"xmp_creatortool\":\"Microsoft® Word 2013\",\"access_permission_modify_annotations\":\"true\",\"access_permission_can_print_degraded\":\"true\",\"dc_creator\":\"Johannes Brucher\",\"dcterms_created\":\"2018-09-06T13:51:27Z\",\"last_modified\":\"2018-09-06T13:51:27Z\",\"dcterms_modified\":\"2018-09-06T13:51:27Z\",\"dc_format\":\"application/pdf; version=1.5\",\"last_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_docinfo_creator_tool\":\"Microsoft® Word 2013\",\"access_permission_fill_in_form\":\"true\",\"pdf_docinfo_modified\":\"2018-09-06T13:51:27Z\",\"meta_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_encrypted\":\"false\",\"modified\":\"2018-09-06T13:51:27Z\",\"content_type\":\"application/pdf\",\"pdf_docinfo_creator\":\"Johannes Brucher\",\"x_parsed_by\":[\"org.apache.tika.parser.DefaultParser\",\"org.apache.tika.parser.pdf.PDFParser\"],\"creator\":\"Johannes Brucher\",\"meta_author\":\"Johannes Brucher\",\"meta_creation_date\":\"2018-09-06T13:51:27Z\",\"created\":\"Thu Sep 06 15:51:27 CEST 2018\",\"access_permission_extract_for_accessibility\":\"true\",\"access_permission_assemble_document\":\"true\",\"xmptpg_npages\":\"1\",\"creation_date\":\"2018-09-06T13:51:27Z\",\"access_permission_extract_content\":\"true\",\"access_permission_can_print\":\"true\",\"author\":\"Johannes Brucher\",\"producer\":\"Microsoft® Word 2013\",\"access_permission_can_modify\":\"true\",\"pdf_docinfo_producer\":\"Microsoft® Word 2013\",\"pdf_docinfo_created\":\"2018-09-06T13:51:27Z\"}"));
	}
	
	@Test
	public void testNormalAttributeDestContentOnly() {

		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.setProperty(ExtractTikaContent.EXTRACT_STRATEGY, ExtractTikaContent.STRATEGY_CONTENT);
		this.runner.setProperty(ExtractTikaContent.FIELDNAME_FORMAT, ExtractTikaContent.FORMAT_LOWER);
		this.runner.setProperty(ExtractTikaContent.CONTENT_DEST, ExtractTikaContent.FLOW_FILE_ATTRIBUTE);
		this.runner.enqueue(this.getClass().getResourceAsStream("/test_document.pdf"));
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0).isAttributeEqual(
				"tika_json",
				"{\"content\":\"\\nTest document… \\n\\n\\n\"}"));
	}

	@Test
	public void testNormalContentDestContentOnly() {

		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.setProperty(ExtractTikaContent.EXTRACT_STRATEGY, ExtractTikaContent.STRATEGY_CONTENT);
		this.runner.setProperty(ExtractTikaContent.FIELDNAME_FORMAT, ExtractTikaContent.FORMAT_LOWER);
		this.runner.setProperty(ExtractTikaContent.CONTENT_DEST, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.enqueue(this.getClass().getResourceAsStream("/test_document.pdf"));
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0)
				.isContentEqual("{\"content\":\"\\nTest document… \\n\\n\\n\"}"));
	}
	
	@Test
	public void testNormalAttributeDestMetaOnly() {

		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.setProperty(ExtractTikaContent.EXTRACT_STRATEGY, ExtractTikaContent.STRATEGY_META);
		this.runner.setProperty(ExtractTikaContent.FIELDNAME_FORMAT, ExtractTikaContent.FORMAT_LOWER);
		this.runner.setProperty(ExtractTikaContent.CONTENT_DEST, ExtractTikaContent.FLOW_FILE_ATTRIBUTE);
		this.runner.enqueue(this.getClass().getResourceAsStream("/test_document.pdf"));
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0).isAttributeEqual(
				"tika_json",
				"{\"date\":\"2018-09-06T13:51:27Z\",\"pdf_pdfversion\":\"1.5\",\"xmp_creatortool\":\"Microsoft® Word 2013\",\"access_permission_modify_annotations\":\"true\",\"access_permission_can_print_degraded\":\"true\",\"dc_creator\":\"Johannes Brucher\",\"dcterms_created\":\"2018-09-06T13:51:27Z\",\"last_modified\":\"2018-09-06T13:51:27Z\",\"dcterms_modified\":\"2018-09-06T13:51:27Z\",\"dc_format\":\"application/pdf; version=1.5\",\"last_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_docinfo_creator_tool\":\"Microsoft® Word 2013\",\"access_permission_fill_in_form\":\"true\",\"pdf_docinfo_modified\":\"2018-09-06T13:51:27Z\",\"meta_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_encrypted\":\"false\",\"modified\":\"2018-09-06T13:51:27Z\",\"content_type\":\"application/pdf\",\"pdf_docinfo_creator\":\"Johannes Brucher\",\"x_parsed_by\":[\"org.apache.tika.parser.DefaultParser\",\"org.apache.tika.parser.pdf.PDFParser\"],\"creator\":\"Johannes Brucher\",\"meta_author\":\"Johannes Brucher\",\"meta_creation_date\":\"2018-09-06T13:51:27Z\",\"created\":\"Thu Sep 06 15:51:27 CEST 2018\",\"access_permission_extract_for_accessibility\":\"true\",\"access_permission_assemble_document\":\"true\",\"xmptpg_npages\":\"1\",\"creation_date\":\"2018-09-06T13:51:27Z\",\"access_permission_extract_content\":\"true\",\"access_permission_can_print\":\"true\",\"author\":\"Johannes Brucher\",\"producer\":\"Microsoft® Word 2013\",\"access_permission_can_modify\":\"true\",\"pdf_docinfo_producer\":\"Microsoft® Word 2013\",\"pdf_docinfo_created\":\"2018-09-06T13:51:27Z\"}"));
	}

	@Test
	public void testNormalContentDestMetaOnly() {

		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.setProperty(ExtractTikaContent.EXTRACT_STRATEGY, ExtractTikaContent.STRATEGY_META);
		this.runner.setProperty(ExtractTikaContent.FIELDNAME_FORMAT, ExtractTikaContent.FORMAT_LOWER);
		this.runner.setProperty(ExtractTikaContent.CONTENT_DEST, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.enqueue(this.getClass().getResourceAsStream("/test_document.pdf"));
		this.runner.run();

		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0)
				.isContentEqual("{\"date\":\"2018-09-06T13:51:27Z\",\"pdf_pdfversion\":\"1.5\",\"xmp_creatortool\":\"Microsoft® Word 2013\",\"access_permission_modify_annotations\":\"true\",\"access_permission_can_print_degraded\":\"true\",\"dc_creator\":\"Johannes Brucher\",\"dcterms_created\":\"2018-09-06T13:51:27Z\",\"last_modified\":\"2018-09-06T13:51:27Z\",\"dcterms_modified\":\"2018-09-06T13:51:27Z\",\"dc_format\":\"application/pdf; version=1.5\",\"last_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_docinfo_creator_tool\":\"Microsoft® Word 2013\",\"access_permission_fill_in_form\":\"true\",\"pdf_docinfo_modified\":\"2018-09-06T13:51:27Z\",\"meta_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_encrypted\":\"false\",\"modified\":\"2018-09-06T13:51:27Z\",\"content_type\":\"application/pdf\",\"pdf_docinfo_creator\":\"Johannes Brucher\",\"x_parsed_by\":[\"org.apache.tika.parser.DefaultParser\",\"org.apache.tika.parser.pdf.PDFParser\"],\"creator\":\"Johannes Brucher\",\"meta_author\":\"Johannes Brucher\",\"meta_creation_date\":\"2018-09-06T13:51:27Z\",\"created\":\"Thu Sep 06 15:51:27 CEST 2018\",\"access_permission_extract_for_accessibility\":\"true\",\"access_permission_assemble_document\":\"true\",\"xmptpg_npages\":\"1\",\"creation_date\":\"2018-09-06T13:51:27Z\",\"access_permission_extract_content\":\"true\",\"access_permission_can_print\":\"true\",\"author\":\"Johannes Brucher\",\"producer\":\"Microsoft® Word 2013\",\"access_permission_can_modify\":\"true\",\"pdf_docinfo_producer\":\"Microsoft® Word 2013\",\"pdf_docinfo_created\":\"2018-09-06T13:51:27Z\"}"));
	}
	
	@Test
	public void testNormalContentInAtrributeDestContent() throws IOException {
		
		this.runner.setProperty(ExtractTikaContent.EXTRACT_STRATEGY, ExtractTikaContent.STRATEGY_ALL);
		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_ATTRIBUTE);
		this.runner.setProperty(ExtractTikaContent.ATTRIBUTE_NAME, "foobar");
		this.runner.setProperty(ExtractTikaContent.CONTENT_DEST, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.setProperty(ExtractTikaContent.FIELDNAME_FORMAT, ExtractTikaContent.FORMAT_UPPER);
		this.runner.setProperty(ExtractTikaContent.CONTENT_FIELDNAME, "COntENT");

		Map<String,String> att = new HashMap<>();
		att.put("foobar", Base64.encodeBase64String(IOUtils.toByteArray(this.getClass().getResourceAsStream("/test_document.pdf"))));
		this.runner.enqueue("", att);
		this.runner.run();
		
		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0).isContentEqual(
				"{\"CONTENT\":\"\\nTest document… \\n\\n\\n\",\"DATE\":\"2018-09-06T13:51:27Z\",\"PDF_PDFVERSION\":\"1.5\",\"XMP_CREATORTOOL\":\"Microsoft® Word 2013\",\"ACCESS_PERMISSION_MODIFY_ANNOTATIONS\":\"true\",\"ACCESS_PERMISSION_CAN_PRINT_DEGRADED\":\"true\",\"DC_CREATOR\":\"Johannes Brucher\",\"DCTERMS_CREATED\":\"2018-09-06T13:51:27Z\",\"LAST_MODIFIED\":\"2018-09-06T13:51:27Z\",\"DCTERMS_MODIFIED\":\"2018-09-06T13:51:27Z\",\"DC_FORMAT\":\"application/pdf; version=1.5\",\"LAST_SAVE_DATE\":\"2018-09-06T13:51:27Z\",\"PDF_DOCINFO_CREATOR_TOOL\":\"Microsoft® Word 2013\",\"ACCESS_PERMISSION_FILL_IN_FORM\":\"true\",\"PDF_DOCINFO_MODIFIED\":\"2018-09-06T13:51:27Z\",\"META_SAVE_DATE\":\"2018-09-06T13:51:27Z\",\"PDF_ENCRYPTED\":\"false\",\"MODIFIED\":\"2018-09-06T13:51:27Z\",\"CONTENT_TYPE\":\"application/pdf\",\"PDF_DOCINFO_CREATOR\":\"Johannes Brucher\",\"X_PARSED_BY\":[\"org.apache.tika.parser.DefaultParser\",\"org.apache.tika.parser.pdf.PDFParser\"],\"CREATOR\":\"Johannes Brucher\",\"META_AUTHOR\":\"Johannes Brucher\",\"META_CREATION_DATE\":\"2018-09-06T13:51:27Z\",\"CREATED\":\"Thu Sep 06 15:51:27 CEST 2018\",\"ACCESS_PERMISSION_EXTRACT_FOR_ACCESSIBILITY\":\"true\",\"ACCESS_PERMISSION_ASSEMBLE_DOCUMENT\":\"true\",\"XMPTPG_NPAGES\":\"1\",\"CREATION_DATE\":\"2018-09-06T13:51:27Z\",\"ACCESS_PERMISSION_EXTRACT_CONTENT\":\"true\",\"ACCESS_PERMISSION_CAN_PRINT\":\"true\",\"AUTHOR\":\"Johannes Brucher\",\"PRODUCER\":\"Microsoft® Word 2013\",\"ACCESS_PERMISSION_CAN_MODIFY\":\"true\",\"PDF_DOCINFO_PRODUCER\":\"Microsoft® Word 2013\",\"PDF_DOCINFO_CREATED\":\"2018-09-06T13:51:27Z\"}"));
	}
	
	@Test
	public void testNormalContentInAtrributeDestAttribute() throws IOException {
		
		this.runner.setProperty(ExtractTikaContent.EXTRACT_STRATEGY, ExtractTikaContent.STRATEGY_ALL);
		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_ATTRIBUTE);
		this.runner.setProperty(ExtractTikaContent.ATTRIBUTE_NAME, "foobar");
		this.runner.setProperty(ExtractTikaContent.CONTENT_DEST, ExtractTikaContent.FLOW_FILE_ATTRIBUTE);
		this.runner.setProperty(ExtractTikaContent.FIELDNAME_FORMAT, ExtractTikaContent.FORMAT_LOWER);
		this.runner.setProperty(ExtractTikaContent.CONTENT_FIELDNAME, "woop:woop");

		Map<String,String> att = new HashMap<>();
		att.put("foobar", Base64.encodeBase64String(IOUtils.toByteArray(this.getClass().getResourceAsStream("/test_document.pdf"))));
		this.runner.enqueue("", att);
		this.runner.run();
		
		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);

		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0).isAttributeEqual(
				"tika_json",
				"{\"woop_woop\":\"\\nTest document… \\n\\n\\n\",\"date\":\"2018-09-06T13:51:27Z\",\"pdf_pdfversion\":\"1.5\",\"xmp_creatortool\":\"Microsoft® Word 2013\",\"access_permission_modify_annotations\":\"true\",\"access_permission_can_print_degraded\":\"true\",\"dc_creator\":\"Johannes Brucher\",\"dcterms_created\":\"2018-09-06T13:51:27Z\",\"last_modified\":\"2018-09-06T13:51:27Z\",\"dcterms_modified\":\"2018-09-06T13:51:27Z\",\"dc_format\":\"application/pdf; version=1.5\",\"last_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_docinfo_creator_tool\":\"Microsoft® Word 2013\",\"access_permission_fill_in_form\":\"true\",\"pdf_docinfo_modified\":\"2018-09-06T13:51:27Z\",\"meta_save_date\":\"2018-09-06T13:51:27Z\",\"pdf_encrypted\":\"false\",\"modified\":\"2018-09-06T13:51:27Z\",\"content_type\":\"application/pdf\",\"pdf_docinfo_creator\":\"Johannes Brucher\",\"x_parsed_by\":[\"org.apache.tika.parser.DefaultParser\",\"org.apache.tika.parser.pdf.PDFParser\"],\"creator\":\"Johannes Brucher\",\"meta_author\":\"Johannes Brucher\",\"meta_creation_date\":\"2018-09-06T13:51:27Z\",\"created\":\"Thu Sep 06 15:51:27 CEST 2018\",\"access_permission_extract_for_accessibility\":\"true\",\"access_permission_assemble_document\":\"true\",\"xmptpg_npages\":\"1\",\"creation_date\":\"2018-09-06T13:51:27Z\",\"access_permission_extract_content\":\"true\",\"access_permission_can_print\":\"true\",\"author\":\"Johannes Brucher\",\"producer\":\"Microsoft® Word 2013\",\"access_permission_can_modify\":\"true\",\"pdf_docinfo_producer\":\"Microsoft® Word 2013\",\"pdf_docinfo_created\":\"2018-09-06T13:51:27Z\"}"));
	}

	@Test
	public void testEmptyProperties() {
		this.runner.enqueue(this.getClass().getResourceAsStream("/test_document.pdf"));
		this.runner.run();
		
		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 1);
		
		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_SUCCESS).get(0)
				.isContentEqual("{\"content\":\"\\nTest document… \\n\\n\\n\"}"));
	}
	
	@Test
	public void testWrongPropertiesNoContentInAttributes() throws IOException {
		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_ATTRIBUTE);
		String content = IOUtils.toString(this.getClass().getResourceAsStream("/test_document.pdf"), StandardCharsets.UTF_8);
		this.runner.enqueue(content);
		this.runner.run();
		
		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_ORIGINAL, 1);
		
		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_ORIGINAL).get(0)
				.isContentEqual(content));
	}
	
	@Test
	public void testWrongPropertiesNoContentInContent() throws IOException {
		this.runner.setProperty(ExtractTikaContent.CONTENT_LOCATION, ExtractTikaContent.FLOW_FILE_CONTENT);
		this.runner.enqueue("");
		this.runner.run();
		
		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_ORIGINAL, 1);
		
		assertTrue(this.runner.getFlowFilesForRelationship(ExtractTikaContent.REL_ORIGINAL).get(0)
				.isContentEqual(""));
	}
	
	@Test
	public void testEmptyAll() {
		this.runner.run();
		
		this.runner.assertQueueEmpty();
		this.runner.assertTransferCount(ExtractTikaContent.REL_FAILURE, 0);
		this.runner.assertTransferCount(ExtractTikaContent.REL_SUCCESS, 0);
		this.runner.assertTransferCount(ExtractTikaContent.REL_ORIGINAL, 0);
	}
}
