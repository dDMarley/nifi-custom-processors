package com.ddmarley.nifi.processor;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.SAXException;

@Tags({ "Tika", "Extract", "Content", "Metadata", "PDF", "Word" })
@CapabilityDescription("Run Apache Tika Text Extraction from PDF, Word, Excel.   Parameter for HTML or TEXT output.  Parameter for Maximum Length returned.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ExtractTikaContent extends AbstractProcessor {

	static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original").description("").build();
	static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("").build();
	static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("").build();

	static final PropertyDescriptor EXTRACT_METADATA_ONLY = new PropertyDescriptor.Builder()
			.name("Extract metadata only").description("").required(true).allowableValues("true", "false")
			.defaultValue("false").build();

	static final String FLOW_FILE_CONTENT = "flowFileContent";
	static final String FLOW_FILE_ATTRIBUTE = "flowFileAttribute";
	static final PropertyDescriptor CONTENT_LOCATION = new PropertyDescriptor.Builder().name("Content location")
			.description("").required(false).allowableValues("flowFileContent", "flowFileAttribute")
			.defaultValue("flowFileContent").build();

	static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder().name("attribute name")
			.description("").required(false).addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
			.build();

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(EXTRACT_METADATA_ONLY);
		properties.add(CONTENT_LOCATION);
		properties.add(ATTRIBUTE_NAME);
		return properties;
	}

	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> rels = new HashSet<>();
		rels.add(REL_ORIGINAL);
		rels.add(REL_SUCCESS);
		rels.add(REL_FAILURE);
		return rels;
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		final boolean metadataOnly = context.getProperty(EXTRACT_METADATA_ONLY).asBoolean();
		final String contentLocation = context.getProperty(CONTENT_LOCATION).getValue();

		final AtomicReference<TikaResults> results = new AtomicReference<TikaResults>(null);
		if (contentLocation.equals(FLOW_FILE_CONTENT)) {
			session.read(flowFile, in -> {
				try {
					results.set(this.ExtractWithTika(new BufferedInputStream(in), metadataOnly));
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
			});
		} else if (contentLocation.equals(FLOW_FILE_ATTRIBUTE)) {

			final String attContent = flowFile.getAttribute(context.getProperty(ATTRIBUTE_NAME).getValue());
			try {
				results.set(this.ExtractWithTika(new ByteArrayInputStream(attContent.getBytes(StandardCharsets.UTF_8)),
						metadataOnly));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Write Tika results into JSON:
		TikaResults res = results.get();

	}

	protected TikaResults ExtractWithTika(InputStream content, boolean metadataOnly) throws IOException, TikaException {

		Tika tika = new Tika();
		Metadata metadata = new Metadata();
		// If we want metadata only, we have to set the write limit to 0
		int writeLimit = 0;
		if (!metadataOnly) {
			// If we want content extraction as well we disable any write limitations at all!
			writeLimit = -1;
		}
		WriteOutContentHandler handler = new WriteOutContentHandler(writeLimit);
		
		Parser parser = new AutoDetectParser();
		ParseContext context = new ParseContext();
		context.set(Parser.class, parser);
		
		String mimeType = tika.detect(content);
		metadata.set(Metadata.CONTENT_TYPE, mimeType);
		try {
			parser.parse(content, new BodyContentHandler(handler), metadata, context);
		} catch (SAXException e) {
			if (!handler.isWriteLimitReached(e)) {
                // This should never happen with BodyContentHandler...
                throw new TikaException("Unexpected SAX processing failure", e);
            }
		} finally {
			content.close();
		}
		
		return new TikaResults(handler.toString(), metadata);
	}
	
	public class TikaResults {
		private String content;
		private Metadata metadata;
		
		public TikaResults(String content, Metadata metadata) {
			this.content = content;
			this.metadata = metadata;
		}
		
		public String getContent() {
			return content;
		}
		public Metadata getMetadata() {
			return metadata;
		}
	}
}
