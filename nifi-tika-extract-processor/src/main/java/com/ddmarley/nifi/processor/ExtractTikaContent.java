package com.ddmarley.nifi.processor;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Tags({ "Tika", "Extract", "Content", "Metadata", "PDF", "Word", "Rich", "Documents" })
@CapabilityDescription("Run Apache Tika to extract content and/or metadata on rich documents. The extracted results are stored as JSON object either in the FlowFiles content area or in its attributes.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({ @ReadsAttribute(attribute = "Attribute name", description = "The configured 'Attribute name' where to locate the content to extract") })
@WritesAttributes({ @WritesAttribute(attribute = "tika_json", description = "If content destination is attribute") })
public class ExtractTikaContent extends AbstractProcessor {

	static final String FLOW_FILE_CONTENT = "flowFileContent";
	static final String FLOW_FILE_ATTRIBUTE = "flowFileAttribute";
	
	static final String STRATEGY_CONTENT = "contentOnly";
	static final String STRATEGY_META = "metadataOnly";
	static final String STRATEGY_ALL = "extractAll";

	static final String FORMAT_UPPER = "uppercase";
	static final String FORMAT_LOWER = "lowercase";
	static final String FIELDNAME_CONTENT = "content";

	static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original").description("").build();
	static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("").build();
	static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("").build();

	static final PropertyDescriptor EXTRACT_STRATEGY = new PropertyDescriptor.Builder()
			.name("Extract strategy").description("Weather to extract content only, metadata only or even both.")
			.required(true).allowableValues(STRATEGY_CONTENT, STRATEGY_META, STRATEGY_ALL)
			.defaultValue(STRATEGY_CONTENT).build();

	static final PropertyDescriptor CONTENT_LOCATION = new PropertyDescriptor.Builder().name("Content location")
			.description("Where the binary content is located. This could be the content of the incoming FlowFile, "
					+ "or in an attribute of the incoming FlowFile. If it's an attribute, we assume the content string is base64 encoded!")
			.required(true).allowableValues(FLOW_FILE_CONTENT, FLOW_FILE_ATTRIBUTE)
			.defaultValue(FLOW_FILE_CONTENT).build();

	static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder().name("Attribute name")
			.description("The name of the attribute where to find the base64 encoded content.").required(false).addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
			.build();

	static final PropertyDescriptor CONTENT_DEST = new PropertyDescriptor.Builder().name("Content destination")
			.description("The place where we should write the extracted content, either the FlowFile content or its attribute. "
					+ "If attribute is choosen, the attribute key is 'tika_json'")
			.required(true).allowableValues(FLOW_FILE_CONTENT, FLOW_FILE_ATTRIBUTE)
			.defaultValue(FLOW_FILE_CONTENT).build();

	static final PropertyDescriptor FIELDNAME_FORMAT = new PropertyDescriptor.Builder().name("Field name format")
			.description("The format of all field names. It's either upper case or lower case.")
			.required(false).allowableValues(FORMAT_UPPER, FORMAT_LOWER).defaultValue(FORMAT_LOWER)
			.build();
	
	static final PropertyDescriptor CONTENT_FIELDNAME = new PropertyDescriptor.Builder().name("Field name of content")
			.description("The name of the content field, default is " + FIELDNAME_CONTENT)
			.required(false).addValidator(StandardValidators.NON_BLANK_VALIDATOR).defaultValue(FIELDNAME_CONTENT)
			.build();

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(CONTENT_LOCATION);
		properties.add(ATTRIBUTE_NAME);
		properties.add(EXTRACT_STRATEGY);
		properties.add(CONTENT_DEST);
		properties.add(FIELDNAME_FORMAT);
		properties.add(CONTENT_FIELDNAME);
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
		// Nothing to do without a flowFile
		if (flowFile == null) {
			return;
		}

		final String extractStrategy = context.getProperty(EXTRACT_STRATEGY).getValue();
		final String contentLocation = context.getProperty(CONTENT_LOCATION).getValue();

		final AtomicReference<TikaResults> results = new AtomicReference<TikaResults>(null);
		final AtomicReference<InputStream> stream = new AtomicReference<InputStream>(null);
		if (contentLocation.equals(FLOW_FILE_CONTENT)) {
			session.read(flowFile, in -> {
				stream.set(new BufferedInputStream(in));
			});
			try {
				if(stream.get().available() == 0) {
					getLogger().warn("Could not get binary content from flowFile content. Routing to " + REL_ORIGINAL);
					session.transfer(flowFile, REL_ORIGINAL);
					return;
				}
				results.set(this.ExtractWithTika(stream.get(), extractStrategy));
			} catch (TikaException | IOException e) {
				getLogger().error("Tika extraction failed for content binary: " + e.getMessage());
				session.transfer(flowFile, REL_FAILURE);
				return;
			}
		} else if (contentLocation.equals(FLOW_FILE_ATTRIBUTE)) {
			final String attContent = flowFile.getAttribute(context.getProperty(ATTRIBUTE_NAME).getValue());
			if(StringUtils.isBlank(attContent)) {
				getLogger().warn("Could not get binary content from attributes. Routing to " + REL_ORIGINAL);
				session.transfer(flowFile, REL_ORIGINAL);
				return;
			}
			try {
				results.set(this.ExtractWithTika(new ByteArrayInputStream(Base64.decodeBase64(attContent)),
						extractStrategy));
			} catch (TikaException | IOException e) {
				getLogger().error("Tika extraction failed for attribute binary: " + e.getMessage());
				session.transfer(flowFile, REL_FAILURE);
				return;
			}
		}

		// Write Tika results into destination:
		final TikaResults res = results.get();
		final ObjectMapper mapper = new ObjectMapper();
		final ObjectNode jsonDoc = mapper.createObjectNode();
		// Put possible content into jsonDoc:
		if(StringUtils.isNotBlank(res.getContent())) {
			jsonDoc.put(this.toValidFieldName(context.getProperty(CONTENT_FIELDNAME).getValue(), context), res.getContent());
		}
		// Put possible metadata into jsonDoc:
		for (String name : res.getMetadata().names()) {
			String[] vals = res.getMetadata().getValues(name);
			name = this.toValidFieldName(name, context);
			if (vals.length > 1) {
				ArrayNode jarray = jsonDoc.putArray(name);
				Arrays.stream(vals).forEach(v -> jarray.add(v));
			} else if (vals.length == 1) {
				jsonDoc.put(name, vals[0]);
			}
		}
		try {
			System.out.println(mapper.writeValueAsString(jsonDoc));
		} catch (JsonProcessingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// Write jsonDoc into flowFile destination:
		try {
			String destination = context.getProperty(CONTENT_DEST).getValue();
			if (destination.equals(FLOW_FILE_CONTENT)) {
				flowFile = session.write(flowFile, out -> {
					out.write(mapper.writeValueAsString(jsonDoc).getBytes(StandardCharsets.UTF_8));
				});
			} else if (destination.equals(FLOW_FILE_ATTRIBUTE)) {
				flowFile = session.putAttribute(flowFile, "tika_json", mapper.writeValueAsString(jsonDoc));
			}
		} catch (JsonProcessingException e) {
			getLogger().error("Error in write 'Json to String': " + e.getMessage());
			session.transfer(flowFile, REL_FAILURE);
			return;
		}
		// In case everything is ok, transfer to success
		session.transfer(flowFile, REL_SUCCESS);
	}

	/**
	 * Extracts the given content with Tika.
	 * @param content The content as an Inputstream
	 * @param extractStrategy The extractStrategy based on {@code EXTRACT_STRATEGY}
	 * @return The extract results (content and metadata) wrapped in a {@code TikaResults} object
	 */
	protected TikaResults ExtractWithTika(InputStream content, String extractStrategy) throws TikaException, IOException {

		Tika tika = new Tika();
		Metadata metadata = new Metadata();
		// If we want metadata only, we have to set the write limit to 0
		int writeLimit = 0;
		if (!extractStrategy.equals(STRATEGY_META)) {
			// If we want content extraction as well we disable any write limitations at
			// all --> '-1'!
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
		if(extractStrategy.equals(STRATEGY_CONTENT)) {
			return new TikaResults(handler.toString(), new Metadata());
		} else {
			return new TikaResults(handler.toString(), metadata);
		}
	}

	/**
	 * Make sure to provide valid field names:<br>
	 * 1. Any none character is substituted with '_'<br>
	 * 2. Based on {@code FIELDNAME_FORMAT} transform to upper or lower case. 
	 * @param fname Field name to process
	 * @param context The Nifi ProcessContext
	 * @return The beautified field name
	 */
	protected String toValidFieldName(String fname, ProcessContext context) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fname.length(); i++) {
			char ch = fname.charAt(i);
			if (!Character.isLetterOrDigit(ch)) {
				ch = '_';
			} else if (context.getProperty(FIELDNAME_FORMAT).getValue().equals(FORMAT_LOWER)) {
				ch = Character.toLowerCase(ch);
			} else if (context.getProperty(FIELDNAME_FORMAT).getValue().equals(FORMAT_UPPER)) {
				ch = Character.toUpperCase(ch);
			}
			sb.append(ch);
		}
		return sb.toString();
	}

	/**
	 * Helper Class storing the Tika results in one object.
	 */
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
