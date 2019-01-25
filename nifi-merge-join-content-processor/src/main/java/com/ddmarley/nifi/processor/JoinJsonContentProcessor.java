package com.ddmarley.nifi.processor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.bin.Bin;
import org.apache.nifi.processor.util.bin.BinProcessingResult;
import org.apache.nifi.processors.standard.MergeContent;
import org.apache.nifi.processors.standard.merge.AttributeStrategy;
import org.apache.nifi.processors.standard.merge.AttributeStrategyUtil;

import com.jayway.jsonpath.JsonPath;

@Tags({ "JSON", "join", "merge", "content" })
@CapabilityDescription("Merge multiple FlowFiles and join there content into a single FlowFile.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({
		@ReadsAttribute(attribute = "Correlation Attribute Name", description = "If specified, like FlowFiles will be binned together, where 'like FlowFiles' means FlowFiles that have the same value for \"\r\n" + 
				"                    + \"this Attribute. If not specified, FlowFiles are bundled by the order in which they are pulled from the queue") })
public class JoinJsonContentProcessor extends MergeContent {
	
	private String mimeType = "application/json";

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(CORRELATION_ATTRIBUTE_NAME);
		descriptors.add(AttributeStrategyUtil.ATTRIBUTE_STRATEGY);
		descriptors.add(MIN_ENTRIES);
		descriptors.add(MAX_ENTRIES);
		descriptors.add(MIN_SIZE);
		descriptors.add(MAX_SIZE);
		descriptors.add(MAX_BIN_AGE);
		descriptors.add(MAX_BIN_COUNT);
		return descriptors;
	}

	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_ORIGINAL);
		relationships.add(REL_FAILURE);
		relationships.add(REL_MERGED);
		return relationships;
	}

	@Override
	protected BinProcessingResult processBin(final Bin bin, final ProcessContext context) throws ProcessException {

		final BinProcessingResult binProcessingResult = new BinProcessingResult(true);
		final AttributeStrategy attributeStrategy = AttributeStrategyUtil.strategyFor(context);

		final List<FlowFile> contents = bin.getContents();
		final ProcessSession session = bin.getSession();
		FlowFile bundle = session.create(bin.getContents());
		
		final AtomicReference<String> bundleMimeTypeRef = new AtomicReference<>(null);
		try {
			bundle = session.write(bundle, new OutputStreamCallback() {
				@Override
				public void process(final OutputStream out) throws IOException {
					final LinkedHashMap<String, Object> joinedMap = new LinkedHashMap<>();
					boolean isFirst = true;
					final Iterator<FlowFile> itr = contents.iterator();
					while (itr.hasNext()) {
						final FlowFile flowFile = itr.next();
						bin.getSession().read(flowFile, false, new InputStreamCallback() {
							@Override
							public void process(final InputStream in) throws IOException {
								joinedMap.putAll(JsonPath.parse(in).read("$"));
							}
						});

						final String flowFileMimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
						if (isFirst) {
							bundleMimeTypeRef.set(flowFileMimeType);
							isFirst = false;
						} else {
							if (bundleMimeTypeRef.get() != null
									&& !bundleMimeTypeRef.get().equals(flowFileMimeType)) {
								bundleMimeTypeRef.set(null);
							}
						}
					}
					out.write(JsonPath.parse(joinedMap).jsonString().getBytes(StandardCharsets.UTF_8));
				}
			});
		} catch (final Exception e) {
			session.remove(bundle);
			throw e;
		}

		session.getProvenanceReporter().join(contents, bundle);
		bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents));
		if (bundleMimeTypeRef.get() != null) {
			this.mimeType = bundleMimeTypeRef.get();
		}

		// keep the filename, as it is added to the bundle.
		final String filename = bundle.getAttribute(CoreAttributes.FILENAME.key());

		// merge all of the attributes
		final Map<String, String> bundleAttributes = attributeStrategy.getMergedAttributes(contents);
		bundleAttributes.put(CoreAttributes.MIME_TYPE.key(), this.getMergedContentType());
		// restore the filename of the bundle
		bundleAttributes.put(CoreAttributes.FILENAME.key(), filename);
		bundleAttributes.put(MERGE_COUNT_ATTRIBUTE, Integer.toString(contents.size()));
		bundleAttributes.put(MERGE_BIN_AGE_ATTRIBUTE, Long.toString(bin.getBinAge()));
		bundle = session.putAllAttributes(bundle, bundleAttributes);

		final String inputDescription = contents.size() < 10 ? contents.toString() : contents.size() + " FlowFiles";
		getLogger().info("Merged {} into {}", new Object[] { inputDescription, bundle });
		session.transfer(bundle, REL_MERGED);
		binProcessingResult.getAttributes().put(MERGE_UUID_ATTRIBUTE, bundle.getAttribute(CoreAttributes.UUID.key()));

		for (final FlowFile unmerged : this.getUnmergedFlowFiles()) {
			final FlowFile unmergedCopy = session.clone(unmerged);
			session.transfer(unmergedCopy, REL_FAILURE);
		}

		// We haven't committed anything, parent will take care of it
		binProcessingResult.setCommitted(false);
		return binProcessingResult;
	}
	
	public String getMergedContentType() {
		return mimeType;
	}
	
	public List<FlowFile> getUnmergedFlowFiles() {
		return Collections.emptyList();
	}

    private String createFilename(final List<FlowFile> flowFiles) {
        if (flowFiles.size() == 1) {
            return flowFiles.get(0).getAttribute(CoreAttributes.FILENAME.key());
        } else {
            final FlowFile ff = flowFiles.get(0);
            final String origFilename = ff.getAttribute(SEGMENT_ORIGINAL_FILENAME);
            if (origFilename != null) {
                return origFilename;
            } else {
                return String.valueOf(System.nanoTime());
            }
        }
    }
}
