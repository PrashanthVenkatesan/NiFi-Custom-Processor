/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.processors.sketch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "Attribute Expression Language", "counter", "data science", "cms", "sketch" })
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Count Min sketch is a probabilistic data structure for finding the frequency of events in a stream of data.")
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class GetCMS extends AbstractProcessor {
	private CountMinSketch cms = null;
	// Properties
	public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder().name("KEY").displayName("KEY")
			.description("Key Value to track").expressionLanguageSupported(true)
			.addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).required(true).build();

	// relationships
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.description("All FlowFiles are successfully processed are routed here").name("success").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("When a FlowFile fails for a some reason").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(KEY);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
		if (cms == null)
			cms = CountMinSketch.getInstance();

		long estimatedCount = 0;
		try {
			estimatedCount = cms.getEstimatedCount(key);
		} catch (NullPointerException e) {
			estimatedCount = 0;
		} catch (Throwable e) {
			getLogger().error("Ran into an error while processing {}.", new Object[] { flowFile }, e);
			session.transfer(flowFile, REL_FAILURE);
		} finally {
			flowFile = session.putAttribute(flowFile, key, String.valueOf(estimatedCount));
			getLogger().info("Counter: Key {} - Value {}", new Object[] { key , estimatedCount});
			session.transfer(flowFile, REL_SUCCESS);
		}
	}
}
