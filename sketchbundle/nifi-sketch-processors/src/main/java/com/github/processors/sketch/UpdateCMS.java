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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
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

public class UpdateCMS extends AbstractProcessor {
	private CountMinSketch cms = null;

	// Properties
	public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder().name("KEY").displayName("KEY")
			.description("Key Value to track").expressionLanguageSupported(true)
			.addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).required(true).build();

	public static final PropertyDescriptor VALUE = new PropertyDescriptor.Builder().name("VALUE").displayName("VALUE")
			.description("Delta Value to add in a counter").required(false).expressionLanguageSupported(true)
			.addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).build();

	public static final PropertyDescriptor DELTA = new PropertyDescriptor.Builder().name("DELTA").displayName("DELTA")
			.description("specifies the probability that the estimation is wrong (or confidence interval."
					+ "Default value is '0.01' which is 99% confidence level.)")
			.required(false).addValidator(StandardValidators.NUMBER_VALIDATOR).build();

	public static final PropertyDescriptor EPSILON = new PropertyDescriptor.Builder().name("EPSILON")
			.displayName("EPSILON")
			.description("specifies the error in estimation.The default value is '0.01' which is 1% estimation error")
			.required(false).addValidator(StandardValidators.NUMBER_VALIDATOR).build();

	public static final PropertyDescriptor SEED = new PropertyDescriptor.Builder().name("SEED").displayName("SEED")
			.description("The seed value for the murmur hash function").required(false)
			.addValidator(StandardValidators.INTEGER_VALIDATOR).build();

	public static final PropertyDescriptor WIDTH = new PropertyDescriptor.Builder().name("WIDTH").displayName("WIDTH")
			.description("The width of the sketch matrix").required(false)
			.addValidator(StandardValidators.INTEGER_VALIDATOR).build();

	public static final PropertyDescriptor DEPTH = new PropertyDescriptor.Builder().name("DEPTH").displayName("DEPTH")
			.description("The depth of the sketch matrix").required(false)
			.addValidator(StandardValidators.INTEGER_VALIDATOR).build();

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
		descriptors.add(VALUE);
		descriptors.add(DELTA);
		descriptors.add(EPSILON);
		descriptors.add(SEED);
		descriptors.add(WIDTH);
		descriptors.add(DEPTH);

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
		final PropertyValue delta = context.getProperty(DELTA);
		final PropertyValue epsilon = context.getProperty(EPSILON);
		final PropertyValue seed = context.getProperty(SEED);
		final PropertyValue width = context.getProperty(WIDTH);
		final PropertyValue depth = context.getProperty(DEPTH);

		if (null == cms) {
			if (notNull(delta.getValue()) && notNull(epsilon.getValue())) {
				final float d = delta.asFloat();
				final float e = epsilon.asFloat();
				if (notNull(seed.getValue())) {
					final int s = seed.asInteger();
					CountMinSketch.createInstance(d, e, s);
				} else {
					CountMinSketch.createInstance(d, e);
				}
			} else if (notNull(width.getValue()) && notNull(depth.getValue())) {
				final int w = width.asInteger();
				final int d = depth.asInteger();
				if (notNull(seed.getValue())) {
					final int s = seed.asInteger();
					CountMinSketch.createInstance(w, d, s);
				} else {
					CountMinSketch.createInstance(w, d);
				}
			} else {
				CountMinSketch.createInstance(SketchConstants.DEFAULT_DELTA, SketchConstants.DEFAULT_EPSILON);
			}
			cms = CountMinSketch.getInstance();
		}
	}

	private boolean notNull(final String property) {
		return property != null && !property.equals("");
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();

		long value = 0;
		if (notNull(context.getProperty(VALUE).getValue()))
			value = context.getProperty(VALUE).evaluateAttributeExpressions(flowFile).asLong();
		
		try {
			if (value != 0) {
				cms.update(key, value);
			} else {
				cms.update(key);
			}
		} catch (Throwable e) {
			getLogger().error("Ran into an error while processing {}.", new Object[] { flowFile }, e);
			session.transfer(flowFile, REL_FAILURE);
		} finally {
			session.transfer(flowFile, REL_SUCCESS);
		}
	}
}
