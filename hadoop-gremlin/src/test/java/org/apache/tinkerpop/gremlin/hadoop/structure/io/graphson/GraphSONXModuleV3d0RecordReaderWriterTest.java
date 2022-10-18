/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.RecordReaderWriterTest;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraphSONXModuleV3d0RecordReaderWriterTest extends RecordReaderWriterTest {

    private static final Logger logger = LoggerFactory.getLogger(GraphSONXModuleV3d0RecordReaderWriterTest.class);

    public GraphSONXModuleV3d0RecordReaderWriterTest() {
        // should be default
        // super.configuration.set(Constants.GREMLIN_HADOOP_GRAPHSON_VERSION, GraphSONVersion.V3_0.name());
    }

    @Override
    protected String getInputFilename() {
        return "tinkerpop-classic-byteid-v3d0.json";
    }

    @Override
    protected Class<? extends InputFormat<NullWritable, VertexWritable>> getInputFormat() {
        return GraphSONInputFormat.class;
    }

    @Override
    protected Class<? extends OutputFormat<NullWritable, VertexWritable>> getOutputFormat() {
        return GraphSONOutputFormat.class;
    }

    @Override
    protected void validateFileSplits(final List<FileSplit> fileSplits, final Configuration configuration,
                                      final Class<? extends InputFormat<NullWritable, VertexWritable>> inputFormatClass,
                                      final Optional<Class<? extends OutputFormat<NullWritable, VertexWritable>>> outFormatClass) throws Exception {

        final InputFormat inputFormat = ReflectionUtils.newInstance(inputFormatClass, configuration);
        final TaskAttemptContext job = new TaskAttemptContextImpl(configuration, new TaskAttemptID(UUID.randomUUID().toString(), 0, TaskType.MAP, 0, 0));

        int vertexCount = 0;
        int outEdgeCount = 0;
        int inEdgeCount = 0;

        final OutputFormat<NullWritable, VertexWritable> outputFormat = outFormatClass.isPresent() ? ReflectionUtils.newInstance(outFormatClass.get(), configuration) : null;
        final RecordWriter<NullWritable, VertexWritable> writer = null == outputFormat ? null : outputFormat.getRecordWriter(job);

        for (final FileSplit split : fileSplits) {
            logger.info("\treading file split {}", split.getPath().getName() + " ({}", split.getStart() + "..." + (split.getStart() + split.getLength()), "{} {} bytes)");
            final RecordReader reader = inputFormat.createRecordReader(split, job);

            float lastProgress = -1f;
            while (reader.nextKeyValue()) {
                //System.out.println("" + reader.getProgress() + "> " + reader.getCurrentKey() + ": " + reader.getCurrentValue());
                final float progress = reader.getProgress();
                assertTrue(progress >= lastProgress);
                assertEquals(NullWritable.class, reader.getCurrentKey().getClass());
                final VertexWritable vertexWritable = (VertexWritable) reader.getCurrentValue();
                if (null != writer) writer.write(NullWritable.get(), vertexWritable);
                vertexCount++;
                outEdgeCount = outEdgeCount + (int) IteratorUtils.count(vertexWritable.get().edges(Direction.OUT));
                inEdgeCount = inEdgeCount + (int) IteratorUtils.count(vertexWritable.get().edges(Direction.IN));
                //
                final Vertex vertex = vertexWritable.get();
                assertTrue(vertex.id().getClass().equals(Integer.class) || vertex.id().getClass().equals(Byte.class));
                lastProgress = progress;
            }
        }

        assertEquals(6, outEdgeCount);
        assertEquals(6, inEdgeCount);
        assertEquals(outEdgeCount, inEdgeCount);
        assertEquals(6, vertexCount);

        if (null != writer) {
            writer.close(new TaskAttemptContextImpl(configuration, job.getTaskAttemptID()));
            for (int i = 1; i < 10; i++) {
                final File outputDirectory = new File(new URL(configuration.get("mapreduce.output.fileoutputformat.outputdir")).toURI());
                final List<FileSplit> splits = generateFileSplits(new File(outputDirectory.getAbsoluteFile() + "/_temporary/0/_temporary/" + job.getTaskAttemptID().getTaskID().toString().replace("task", "attempt") + "_0" + "/part-m-00000"), i);
                validateFileSplits(splits, configuration, inputFormatClass, Optional.empty());
            }
        }
    }
}
