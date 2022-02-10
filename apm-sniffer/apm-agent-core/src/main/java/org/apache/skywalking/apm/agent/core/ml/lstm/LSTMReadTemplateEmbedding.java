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
 *
 */

package org.apache.skywalking.apm.agent.core.ml.lstm;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;

import info.debatty.java.stringsimilarity.Cosine;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LSTMReadTemplateEmbedding {
    private static final ILog LOGGER = LogManager.getLogger(LSTMReadTemplateEmbedding.class);

    private static final String TEMPLATE_CSV_PATH = System.getenv("TEMPLATE_CSV_PATH");

    public static Map<String, LSTMTemplateEmbedding> EMBEDDING_MAP = new HashMap<>();

    public static Cosine SIM = new Cosine(3);

    public static void initEmbeddingMap() throws Exception{
        Path csvPath = Paths.get(TEMPLATE_CSV_PATH);
        NDManager nd = NDManager.newBaseManager();

        try(BufferedReader br = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            LOGGER.warn("Start create embedding map");
            HeaderColumnNameMappingStrategy<Embedding> strategy = new HeaderColumnNameMappingStrategy<>();
            strategy.setType(Embedding.class);

            CsvToBean csvToBean = new CsvToBeanBuilder(br)
                    .withType(Embedding.class)
                    .withMappingStrategy(strategy)
                    .withIgnoreLeadingWhiteSpace(true)
                    .build();

            List<Embedding> embeds = csvToBean.parse();

            for(Embedding embed: embeds) {
                embed.ExtractedEmbedding = Embedding.parseEmbedding(embed.Embedding);
                NDArray embedding = nd.create(embed.ExtractedEmbedding);
                LSTMTemplateEmbedding te = new LSTMTemplateEmbedding(embed.EventId,
                        embed.EventTemplate, embedding);
                EMBEDDING_MAP.put(embed.EventId, te);
            }
            LOGGER.warn("embeds travel ended");
        } catch (Exception e) {
            LOGGER.warn("Read csv exception:" + e);
        }

    }

//    public static void initEmbeddingMap() throws Exception {
//        LOGGER.warn("start init embedding map");
//        // Read templates
//        Table templateEmbedTable = Table.read().usingOptions(CsvReadOptions
//                .builder(TEMPLATE_CSV_PATH)
//                .maxCharsPerColumn(65535));
//        LOGGER.warn("readed table");
//
//        NDManager nd = NDManager.newBaseManager();
//        LOGGER.warn("inited NDManager");
//
//        for (Row row: templateEmbedTable) {
//            String eventId = row.getString("EventId");
//            String eventTemplate = row.getString("EventTemplate");
//            NDArray embedding = nd.create(parseEmbedding(row.getString("Embedding")));
//            LSTMTemplateEmbedding te = new LSTMTemplateEmbedding(eventId, eventTemplate, embedding);
//            EMBEDDING_MAP.put(eventId, te);
//        }
//        LOGGER.warn("end init embedding map");
//    }

    public static String getMostSimEvent(String logContent) {
        String mostSimEventId = "-1";
        double mostSimValue = 1;

        for (String eventId: EMBEDDING_MAP.keySet()) {
            String template = EMBEDDING_MAP.get(eventId).eventTemplate;
            double simVal = SIM.distance(logContent, template);
            if (simVal <= mostSimValue) {
                mostSimValue = simVal;
                mostSimEventId = eventId;
            }
        }

        return mostSimEventId;
    }

    private static double[] parseEmbedding(String embedding) throws Exception {
        embedding = embedding.replace("\n", " ");
        
        while (embedding.contains("  ")) {
            embedding = embedding.replace("  ", " ");
        }

        embedding = embedding.replace("[", "");
        embedding = embedding.replace("]", "");

        // trim space
        embedding = embedding.trim();

        // split by space
        String[] embeddingNumbers = embedding.split(" ");

        if (embeddingNumbers.length != 300) {
            throw new Exception("embeddingNumbers.length is:" + embeddingNumbers.length);
        }

        double[] result = new double[300];
        for (int i = 0; i < embeddingNumbers.length; i++) {
            result[i] = Double.parseDouble(embeddingNumbers[i]);
        }
        return result;
    }
}
