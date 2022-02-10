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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class GloveWordEmbeddingDict {
    public static Map<String, GloveWordEmbedding> gloveWordEmbeddingMap = new HashMap<>();

    public static Map<String, String> replaceMap = new HashMap<String, String>(){{
        put("configservice","config service");
        put("foodmapservice", "food map service");
        put("springframework", "spring framework");
        put("ticketinfo", "ticket info");
    }};

    private static final String GLOVE_PATH = System.getenv("GLOVE_PATH");

    public static final int embedSize = 50;

    public static final NDManager nd = NDManager.newBaseManager();

    public void initEmbeddingDict()  throws Exception {

        System.out.println("init gloveWordEmbeddingMap...");
        try (BufferedReader br = new BufferedReader(new FileReader(GLOVE_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] wordAndNums = line.split(" ");
                if (wordAndNums.length != embedSize+1) {
                    throw new Exception("embedding length is not " + embedSize+1);
                }

                String word = wordAndNums[0];
                double[] embedNumber = new double[50];

                for(int i = 1; i <= embedSize; i++) {
                    embedNumber[i-1] = Double.parseDouble(wordAndNums[i]);
                }

                NDArray embedding = nd.create(embedNumber);

                GloveWordEmbedding gloveWordEmbedding = new GloveWordEmbedding(word, embedding);
                gloveWordEmbeddingMap.put(word, gloveWordEmbedding);
            }
        }
        System.out.println("init gloveWordEmbeddingMap done");
    }

    public NDArray Log2Embedding(String logEntity) {
        // remove all punct
        String logEntity2 = logEntity.replaceAll("\\p{Punct}", " ");
        // TestTest -> test test
        String logEntity3 = logEntity2.replaceAll("([A-Z])", " $1").toLowerCase();
        // configservice -> config service
        String logEntity4 = logEntity3;

        for(String key: replaceMap.keySet()) {
            logEntity4 = logEntity4.replaceAll(key, replaceMap.get(key));
        }

        String[] result = logEntity4.split(" ");

        double[] embedResult = new double[50];
        NDArray tensorResult = nd.create(embedResult);

        int cnt = 0;

        for(String word: result) {
            // cal embedding
            GloveWordEmbedding wordEmbed = gloveWordEmbeddingMap.get(word);
            if (wordEmbed == null || word.length() <= 1) {
                //System.out.println("word: {" + word + "} is null");
            } else {
                cnt += 1;
                tensorResult = tensorResult.add(wordEmbed.embedding);
            }
        }
        tensorResult = tensorResult.div(cnt);
        return tensorResult;
    }
}
