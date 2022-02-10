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

import com.opencsv.bean.CsvBindByName;

public class Embedding {

    @CsvBindByName
    public String EventId;

    @CsvBindByName
    public String EventTemplate;

    @CsvBindByName
    public Integer Occurrences;

    @CsvBindByName
    public String Tokens;

    @CsvBindByName
    public String Embedding;

    public double[] ExtractedEmbedding;

    public static double[] parseEmbedding(String embedding) throws Exception{
        // 消除换行
        embedding = embedding.replace("\n", " ");

        // 消除多余空格
        while(embedding.contains("  ")) {
            embedding = embedding.replace("  ", " ");
        }

        // 消除左右方括号
        embedding = embedding.replace("[", "");
        embedding = embedding.replace("]", "");

        // 去除首位空格
        embedding = embedding.trim();

        // split by 空格.
        String[] embeddingNumbers = embedding.split(" ");

        if (embeddingNumbers.length != 300) {
            throw new Exception("embeddingNumbers.length is:"+ embeddingNumbers.length);
        }

        double[] result = new double[300];
        for(int i = 0; i < embeddingNumbers.length; i++) {
            result[i] = Double.parseDouble(embeddingNumbers[i]);
        }
        return result;
    }

}
