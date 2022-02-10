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

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.index.NDIndex;
import ai.djl.ndarray.types.Shape;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/*
* Time Test
* */
public class JudgeAnomaly {
    private static final ILog LOGGER = LogManager.getLogger(JudgeAnomaly.class);

    private static final String MODEL_PATH = System.getenv("MODEL_PATH");

    Map<String, LSTMTemplateEmbedding> embeddingMap;
    Predictor<NDList, NDList> predictor;
    String isFake;

    public JudgeAnomaly() {
        try {
            LSTMReadTemplateEmbedding.initEmbeddingMap();
            this.embeddingMap = LSTMReadTemplateEmbedding.EMBEDDING_MAP;

            String modelFileURL = MODEL_PATH;

            LOGGER.info("modelFileURL:" + modelFileURL);

            Criteria<NDList, NDList> criteria = Criteria.builder()
                    .setTypes(NDList.class, NDList.class)
                    .optModelUrls(modelFileURL)
                    .optProgress(new ProgressBar()).build();

            ZooModel<NDList, NDList> model = ModelZoo.loadModel(criteria);
            this.predictor = model.newPredictor();
        } catch (Exception e) {
            LOGGER.error("JudgeAnomaly init exception:" + e);
            System.exit(1);
        }
    }

    private NDList infer(String[] logs, int logCount) throws IOException, ModelNotFoundException, MalformedModelException, TranslateException {
        /*
        * first dim: how many group input
        * second dim: how many logs in a group
        * third dim: log embedding dims
        * */
        if (logCount < 20) {
            logCount = 20;
        }

        try (NDManager ndManager = NDManager.newBaseManager()) {
            NDArray logsEmbedding = ndManager.create(new Shape(logCount, 300));
            long t1 = System.nanoTime();

            for (int i = 0; i < logs.length; i++) {
                String errorLog = logs[i];
                String eventId = LSTMReadTemplateEmbedding.getMostSimEvent(errorLog);
                logsEmbedding.set(new NDIndex(i), this.embeddingMap.get(eventId).embedding);
            }
            NDList errorExampleInput = new NDList();
            errorExampleInput.add(logsEmbedding);
            long t2 = System.nanoTime();

            // [[isError, isNormal]]
            NDList result = this.predictor.predict(errorExampleInput);
            long t3 = System.nanoTime();
            LOGGER.info("search time:" + (t2-t1)/1000000.0 + " predict time:" + (t3-t2)/1000000.0);
            return result;
        } catch (Exception e) {
            LOGGER.error("infer exception:" + e);
            return null;
        }
    }

    public boolean isAnomaly(List<String> logs) {
        LOGGER.warn("Anomaly Logs To be checked:" + logs.get(0));

        try {
            LOGGER.warn("start infer");
            String[] judgeLogs = new String[logs.size()];
            NDList out = infer(logs.toArray(judgeLogs), logs.size());
            LOGGER.warn("end infer");

            if (out != null) {
                LOGGER.info("OutResult:" + out);
                NDArray outArray = out.get(0);
                return outArray.getDouble(0) > outArray.getDouble(1);
            } else {
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Infer Exception:" + e);
            return false;
        }
    }

    public JudgeAnomaly(String isFake) {
        this.isFake = isFake;
    }

    private boolean fakeIsAnomaly(List<String> logs) {
        return false;
    }
}
