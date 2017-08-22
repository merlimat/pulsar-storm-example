/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.storm.MessageToValuesMapper;
import org.apache.pulsar.storm.PulsarBolt;
import org.apache.pulsar.storm.PulsarBoltConfiguration;
import org.apache.pulsar.storm.PulsarSpout;
import org.apache.pulsar.storm.PulsarSpoutConfiguration;
import org.apache.pulsar.storm.TupleToMessageMapper;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExampleTopology {

    private static final String SERVICE_URL = "pulsar://localhost:6650";

    private static final String INGEST_TOPIC = "persistent://sample/standalone/ns1/ingest-topic";

    private static final String SUBSCRIPTION_NAME = "topology-subscription";

    private static final String RESULT_TOPIC = "persistent://sample/standalone/ns1/result-topic";

    @SuppressWarnings("serial")
    static MessageToValuesMapper messageToValuesMapper = new MessageToValuesMapper() {

        @Override
        public Values toValues(Message msg) {
            return new Values(new String(msg.getData()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // declare the output fields
            declarer.declare(new Fields("string"));
        }
    };

    @SuppressWarnings("serial")
    static TupleToMessageMapper tupleToMessageMapper = new TupleToMessageMapper() {

        @Override
        public Message toMessage(Tuple tuple) {
            String receivedMessage = tuple.getString(0);
            // message processing
            String processedMsg = receivedMessage + "-processed";
            return MessageBuilder.create().setContent(processedMsg.getBytes()).build();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // declare the output fields
        }
    };

    /**
     * Simple topology that consumes from a topic and writes into a different one
     */
    public static void main(String[] args) throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration();

        // create spout
        PulsarSpoutConfiguration spoutConf = new PulsarSpoutConfiguration();
        spoutConf.setServiceUrl(SERVICE_URL);
        spoutConf.setTopic(INGEST_TOPIC);
        spoutConf.setSubscriptionName(SUBSCRIPTION_NAME);
        spoutConf.setMessageToValuesMapper(messageToValuesMapper);
        PulsarSpout spout = new PulsarSpout(spoutConf, clientConf);

        // create bolt to publish results
        PulsarBoltConfiguration boltConf = new PulsarBoltConfiguration();
        boltConf.setServiceUrl(SERVICE_URL);
        boltConf.setTopic(RESULT_TOPIC);
        boltConf.setTupleToMessageMapper(tupleToMessageMapper);
        PulsarBolt bolt = new PulsarBolt(boltConf, clientConf);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testSpout", spout);
        builder.setBolt("testBolt", bolt).shuffleGrouping("testSpout");

        // Create the config for the topology
        Config conf = new Config();
        conf.setDebug(true);
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
