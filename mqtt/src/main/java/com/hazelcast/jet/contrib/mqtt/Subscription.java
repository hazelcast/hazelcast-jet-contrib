/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.mqtt;

import java.io.Serializable;

import static com.hazelcast.jet.contrib.mqtt.Subscription.QualityOfService.AT_LEAST_ONCE;

/**
 * Represents a subscription to a topic with quality of service.
 */
public class Subscription implements Serializable {

    private final String topic;
    private final QualityOfService qualityOfService;

    Subscription(String topic, QualityOfService qualityOfService) {
        this.topic = topic;
        this.qualityOfService = qualityOfService;
    }

    /**
     * Creates a subscription using given topic and
     * {@link QualityOfService#AT_LEAST_ONCE}.
     */
    public static Subscription of(String topic) {
        return new Subscription(topic, AT_LEAST_ONCE);
    }

    /**
     * Creates a subscription using given topic and qualityOfService
     */
    public static Subscription of(String topic, QualityOfService qualityOfService) {
        return new Subscription(topic, qualityOfService);
    }

    /**
     * Creates a subscription using given topic and qos
     */
    public static Subscription of(String topic, int qos) {
        return new Subscription(topic, QualityOfService.of(qos));
    }

    /**
     * @return the topic name or pattern
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @return the quality of service
     */
    public QualityOfService getQualityOfService() {
        return qualityOfService;
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "topic='" + topic + '\'' +
                ", qos=" + qualityOfService +
                '}';
    }

    /**
     * Represents the quality of service for the subscription.
     */
    public enum QualityOfService {
        /**
         * AT_MOST_ONCE, QoS(0)
         */
        AT_MOST_ONCE(0),

        /**
         * AT_LEAST_ONCE, QoS(1)
         */
        AT_LEAST_ONCE(1),

        /**
         * EXACTLY_ONCE, QoS(2)
         */
        EXACTLY_ONCE(2);

        private final int qos;

        QualityOfService(int qos) {
            this.qos = qos;
        }

        /**
         * @return QoS representation
         */
        public int getQos() {
            return qos;
        }

        static QualityOfService of(int qos) {
            switch (qos) {
                case 0:
                    return AT_MOST_ONCE;
                case 1:
                    return AT_LEAST_ONCE;
                case 2:
                    return EXACTLY_ONCE;
                default:
                    throw new IllegalArgumentException("Unknown qos level");
            }
        }

    }

}
