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

package com.hazelcast.jet.contrib.debezium;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@JsonIgnoreProperties({"purchaser"})
public class Order {

    @JsonProperty("order_number")
    public int orderNumber;

    @JsonDeserialize(using = DateHandler.class)
    @JsonProperty("order_date")
    public Date orderDate;

    @JsonProperty("quantity")
    public int quantity;

    @JsonProperty("product_id")
    public int productId;

    public Order() {
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderNumber, orderDate, quantity, productId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Order other = (Order) obj;
        return orderNumber == other.orderNumber
                && Objects.equals(orderDate, other.orderDate)
                && Objects.equals(quantity, other.quantity)
                && Objects.equals(productId, other.productId);
    }

    @Override
    public String toString() {
        return "Order {orderNumber=" + orderNumber + ", orderDate=" + orderDate + ", quantity=" + quantity +
                ", productId=" + productId + '}';
    }

    public static class DateHandler extends JsonDeserializer<Date> {

        @Override
        public Date deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
            long days = parser.getLongValue(); //todo: very weird, the connector should give us milliseconds...
            try {
                return new Date(TimeUnit.DAYS.toMillis(days));
            } catch (Exception e) {
                return null;
            }
        }
    }

}
