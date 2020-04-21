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

package com.hazelcast.jet.contrib.slack.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 *Plain object to represent the slack api response.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SlackResponse implements Serializable {

    private boolean ok;
    private String error;

    /**
     * Returns the response execution status.
     *
     * @return
     */
    public boolean isOk() {
        return ok;
    }

    /**
     * Setter metthod to set the response status.
     * @param ok
     */
    public void setOk(boolean ok) {
        this.ok = ok;
    }

    /**
     * Get the error message.
     *
     * @return
     */
    public String getError() {
        return error;
    }

    /**
     * Setter method to set the error value.
     * @param error
     */
    public void setError(String error) {
        this.error = error;
    }
}
