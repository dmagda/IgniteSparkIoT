/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.iot.model;

import java.io.Serializable;
import java.util.Date;

/**
 *
 */
public class TempKey implements Serializable {
    /**
     * Sensor ID. Set as an affinity key in 'ignite-config.xml'.
     */
    private int sensorId;

    /**
     * Timestamp of the record.
     */
    private Date ts;

    /**
     * @param sensorId
     * @param ts
     */
    public TempKey(int sensorId, Date ts) {
        this.sensorId = sensorId;
        this.ts = ts;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TempKey key = (TempKey)o;

        if (sensorId != key.sensorId)
            return false;
        return ts != null ? ts.equals(key.ts) : key.ts == null;

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = sensorId;
        result = 31 * result + (ts != null ? ts.hashCode() : 0);
        return result;
    }
}
