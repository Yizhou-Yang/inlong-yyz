/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.tchousex.flink.catalog;

import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

public class Field {

    private String name;
    private String type;
    private String comment;

    public Field(String name, String type) {
        this(name, type, "");
    }

    public Field(String name, String type, String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
    }

    public Field() {
    }

    public String getName() {
        return name.toLowerCase(Locale.ROOT);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return StringUtils.equalsIgnoreCase(name, field.name) && StringUtils.equalsIgnoreCase(type,
                field.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, type);
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }
}
