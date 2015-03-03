/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.deployer.utilities;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import ezbake.services.deploy.thrift.ArtifactType;
import ezbake.services.deploy.thrift.Language;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ArtifactTypeKey implements java.io.Serializable, Cloneable, Comparable<ArtifactTypeKey> {

    /**
     * @see ezbake.services.deploy.thrift.ArtifactType
     */
    public ArtifactType type; // required
    /**
     * @see ezbake.services.deploy.thrift.Language
     */
    public Language language; // required

    /**
     * The set of fields this struct contains, along with convenience methods for finding and manipulating them.
     */
    public enum _Fields {
        /**
         * @see ezbake.services.deploy.thrift.ArtifactType
         */
        TYPE("type"),
        /**
         * @see ezbake.services.deploy.thrift.Language
         */
        LANGUAGE("language");

        private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

        static {
            for (_Fields field : EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /**
         * Find the _Fields constant that matches name, or null if its not found.
         */
        public static _Fields findByName(String name) {
            return byName.get(name);
        }

        private final String _fieldName;

        _Fields(String fieldName) {
            _fieldName = fieldName;
        }

        public String getFieldName() {
            return _fieldName;
        }
    }

    public ArtifactTypeKey() {
    }

    public ArtifactTypeKey(ArtifactType type, Language language) {
        this.type = type;
        this.language = language;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public ArtifactTypeKey(ArtifactTypeKey other) {
        if (other.isSetType()) {
            this.type = other.type;
        }
        if (other.isSetLanguage()) {
            this.language = other.language;
        }
    }

    public ArtifactTypeKey deepCopy() {
        return new ArtifactTypeKey(this);
    }

    public void clear() {
        this.type = null;
        this.language = null;
    }

    /**
     * @see ezbake.services.deploy.thrift.ArtifactType
     */
    public ArtifactType getType() {
        return this.type;
    }

    /**
     * @see ezbake.services.deploy.thrift.ArtifactType
     */
    public ArtifactTypeKey setType(ArtifactType type) {
        this.type = type;
        return this;
    }

    public void unsetType() {
        this.type = null;
    }

    /**
     * Returns true if field type is set (has been assigned a value) and false otherwise
     */
    public boolean isSetType() {
        return this.type != null;
    }

    public void setTypeIsSet(boolean value) {
        if (!value) {
            this.type = null;
        }
    }

    /**
     * @see ezbake.services.deploy.thrift.Language
     */
    public Language getLanguage() {
        return this.language;
    }

    /**
     * @see ezbake.services.deploy.thrift.Language
     */
    public ArtifactTypeKey setLanguage(Language language) {
        this.language = language;
        return this;
    }

    public void unsetLanguage() {
        this.language = null;
    }

    /**
     * Returns true if field language is set (has been assigned a value) and false otherwise
     */
    public boolean isSetLanguage() {
        return this.language != null;
    }

    public void setLanguageIsSet(boolean value) {
        if (!value) {
            this.language = null;
        }
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
            case TYPE:
                if (value == null) {
                    unsetType();
                } else {
                    setType((ArtifactType) value);
                }
                break;

            case LANGUAGE:
                if (value == null) {
                    unsetLanguage();
                } else {
                    setLanguage((Language) value);
                }
                break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
            case TYPE:
                return getType();

            case LANGUAGE:
                return getLanguage();

        }
        throw new IllegalStateException();
    }

    /**
     * Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
     */
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new IllegalArgumentException();
        }

        switch (field) {
            case TYPE:
                return isSetType();
            case LANGUAGE:
                return isSetLanguage();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if (that == null)
            return false;
        if (that instanceof ArtifactTypeKey)
            return this.equals((ArtifactTypeKey)that);
        return false;
    }

    public boolean equals(ArtifactTypeKey that) {
        if (that == null)
            return false;

        boolean this_present_type = true && this.isSetType();
        boolean that_present_type = true && that.isSetType();
        if (this_present_type || that_present_type) {
            if (!(this_present_type && that_present_type))
                return false;
            if (!this.type.equals(that.type))
                return false;
        }

        boolean this_present_language = true && this.isSetLanguage();
        boolean that_present_language = true && that.isSetLanguage();
        if (this_present_language || that_present_language) {
            if (!(this_present_language && that_present_language))
                return false;
            if (!this.language.equals(that.language))
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();

        boolean present_type = true && (isSetType());
        builder.append(present_type);
        if (present_type)
            builder.append(type.getValue());

        boolean present_language = true && (isSetLanguage());
        builder.append(present_language);
        if (present_language)
            builder.append(language.getValue());

        return builder.toHashCode();
    }

    public int compareTo(ArtifactTypeKey other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetType()).compareTo(other.isSetType());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetType()) {
            lastComparison = this.type.compareTo(other.type);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetLanguage()).compareTo(other.isSetLanguage());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetLanguage()) {
            lastComparison = this.language.compareTo(other.language);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ArtifactTypeKey(");
        boolean first = true;

        sb.append("type:");
        if (this.type == null) {
            sb.append("null");
        } else {
            sb.append(this.type);
        }
        first = false;
        if (!first) sb.append(", ");
        sb.append("language:");
        if (this.language == null) {
            sb.append("null");
        } else {
            sb.append(this.language);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }


    /**
     * Define a final set of permutations that will be computed at runtime
     */
    private static final Set<ArtifactTypeKey> permutations = computePermutations();

    /**
     * Compute the permutations of artifact types. Should only be called once
     * @return the permutations
     */
    private static ImmutableSet<ArtifactTypeKey> computePermutations() {
        ImmutableSet.Builder<ArtifactTypeKey> permutations = ImmutableSet.builder();
        for (ArtifactType type : ArtifactType.values()) {
            for (Language language : Language.values()) {
                permutations.add(new ArtifactTypeKey(type, language));
            }
        }

        return permutations.build();
    }

    /**
     * Return the immutable Set of permutations
     * @return the Set of permutations
     */
    public static Set<ArtifactTypeKey> getPermutations() {
        return permutations;
    }
}

