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

package ezbake.services.provenance.graph.frames;

import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.modules.javahandler.JavaHandler;
import org.apache.thrift.TException;

import java.util.*;

/**
 * A vertex created to assist in the purging of documents from the system
 */
public interface PurgeEvent extends BaseVertex {

    public static final String TYPE = "Purge";
    public static final String PurgeId = "PurgeId";
    public static final String PurgeIdEs = "PurgeIdEs";
    public static final String Name = "PurgeName";
    public static final String Description = "Description";
    public static final String DocumentUris = "DocumentUris";
    public static final String DocumentUrisNotFound = "DocumentUrisNotFound";
    public static final String PurgeDocumentIds = "PurgeDocumentIds";
    public static final String CompletelyPurgedDocumentIds = "CompletelyPurgedDocumentIds";
    public static final String Resolved = "Resolved";

    /**
     * PurgeId
     * A unique ID generated to represent this purge event
     */
    @Property(PurgeId)
    public long getPurgeId();

    @Property(PurgeId)
    public void setPurgeId(long id);

    @Property(PurgeIdEs)
    public long getPurgeIdEs();

    @Property(PurgeIdEs)
    public void setPurgeIdEs(long id);

    /**
     * Name
     * <p/>
     * An optional text name for the purge.
     */
    @Property(Name)
    public String getName();

    @Property(Name)
    public void setName(String name);

    /**
     * Description
     * <p/>
     * An optional text description of the purge.
     */
    @Property(Description)
    public String getDescription();

    @Property(Description)
    public void setDescription(String description);

    /**
     * DocumentUris
     * <p/>
     * The list of document Uris that should serve as the starting point for the purge
     */
    @Property(DocumentUris)
    public void setDocumentUris(String[] documentUris);

    @Property(DocumentUris)
    public String[] getDocumentUrisInternal();

    @JavaHandler
    public List<String> getDocumentUris();

    /**
     * DocumentUrisNotFound
     * <p/>
     * Members of DocumentUris that could not be found in the system at the time of purge.
     */
    @Property(DocumentUrisNotFound)
    public void setDocumentUrisNotFound(String[] documentsNotFound);

    @Property(DocumentUrisNotFound)
    public String[] getDocumentUrisNotFoundInternal();

    @JavaHandler
    public List<String> getDocumentUrisNotFound();

    /**
     * PurgeDocumentIds
     * <p/>
     * The Document IDs corresponding to the found URIs within DocumentUris at time of purge initiation.
     */
    @Property((PurgeDocumentIds))
    public void setPurgeDocumentIds(Long[] purgeDocumentIds);

    @Property(PurgeDocumentIds)
    public Long[] getPurgeDocumentIdsInternal();

    @JavaHandler
    public Set<Long> getPurgeDocumentIds();

    /**
     * CompletelyPurgedDocumentIds
     * <p/>
     * The Document Ids corresponding to the URIs that the purge service acknowledges have been completely purged from the system.
     * This set is empty when markForPurge() creates this vertex and is populated by subsequent calls from the purge service.
     */
    @Property((CompletelyPurgedDocumentIds))
    public void setCompletelyPurgedDocumentIdsInternal(Long[] completelyPurgedDocumentIds);

    @Property(CompletelyPurgedDocumentIds)
    public Long[] getCompletelyPurgedDocumentIdsInternal();

    @JavaHandler
    public void setCompletelyPurgedDocumentIds(Set<Long> completelyPurgedDocumentIds);

    @JavaHandler
    public Set<Long> getCompletelyPurgedDocumentIds();

    /**
     * Resolved
     * A boolean flag set to fault when the vertex is created, updated in the updatePurge method.
     */
    @Property(Resolved)
    public void setResolved(boolean resolved);

    @Property(Resolved)
    public boolean getResolved();

    @JavaHandler
    public void updateProperties(ezbake.base.thrift.EzSecurityToken securityToken, long id, String name, String description, List<String> documentUris,
                                 List<String> documentNotFound, Set<Long> purgeDocIds, Set<Long> completePurgedDocIds, boolean resolved) throws TException;


    public abstract class Impl extends BaseVertexImpl implements PurgeEvent {

        @Override
        public void updateProperties(ezbake.base.thrift.EzSecurityToken securityToken, long id, String name, String description,
                                     List<String> documentUris, List<String> documentNotFound, Set<Long> purgeDocIds, Set<Long> completePurgedDocIds, boolean resolved) throws TException {
            this.setPurgeId(id);
            this.setPurgeIdEs(id);
            this.setName(name);
            this.setDescription(description);
            this.setDocumentUris(documentUris.toArray(new String[documentUris.size()]));
            this.setDocumentUrisNotFound(documentNotFound.toArray(new String[documentNotFound.size()]));
            this.setPurgeDocumentIds(purgeDocIds.toArray(new Long[purgeDocIds.size()]));
            this.setCompletelyPurgedDocumentIdsInternal(completePurgedDocIds.toArray(new Long[completePurgedDocIds.size()]));
            this.setResolved(resolved);
            super.updateCommonProperties(securityToken, TYPE);
        }

        public List<String> getDocumentUris() {
            return new ArrayList<String>(Arrays.asList(this.getDocumentUrisInternal()));
        }

        public List<String> getDocumentUrisNotFound() {
            return new ArrayList<String>(Arrays.asList(this.getDocumentUrisNotFoundInternal()));
        }

        public Set<Long> getPurgeDocumentIds() {
            return new HashSet<>(Arrays.asList(this.getPurgeDocumentIdsInternal()));
        }

        public void setCompletelyPurgedDocumentIds(Set<Long> completelyPurgedDocumentIds) {
            this.setCompletelyPurgedDocumentIdsInternal(completelyPurgedDocumentIds.toArray(new Long[completelyPurgedDocumentIds.size()]));
        }

        public Set<Long> getCompletelyPurgedDocumentIds() {
            return new HashSet<>(Arrays.asList(this.getCompletelyPurgedDocumentIdsInternal()));
        }
    }
}
