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

package ezbake.ins.thrift.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.TokenType;
import ezbake.base.thrift.Visibility;
import ezbake.common.ins.INSUtility;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.elastic.thrift.Document;
import ezbake.data.elastic.thrift.EzElastic;
import ezbake.data.elastic.thrift.Page;
import ezbake.data.elastic.thrift.Query;
import ezbake.data.elastic.thrift.SearchResult;
import ezbake.ins.thrift.gen.AppService;
import ezbake.ins.thrift.gen.Application;
import ezbake.ins.thrift.gen.ApplicationNotFoundException;
import ezbake.ins.thrift.gen.ApplicationSummary;
import ezbake.ins.thrift.gen.BroadcastTopic;
import ezbake.ins.thrift.gen.FeedPipeline;
import ezbake.ins.thrift.gen.FeedType;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.ins.thrift.gen.JobRegistration;
import ezbake.ins.thrift.gen.ListenerPipeline;
import ezbake.ins.thrift.gen.WebApplicationLink;
import ezbake.query.intents.IntentType;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.SecurityID;
import ezbake.security.thrift.EzSecurityRegistration;
import ezbake.security.thrift.EzSecurityRegistrationConstants;
import ezbake.security.thrift.RegistrationStatus;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftTestUtils;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;


public class InternalNameServiceHandler extends EzBakeBaseThriftService implements InternalNameService.Iface {
    public static final String INSMode = "ins.use.dev.mode";
    public static final String DefaultVisibilityProperty = "ins.default.visibility";
    public static final String DevModePrefix = "DEV";

    private static final Logger logger = LoggerFactory.getLogger(InternalNameServiceHandler.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String CategoryIndex = "Ins_Category";
    private static final String SystemTopicIndex = "Ins_SystemTopic";
    private static final String FeedIndex = "Ins_Feed";
    private static final String ApplicationIndex = "Ins_App";

    private static final String AllowedUsersField = "allowedUsers";
    private static final String CategoriesField = "categories";
    private static final String UrnMapField = "webApp.urnMap";
    private static final String IsChloeEnabledField = "webApp.isChloeEnabled";
    private static final String IntentServiceMapField = "intentServiceMap";
    private static final String AppNameField = "appName";
    private static final String FeedPipelinesField = "feedPipelines.broadcastTopics.name";
    protected static final String FeedPipeLinesFeedName = "feedPipelines.feedName";
    private static final String ListenerPipelinesField = "listenerPipelines.broadcastTopics.name";
    private static final String ElasticServiceName = "documentService";


    private ThriftClientPool pool;
    private EzbakeSecurityClient securityClient;
    private String applicationSecurityId;
    private String deployerSecurityId;
    private boolean inDevMode = false;

    /**
     * default constructor for thrift runner
     */
    public InternalNameServiceHandler() {
    }

    /**
     * Constructor for testing
     *
     * @param securityClient        The security client
     * @param pool                  The pool for thrift clients
     * @param applicationSecurityId the security id for this service
     */
    @SuppressWarnings("unused") //Used in mocks
    protected InternalNameServiceHandler(EzbakeSecurityClient securityClient, ThriftClientPool pool,
                                         String applicationSecurityId) {
        this.securityClient = securityClient;
        this.pool = pool;
        this.applicationSecurityId = applicationSecurityId;
    }

    @Override
    public Set<FeedPipeline> getPipelineFeeds() throws TException {
        logger.trace("Getting pipelines");
        if (inDevMode) {
            return getTestOutput("getPipelineFeeds", "", new TypeReference<Set<FeedPipeline>>() {
            });
        }
        SearchResult results = search(new Query("*"), FeedIndex, getSecurityToken());
        Set<FeedPipeline> pipelines = Sets.newHashSet();
        for (Document doc : results.getMatchingDocuments()) {
            String json = doc.get_jsonObject();
            try {
                FeedPipeline pipeline = objectMapper.readValue(json, FeedPipeline.class);
                pipelines.add(pipeline);
            } catch (Exception ex) {
                throw new TException(ex);
            }
        }
        return pipelines;
    }

    @Override
    public boolean saveApplication(Application application, EzSecurityToken ezSecurityToken) throws TException {
        checkNotNull(application);
        checkNotNull(ezSecurityToken);
        validateToken(ezSecurityToken);
        logger.trace("Saving application {}", application.getId());
        devModeUnsupported();
        addDocument(application.getId(), ApplicationIndex, getThriftJson(application), ezSecurityToken);
        for (FeedPipeline pipeline : application.getFeedPipelines()) {
            addDocument(application.getId() + "_" + pipeline.getFeedName(), FeedIndex, getThriftJson(pipeline), ezSecurityToken);
        }
        return true;
    }

    @Override
    public boolean deleteApplication(String appId, EzSecurityToken ezSecurityToken) throws TException {
        checkNotNull(ezSecurityToken);
        validateToken(ezSecurityToken);
        logger.trace("Deleting application {}", appId);
        devModeUnsupported();
        Application application;
        try {
            application = getAppById(appId, ezSecurityToken);
        } catch (ApplicationNotFoundException ex) {
            return false;
        }
        delete(appId, ApplicationIndex, ezSecurityToken);
        for (FeedPipeline pipeline : application.getFeedPipelines()) {
            delete(application.getId() + "_" + pipeline.getFeedName(), FeedIndex, ezSecurityToken);
        }
        return true;
    }

    @Override
    public Set<Application> getDuplicateAppNames(String appName, EzSecurityToken token) throws TException {
        validateToken(token);
        devModeUnsupported();
        return getApplications(getTermQuery(AppNameField, appName.toLowerCase()), getSecurityToken());
    }

    @Override
    public Set<String> getCategories() throws TException {
        logger.trace("Getting categories");
        devModeUnsupported();
        SearchResult results = search(new Query("*"), CategoryIndex, getSecurityToken());
        Set<String> categories = Sets.newHashSet();
        for (Document doc : results.getMatchingDocuments()) {
            try {
                categories.add(objectMapper.readValue(doc.get_jsonObject(), CategoryDocument.class).category);
            } catch (Exception ex) {
                throw new TException(ex);
            }
        }
        return categories;
    }

    @Override
    public boolean addCategory(String category, EzSecurityToken ezSecurityToken) throws TException {
        checkNotNull(ezSecurityToken);
        validateToken(ezSecurityToken);
        logger.trace("Adding category {}", category);
        devModeUnsupported();
        try {
            addDocument(category, CategoryIndex, objectMapper.writeValueAsString(new CategoryDocument(category)), ezSecurityToken);
        } catch (Exception e) {
            throw new TException("Unable to add category: ", e);
        }
        return true;
    }

    @Override
    public boolean removeCategory(String category, EzSecurityToken ezSecurityToken) throws TException {
        checkNotNull(ezSecurityToken);
        validateToken(ezSecurityToken);
        logger.trace("Deleting category {}", category);
        devModeUnsupported();
        delete(category, CategoryIndex, ezSecurityToken);
        return true;
    }

    @Override
    public Set<String> getSystemTopics() throws TException {
        logger.trace("Getting system topics");
        if (inDevMode) {
            return getTestOutput("getSystemTopics", "", new TypeReference<Set<String>>() {
            });
        }
        SearchResult results = search(new Query("*"), SystemTopicIndex, getSecurityToken());
        Set<String> systemTopics = new HashSet<>();
        for (Document doc : results.getMatchingDocuments()) {
            try {
                systemTopics.add(objectMapper.readValue(doc.get_jsonObject(), SystemTopicDocument.class).systemTopic);
            } catch (Exception ex) {
                throw new TException(ex);
            }
        }
        return systemTopics;
    }

    @Override
    public boolean addSystemTopic(String systemTopic, EzSecurityToken token) throws TException {
        checkNotNull(token);
        validateToken(token);
        logger.trace("Adding system topic {}", systemTopic);
        devModeUnsupported();
        try {
            addDocument(systemTopic, SystemTopicIndex, objectMapper.writeValueAsString(new SystemTopicDocument(systemTopic)), token);
        } catch (Exception e) {
            throw new TException("Unable to add system topic: ", e);
        }
        return true;
    }

    @Override
    public boolean removeSystemTopic(String systemTopic, EzSecurityToken token) throws TException {
        checkNotNull(token);
        validateToken(token);
        logger.trace("Deleting system topic {}", systemTopic);
        devModeUnsupported();
        delete(systemTopic, SystemTopicIndex, token);
        return true;
    }

    @Override
    public Set<String> getTopicsForFeed(String appId, String feedName) throws TException {
        if (inDevMode) {
            return Sets.newHashSet(feedName + "Topic1", feedName + "Topic2");
        }
        return getFeedTopics(appId, feedName, null);
    }

    @Override
    public Set<String> getApprovedTopicsForFeed(String appId, String feedName, EzSecurityToken token) throws TException {
        devModeUnsupported();
        validateToken(token);
        return getFeedTopics(appId, feedName, token);
    }

    private Set<String> getFeedTopics(String appId, String feedName, EzSecurityToken token) throws TException {
        logger.trace("Getting topics for feed {} for Application {}", feedName, appId);

        Set<String> topics = Sets.newHashSet();
        Application application;
        if (token != null) {
            try {
                if (!isApplicationRegistrationApproved(appId, token)) {
                    return topics;
                }
            } catch (Exception e) {
                throw new TException(e);
            }
            application = getApplication(appId, token);
        } else {
            application = getApplication(appId);
        }


        if (application != null) {
            if (application.feedPipelines != null) {
                for (FeedPipeline pipeline : application.feedPipelines) {
                    getTopicsForPipeline(pipeline.getFeedName(), feedName, pipeline.getBroadcastTopics(), topics);
                }
            }
            if (application.listenerPipelines != null) {
                for (ListenerPipeline listener : application.listenerPipelines) {
                    getTopicsForPipeline(listener.getFeedName(), feedName, listener.getBroadcastTopics(), topics);
                }
            }
        }
        return topics;
    }

    @Override
    public Set<String> getListeningTopicsForFeed(String appId, String feedName, EzSecurityToken token) throws TException {
        logger.trace("Getting listening topics for feed {} for Application {}", feedName, appId);
        if (inDevMode) {
            return Sets.newHashSet(feedName + "Topic1", feedName + "Topic2");
        }

        validateToken(token);
        Set<String> topics = Sets.newHashSet();
        try {
            if (!isApplicationRegistrationApproved(appId, token)) {
                return topics;
            }
        } catch (Exception e) {
            throw new TException(e);
        }

        Application application = getApplication(appId, token);
        if (application != null) {
            for (ListenerPipeline listener : application.listenerPipelines) {
                if (listener.getFeedName().equals(feedName) && listener.isSetListeningTopics()) {
                    topics.addAll(listener.getListeningTopics());
                }
            }
        }
        return topics;
    }

    @Override
    public String getURIPrefix(String appId, String categoryKey) throws TException {
        logger.trace("Getting URI Prefix for {} and Application {}", categoryKey, appId);
        if (inDevMode) {
            return INSUtility.buildUriPrefix(DevModePrefix, categoryKey);
        }

        Application application = getApplication(appId, getSecurityToken());
        String category = application.getCategories().get(categoryKey);
        if (!Strings.isNullOrEmpty(category)) {
            return INSUtility.buildUriPrefix(category, categoryKey);
        } else {
            throw new ApplicationNotFoundException("AppId + CategoryKey combination not found");
        }
    }

    /**
     * For every application that has categories field, return Uris
     *
     * @return All Uris for each application
     * @throws TException
     */
    @Override
    public Set<String> getURIPrefixes() throws TException {
        logger.trace("Getting URI Prefixes");
        devModeUnsupported();
        Set<String> uris = Sets.newHashSet();
        Set<Application> apps = getApplications(getExistsQuery(CategoriesField, ""), getSecurityToken());

        for (Application app : apps) {
            for (Map.Entry<String, String> entry : app.getCategories().entrySet()) {
                uris.add(INSUtility.buildUriPrefix(entry.getValue(), entry.getKey()));
            }
        }

        return uris;
    }

    /**
     * Returns the web applications that have registered to be able to open a document with the given Uri
     * The URN map will ONLY contain the one URL that was registered for the given Uri
     *
     * @param uri The uri of the document
     * @return The WebApplication objects that can open the Uri
     * @throws TException
     */
    @Override
    public Set<WebApplicationLink> getWebAppsForUri(String uri) throws TException {
        logger.trace("Getting web apps that can open {}", uri);
        if (inDevMode) {
            return getTestOutput("getWebAppsForUri", uri.replace(":", ""),
                    new TypeReference<Set<WebApplicationLink>>() {
                    });
        }

        String uriPrefix = INSUtility.getUriPrefix(uri);
        Set<WebApplicationLink> webApps = Sets.newHashSet();
        Set<Application> apps = getApplications(getExistsQuery(UrnMapField + ".", uriPrefix), getSecurityToken());

        for (Application app : apps) {
            WebApplicationLink link = app.webApp.getUrnMap().get(uriPrefix);
            link.setAppName(app.getAppName());
            if (app.webApp.isSetRequiredGroupName() && !app.webApp.getRequiredGroupName().trim().isEmpty()) {
                link.setRequiredGroupName(app.webApp.getRequiredGroupName());
            }
            webApps.add(link);
        }

        return webApps;
    }

    @Override
    public Set<WebApplicationLink> getChloeWebApps() throws TException {
        logger.trace("Getting Chloe-enabled apps");
        if (inDevMode) {
            return getTestOutput("getChloeWebApps", "",
                    new TypeReference<Set<WebApplicationLink>>() {
                    });
        }
        Set<WebApplicationLink> webApps = Sets.newHashSet();
        Set<Application> apps = getApplications(getTermQuery(IsChloeEnabledField, "1"), getSecurityToken());

        for (Application app : apps) {
            if (!Strings.isNullOrEmpty(app.getWebApp().getChloeWebUrl())) {
                WebApplicationLink link = new WebApplicationLink();
                link.setAppName(app.getAppName());
                link.setWebUrl(app.webApp.getChloeWebUrl());
                if (app.webApp.isSetRequiredGroupName() && !app.webApp.getRequiredGroupName().trim().isEmpty()) {
                    link.setRequiredGroupName(app.webApp.getRequiredGroupName());
                }
                webApps.add(link);
            }
        }
        return webApps;
    }

    @Override
    public Application getAppById(String appId, EzSecurityToken ezSecurityToken) throws TException {
        checkNotNull(ezSecurityToken);
        validateToken(ezSecurityToken);
        logger.trace("Getting Application {}", appId);
        devModeUnsupported();
        return getApplication(appId, ezSecurityToken);
    }

    /**
     * Get all applications register for given user and security token
     *
     * @param ezSecurityToken The user token
     * @return All applications register for given user
     * @throws TException
     */
    @Override
    public Set<Application> getMyApps(EzSecurityToken ezSecurityToken) throws TException {
        checkNotNull(ezSecurityToken);
        validateToken(ezSecurityToken);
        if (ezSecurityToken.getType() != TokenType.USER) {
            logger.error("Trying to call getMyApps with a non-User token is not allowed");
            throw new TException("EzSecurityToken must be a User Token for getMyApps");
        }
        String userName = ezSecurityToken.getTokenPrincipal().getPrincipal();
        logger.trace("Getting apps for {}", userName);
        devModeUnsupported();

        return getApplications(getTermQuery(AllowedUsersField, userName), ezSecurityToken);
    }

    @Override
    public ApplicationSummary getAppByName(String appName, EzSecurityToken token) throws TException {
        if (inDevMode) {
            return getTestOutput("getAppByName", appName, new TypeReference<ApplicationSummary>() {
            });
        }

        Set<Application> applications = getApplications(getTermQuery(AppNameField, appName.toLowerCase()), token);
        if (applications.size() == 0) {
            logger.info("Trying by id");
            //If we can't find the app by Id either, this will throw an ApplicationNotFoundException
            applications.add(getApplication(appName, token));
        } else if (applications.size() > 1) {
            throw new TException("Uh oh.  Somehow we ended up with more than one app with the same name.  Abort");
        }
        Application app = applications.iterator().next();
        ApplicationSummary summary = new ApplicationSummary();
        summary.setAppIconSrc(app.getAppIconSrc());
        summary.setAppName(app.getAppName());
        if (app.isSetWebApp()) {
            summary.setExternalUri(app.getWebApp().getExternalUri());
        }
        summary.setId(app.getId());
        summary.setPoc(app.getPoc());
        summary.setSponsoringOrganization(app.getSponsoringOrganization());
        return summary;
    }

    @Override
    public String exportApplication(String appId, EzSecurityToken securityToken) throws TException {
        checkNotNull(securityToken);
        validateToken(securityToken);
        logger.trace("Exporting application {}", appId);
        devModeUnsupported();
        Application application = getApplication(appId, securityToken);
        return getThriftJson(application);
    }

    @Override
    public Application importApplication(String exportedApplication, EzSecurityToken securityToken) throws TException {
        checkNotNull(securityToken);
        validateToken(securityToken);
        logger.trace("Importing application");
        devModeUnsupported();
        try {
            Application application = objectMapper.readValue(exportedApplication, Application.class);
            addDocument(application.id, ApplicationIndex, exportedApplication, securityToken);
            return application;
        } catch (Exception ex) {
            throw new TException("Not a valid application export", ex);
        }
    }

    @Override
    public Set<AppService> appsThatSupportIntent(final String intentName) throws TException {
        Set<AppService> result = Sets.newHashSet();

        if (inDevMode) {
            return getTestOutput("appsThatSupportIntent", intentName,
                    new TypeReference<Set<AppService>>() {
                    });
        }
        try {
            IntentType.valueOf(intentName);
        } catch (IllegalArgumentException e) {
            logger.error("Illegal Intent name passed");
            return result;
        }

        Set<Application> apps = getApplications(getExistsQuery(IntentServiceMapField + ".", intentName), getSecurityToken());
        for (Application app : apps) {
            result.add(new AppService(app.getAppName(), app.getIntentServiceMap().get(intentName)));
        }

        return result;
    }

    @Override
    public Set<String> allBroadcastTopicNames(FeedType type) throws TException {
        Set<String> result;

        switch (type) {
            case APP:
                result = getAppBroadcastTopicNames();
                break;
            case SYSTEM:
                result = getSystemTopics();
                break;
            case ALL:
                result = getAppBroadcastTopicNames();
                result.addAll(getSystemTopics());
                break;
            default:
                throw new TException("Unknown broadcast topic type");
        }

        return result;
    }

    @Override
    public Set<JobRegistration> getJobRegistrations(final String appId, final EzSecurityToken ezSecurityToken) throws TException {
        if (inDevMode) {
            return getTestOutput("getJobRegistrations", appId, new TypeReference<Set<JobRegistration>>() {
            });
        }
        try {
            if (!isApplicationRegistrationApproved(appId, ezSecurityToken)) {
                return Sets.newHashSet();
            }
        } catch (Exception e) {
            throw new TException(e);
        }

        //We chain this token so we know who made the call, but only INS is allowed to make the call
        Application app = getAppById(appId, securityClient.fetchDerivedTokenForApp(ezSecurityToken, applicationSecurityId));

        Set<JobRegistration> jobRegistrations = app.getJobRegistrations();
        //Now let's get the uri prefix for the jobs
        if (jobRegistrations != null) {
            for (JobRegistration registration : jobRegistrations) {
                Set<Application> apps = getApplications(getTermQuery(FeedPipeLinesFeedName, registration.getFeedName()),
                        getSecurityToken());
                //There should only be 1 app that is registered with this feed
                if (apps.size() != 1) {
                    logger.error("Getting job registrations looking for feed {} returned {} apps instead of 1",
                            registration.getFeedName(), apps.size());
                    throw new TException("Invalid number of registered apps for a feed were returned");
                }
                Application feedApp = apps.iterator().next();
                String category = feedApp.getCategories().get(registration.getFeedName());
                String uriPrefix = INSUtility.buildUriPrefix(category, registration.getFeedName());
                registration.setUriPrefix(uriPrefix);
            }
        } else {
            jobRegistrations = new HashSet<>();
        }

        return jobRegistrations;
    }

    /**
     * Gets existing applications' summary sans, due to potential object size issues, application icon source dump
     */
    @Override
    public Set<ApplicationSummary> getAllApplicationsSummary() throws TException {
        Set<ApplicationSummary> result = new HashSet<>();
        Set<Application> allApps = getApplications("*", getSecurityToken());

        for (Application app : allApps) {
            ApplicationSummary summary = new ApplicationSummary().setAppName(app.getAppName()).
                    setId(app.getId()).setPoc(app.getPoc()).
                    setSponsoringOrganization(app.getSponsoringOrganization());

            if (app.isSetWebApp()) {
                summary.setExternalUri(app.getWebApp().getExternalUri());
            }

            result.add(summary);
        }

        return result;
    }


    @Override
    @SuppressWarnings("unchecked")
    public TProcessor getThriftProcessor() {
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        EzProperties configuration = new EzProperties(getConfigurationProperties(), true);
        inDevMode = configuration.getBoolean(INSMode, false);
        EzBakeApplicationConfigurationHelper appHelper = new EzBakeApplicationConfigurationHelper(configuration);
        if (Strings.isNullOrEmpty(appHelper.getApplicationName())) {
            //Might not have a config file for this service.
            configuration.setProperty(EzBakePropertyConstants.EZBAKE_APPLICATION_NAME, InternalNameServiceConstants.SERVICE_NAME);
        }

        pool = new ThriftClientPool(configuration);
        if (!inDevMode) {
            securityClient = new EzbakeSecurityClient(configuration);
            applicationSecurityId = appHelper.getSecurityID();
            deployerSecurityId = SecurityID.ReservedSecurityId.Deployer.getCn();
            EzElastic.Client elasticService = null;
            try {
                //Since the allowedUsers field contains DNs and we want to do exact matching, we tell elastic not to
                //tokenize the field.  this means we will only be able to do exact matches, no free text queries on that
                //field
                elasticService = getElasticServiceClient();
                elasticService.setTypeMapping(ApplicationIndex, getCustomMapping(ApplicationIndex), getSecurityToken());
            } catch (TException e) {
                logger.error("Failed to set document mapping.  Since this means no one will be able to see their apps, exiting", e);
                throw new RuntimeException("Failed to set document mapping", e);
            } finally {
                pool.returnToPool(elasticService);
            }
        }

        return new InternalNameService.Processor(this);
    }

    @Override
    public boolean ping() {
        if (inDevMode) {
            return true;
        }

        EzElastic.Client elasticService = null;
        //We're only healthy if our underlying dataset is healthy
        try {
            elasticService = getElasticServiceClient();
            boolean ping = elasticService.ping();
            if (!ping) {
                logger.error("Document service client was retrieved, however, ping failed");
            }
            return ping;
        } catch (Exception ex) {
            logger.error("Failed trying to ping document service", ex);
            return false;
        } finally {
            pool.returnToPool(elasticService);
        }
    }

    private void addDocument(String id, String index, String jsonDocument, EzSecurityToken token) throws TException {
        String visibility = getConfigurationProperties().getProperty(DefaultVisibilityProperty, "U");
        //Create the object required for indexing
        Document document = new Document(index, new Visibility().setFormalVisibility(visibility), jsonDocument);
        document.set_version(1);
        document.set_id(id);

        //Use the pool again
        EzElastic.Client elasticService = getElasticServiceClient();
        try {
            //Index away
            EzSecurityToken elasticToken = getElasticServiceToken(token);
            elasticService.put(document, elasticToken);
            elasticService.forceIndexRefresh(elasticToken);
        } finally {
            pool.returnToPool(elasticService);
        }
    }

    private SearchResult search(Query query, String index, EzSecurityToken token) throws TException {
        query.setType(index);
        //Use the pool again
        EzElastic.Client elasticService = getElasticServiceClient();
        if (!query.isSetPage()) {
            //If no default page information is set, do it here
            query.setPage(new Page().setPageSize((short) 1000).setOffset(0));
        }
        try {
            return elasticService.query(query, getElasticServiceToken(token));
        } finally {
            pool.returnToPool(elasticService);
        }
    }

    private String get(String id, String index, EzSecurityToken token) throws TException {
        //Use the pool again
        EzElastic.Client elasticService = getElasticServiceClient();
        try {
            Document document = elasticService.getWithType(id, index, getElasticServiceToken(token));
            return document.get_jsonObject();
        } finally {
            pool.returnToPool(elasticService);
        }
    }

    @VisibleForTesting
    protected Set<Application> getApplications(String query, EzSecurityToken token) throws TException {
        SearchResult results = search(new Query(query), ApplicationIndex, token);
        Set<Application> applications = Sets.newHashSet();
        for (Document doc : results.getMatchingDocuments()) {
            String json = doc.get_jsonObject();
            try {
                Application app = objectMapper.readValue(json, Application.class);
                applications.add(app);
            } catch (Exception ex) {
                throw new TException(ex);
            }
        }
        return applications;
    }


    @VisibleForTesting
    protected Application getApplication(String appId) throws TException {
        return getApplication(appId, getSecurityToken());
    }

    @VisibleForTesting
    protected Application getApplication(String appId, EzSecurityToken token) throws TException {
        String appDoc = get(appId, ApplicationIndex, token);
        if (!Strings.isNullOrEmpty(appDoc)) {
            try {
                return objectMapper.readValue(appDoc, Application.class);
            } catch (Exception ex) {
                throw new TException(ex);
            }
        } else {
            throw new ApplicationNotFoundException(appId + " not found");
        }

    }

    private void delete(String id, String index, EzSecurityToken token) throws TException {
        //Use the pool again
        EzElastic.Client elasticService = getElasticServiceClient();
        try {
            elasticService.deleteWithType(id, index, getElasticServiceToken(token));
        } finally {
            pool.returnToPool(elasticService);
        }
    }

    private void getTopicsForPipeline(String feedName, String lookingFor, Set<BroadcastTopic> broadcastTopics,
                                      Set<String> allTopics) {
        if (feedName != null && feedName.equalsIgnoreCase(lookingFor) && broadcastTopics != null) {
            for (BroadcastTopic topic : broadcastTopics) {
                allTopics.add(topic.getName());
            }
        }
    }

    private EzElastic.Client getElasticServiceClient() throws TException {
        return pool.getClient(InternalNameServiceConstants.SERVICE_NAME, ElasticServiceName,
                EzElastic.Client.class);
    }

    private EzSecurityToken getElasticServiceToken(EzSecurityToken origToken) throws TException {
        if (new EzProperties(getConfigurationProperties(), true).getBoolean("ezbake.security.fake.token", false)) {
            return ThriftTestUtils.generateTestSecurityToken(applicationSecurityId, applicationSecurityId, Lists.newArrayList("U"));
        } else {
            return securityClient.fetchDerivedTokenForApp(origToken, applicationSecurityId);
        }
    }

    /**
     * This should only be used when a token is not being passed in
     *
     * @return The application token (or a fake token if ezbake.security.fake.token is set to true)
     */
    @VisibleForTesting
    protected EzSecurityToken getSecurityToken() {
        if (new EzProperties(getConfigurationProperties(), true).getBoolean("ezbake.security.fake.token", false)) {
            return ThriftTestUtils.generateTestSecurityToken(applicationSecurityId, applicationSecurityId, Lists.newArrayList("U"));
        } else {
            try {
                return securityClient.fetchAppToken();
            } catch (Exception ex) {
                logger.error("Failed to get security token for INS", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    @VisibleForTesting
    protected boolean validateToken(EzSecurityToken token) {
        //Only the deployer and INS can deploy
        try {
            securityClient.validateReceivedToken(token);
        } catch (EzSecurityTokenException e) {
            logger.error("Token validation failed. ", e);
            throw new SecurityException("Token failed validation");
        }

        String fromId = token.getValidity().getIssuedTo();
        String toId = token.getValidity().getIssuedFor();
        if (!fromId.equals(toId) && !fromId.equals(deployerSecurityId)) {
            throw new SecurityException(String.format(
                    "This call can only be made from Deployer (%s) or INS services. From: %s - To: %s",
                    deployerSecurityId, fromId, toId));
        }

        return true;
    }

    private void devModeUnsupported() throws TException {
        if (inDevMode) {
            logger.error("This method is not supported in dev mode");
            throw new TException("INS Development Mode does not support this method call");
        }
    }

    private String getThriftJson(TBase thriftObject) throws TException {
        TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
        return serializer.toString(thriftObject);
    }

    @VisibleForTesting
    protected String getTermQuery(final String field, final String value) {
        return String.format("{\"constant_score\" : {\"filter\" : {\"term\" : { \"%s\" : \"%s\"} }}}", field, value);
    }

    private String getExistsQuery(final String field, final String value) {
        return String.format("{ \"constant_score\" : { \"filter\" : { \"exists\" : { \"field\" : \"%s%s\" }}}}", field, value);
    }

    private String getCustomMapping(final String index) {
        return String.format("{\"%s\" : { \"properties\" : { \"allowedUsers\": { \"type\": \"string\", \"index\" : \"not_analyzed\" } } } }", index);
    }

    private boolean isApplicationRegistrationApproved(String appId, EzSecurityToken token) throws TException {
        String registrationStatus = getApplicationRegistrationStatus(appId, token).name();

        if (!registrationStatus.equals(RegistrationStatus.ACTIVE.name())) {
            logger.error("Application with application id {} has registration status {}. Expected {}.", appId, registrationStatus, RegistrationStatus.ACTIVE.name());
            return false;
        }

        return true;
    }

    private <T> T getTestOutput(String methodName, String parameter, TypeReference reference) {
        String configValue = "ins.dev.mode." + methodName + (parameter.length() > 0 ? "." + parameter : "");
        String jsonInput = getConfigurationProperties().getProperty(configValue);
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonInput, reference);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get json from config, attempting to read property '"
                    + configValue
                    + "' ", ex);
        }
    }

    @VisibleForTesting
    protected RegistrationStatus getApplicationRegistrationStatus(String appId, EzSecurityToken token) throws TException {
        EzSecurityRegistration.Iface client = null;

        try {
            client = getEzSecurityRegistrationClient();
            EzSecurityToken ezToken = getRegistrationSecurityToken(token);
            return client.getRegistration(ezToken, appId).getStatus();

        } finally {
            pool.returnToPool((TServiceClient) client);
        }
    }

    protected EzSecurityRegistration.Iface getEzSecurityRegistrationClient() throws TException {
        return pool.getClient(EzSecurityRegistrationConstants.SERVICE_NAME, EzSecurityRegistration.Client.class);
    }

    protected EzSecurityToken getRegistrationSecurityToken(EzSecurityToken origToken) {
        try {
            return securityClient.fetchDerivedTokenForApp(origToken, pool.getSecurityId(
                    EzSecurityRegistrationConstants.SERVICE_NAME));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    private static class CategoryDocument {
        @SuppressWarnings("unused") //needed for serialization
        public CategoryDocument() {

        }

        public CategoryDocument(String category) {
            this.category = category;
        }

        public String category;
    }

    private static class SystemTopicDocument {
        @SuppressWarnings("unused") //needed for serialization
        public SystemTopicDocument() {

        }

        public SystemTopicDocument(String systemTopic) {
            this.systemTopic = systemTopic;
        }

        public String systemTopic;
    }

    private Set<String> getAppBroadcastTopicNames() {
        Set<String> result = new HashSet<>();
        Set<Application> apps;
        Set<BroadcastTopic> topics = new HashSet<>();

        try {
            apps = getApplications(getExistsQuery(FeedPipelinesField, "*"), getSecurityToken());
            apps.addAll(getApplications(getExistsQuery(ListenerPipelinesField, "*"), getSecurityToken()));

            for (Application app : apps) {
                try {
                    Set<FeedPipeline> feeds = app.getFeedPipelines();
                    Set<ListenerPipeline> listeners = app.getListenerPipelines();

                    if (feeds != null) {
                        for (FeedPipeline feed : feeds) {
                            if (feed.isSetBroadcastTopics()) {
                                topics.addAll(feed.getBroadcastTopics());
                            }
                        }
                    }

                    if (listeners != null) {
                        for (ListenerPipeline listener : listeners) {
                            if (listener.isSetBroadcastTopics()) {
                                topics.addAll(listener.getBroadcastTopics());
                            }
                        }
                    }
                } catch (Exception ex) {
                    //Don't let one bad app ruin it for everyone
                    logger.error("Failed to get broadcast topics for application " + app.getAppName(), ex);
                }
            }

            for (BroadcastTopic topic : topics) {
                result.add(topic.getName()); //Sets are unique; so, duplicates will automatically be dropped.
            }
        } catch (TException e) {
            logger.error("Error retrieving applications");
        }

        return result;
    }
}
