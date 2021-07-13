/*eslint block-scoped-var: "error"*/

import {
    GetTopicsResponse, Topic, GetConsumerGroupsResponse, GroupDescription, UserData,
    ClusterInfo, TopicMessage, TopicConfigResponse,
    ClusterInfoResponse, GetPartitionsResponse, Partition, GetTopicConsumersResponse, TopicConsumer, AdminInfo, TopicPermissions,
    TopicDocumentationResponse, AclRequest, AclResponse, SchemaOverview, SchemaOverviewResponse, SchemaDetailsResponse, SchemaDetails,
    SchemaType,TopicDocumentation, TopicDescription, ApiError, PartitionReassignmentsResponse, PartitionReassignments,
    PartitionReassignmentRequest, AlterPartitionReassignmentsResponse, Broker, GetAllPartitionsResponse,
    AclRequestDefault, AclResourceType, PatchConfigsResponse, EndpointCompatibilityResponse, EndpointCompatibility, ConfigResourceType,
    AlterConfigOperation, ResourceConfig, PartialTopicConfigsResponse, GetConsumerGroupResponse, EditConsumerGroupOffsetsRequest,
    EditConsumerGroupOffsetsTopic, EditConsumerGroupOffsetsResponse, EditConsumerGroupOffsetsResponseTopic, DeleteConsumerGroupOffsetsTopic,
    DeleteConsumerGroupOffsetsResponseTopic, DeleteConsumerGroupOffsetsRequest, DeleteConsumerGroupOffsetsResponse, TopicOffset,
    GetTopicOffsetsByTimestampResponse, BrokerConfigResponse, ConfigEntry, PatchConfigsRequest
} from "./restInterfaces";
import { comparer, computed, observable, transaction } from "mobx";
import fetchWithTimeout from "../utils/fetchWithTimeout";
import { TimeSince } from "../utils/utils";
import { LazyMap } from "../utils/LazyMap";
import { toJson } from "../utils/jsonUtils";
import { IsDev, IsBusiness, basePathS } from "../utils/env";
import { appGlobal } from "./appGlobal";
import { ServerVersionInfo, uiState } from "./uiState";
import { notification } from "antd";
import { ObjToKv } from "../utils/tsxUtils";
import { Features } from "./supportedFeatures";

const REST_TIMEOUT_SEC = 25;
export const REST_CACHE_DURATION_SEC = 20;
const REST_DEBUG_BASE_URL = null// || "http://localhost:9090"; // only uncommented using "npm run build && serve -s build"

export async function rest<T>(url: string, timeoutSec: number = REST_TIMEOUT_SEC, requestInit?: RequestInit): Promise<T | null> {

    if (REST_DEBUG_BASE_URL) {
        url = REST_DEBUG_BASE_URL + url;
        if (!requestInit) requestInit = {};
        requestInit.mode = "no-cors";
        requestInit.cache = "no-cache";
    }

    const res = await fetchWithTimeout(url, timeoutSec * 1000, requestInit);

    if (res.status == 401) { // Unauthorized
        await handle401(res);
    }
    if (res.status == 403) { // Forbidden
        return null;
    }

    processVersionInfo(res);

    if (!res.ok) {
        const text = await res.text();
        try {
            const errObj = JSON.parse(text) as ApiError;
            if (errObj && typeof errObj.statusCode !== "undefined" && typeof errObj.message !== "undefined") {
                // if the shape matches, reformat it a bit
                throw new Error(`${errObj.message} (${res.status} - ${res.statusText})`);
            }
        }
        catch { } // response is not json

        // use generic error text
        throw new Error(`${text} (${res.status} - ${res.statusText})`);
    }

    const str = await res.text();
    // console.log('json: ' + str);
    const data = (JSON.parse(str) as T);
    return data;
}

async function handle401(res: Response) {
    // Logout
    //   Clear our 'User' data if we have any
    //   Any old/invalid JWT will be cleared by the server
    api.userData = null;

    try {
        const text = await res.text();
        const obj = JSON.parse(text);
        console.log("unauthorized message: " + text);

        const err = obj as ApiError;
        uiState.loginError = String(err.message);
    } catch (err) {
        uiState.loginError = String(err);
    }

    // Save current location url
    // store.urlBeforeLogin = window.location.href;

    // Redirect to login
    appGlobal.history.push('/login');
}

function processVersionInfo(res: Response) {
    try {
        for (const [k, v] of res.headers) {
            if (k.toLowerCase() == 'app-version') {
                const serverVersion = JSON.parse(v) as ServerVersionInfo;
                if (typeof serverVersion === 'object')
                    if (uiState.serverVersion == null || (serverVersion.ts != uiState.serverVersion.ts))
                        uiState.serverVersion = serverVersion;
                break;
            }
        }
    } catch { } // Catch malformed json (old versions where info is not sent as json yet)
}

const cache = new LazyMap<string, CacheEntry>(u => new CacheEntry(u));
class CacheEntry {
    url: string;

    private timeSinceLastResult = new TimeSince(); // set automatically
    /** How long ago (in seconds) the data was last updated */
    get resultAge() { return this.timeSinceLastResult.value / 1000; }

    private promise: Promise<any>;
    get lastPromise() { return this.promise; }
    setPromise<T>(promise: Promise<T>) {
        this.timeSinceRequestStarted.reset();

        this.isPending = true;
        this.promise = promise;

        promise.then(result => {
            this.timeSinceLastResult.reset();
            this.lastResult = result;
        }).finally(() => {
            this.lastRequestTime = this.timeSinceRequestStarted.value;
            const index = api.activeRequests.indexOf(this);
            if (index > -1) {
                api.activeRequests.splice(index, 1);
            }
            this.isPending = false;
        });

        api.activeRequests.push(this);
    }

    lastResult: any | undefined; // set automatically
    isPending: boolean; // set automatically

    private timeSinceRequestStarted = new TimeSince(); // set automatically
    private lastRequestTime: number; // set automatically
    /** How long (in seconds) the last request took (or is currently taking so far) */
    get requestTime() {
        if (this.isPending) {
            return this.timeSinceRequestStarted.value / 1000;
        }
        return this.lastRequestTime / 1000;
    }

    constructor(url: string) {
        this.url = url;
        const sec = 1000;
        const min = 60 * sec;
        const h = 60 * min;
        this.timeSinceLastResult.reset(100 * h);
    }
}

function cachedApiRequest<T>(url: string, force: boolean = false): Promise<T> {
    const entry = cache.get(url);

    if (entry.isPending) {
        // return already running request
        return entry.lastPromise;
    }

    if (entry.resultAge > REST_CACHE_DURATION_SEC || force) {
        // expired or force refresh
        const promise = rest<T>(url);
        entry.setPromise(promise);
    }

    // Return last result (can be still pending, or completed but not yet expired)
    return entry.lastPromise;
}


let currentWS: WebSocket | null = null;

//
// BackendAPI
//
const apiStore = {

    // Data
    endpointCompatibility: null as (EndpointCompatibility | null),

    clusters: ['A', 'B', 'C'],
    clusterInfo: null as (ClusterInfo | null),

    brokerConfigs: new Map<number, ConfigEntry[] | string>(), // config entries, or error string

    adminInfo: undefined as (AdminInfo | undefined | null),

    schemaOverview: undefined as (SchemaOverview | null | undefined), // undefined = request not yet complete; null = server responded with 'there is no data'
    schemaOverviewIsConfigured: undefined as boolean | undefined,
    schemaDetails: null as (SchemaDetails | null),

    topics: null as (Topic[] | null),
    topicConfig: new Map<string, TopicDescription | null>(), // null = not allowed to view config of this topic
    topicDocumentation: new Map<string, TopicDocumentation>(),
    topicPermissions: new Map<string, TopicPermissions | null>(),
    topicPartitions: new Map<string, Partition[] | null>(), // null = not allowed to view partitions of this config
    topicConsumers: new Map<string, TopicConsumer[]>(),
    topicAcls: new Map<string, AclResponse | null>(),

    ACLs: undefined as AclResponse | undefined | null,

    consumerGroups: new Map<string, GroupDescription>(),
    consumerGroupAcls: new Map<string, AclResponse | null>(),

    partitionReassignments: undefined as (PartitionReassignments[] | null | undefined),

    // undefined = we haven't checked yet
    // null = call completed, and we're not logged in
    userData: undefined as (UserData | null | undefined),
    async logout() {
        await fetch('./logout');
        this.userData = null;
    },

    // Make currently running requests observable
    activeRequests: [] as CacheEntry[],

    // Fetch errors
    errors: [] as any[],

    messageSearchPhase: null as string | null,
    messagesFor: '', // for what topic?
    messages: [] as TopicMessage[],
    messagesElapsedMs: null as null | number,
    messagesBytesConsumed: 0,
    messagesTotalConsumed: 0,


    async startMessageSearch(searchRequest: MessageSearchRequest): Promise<void> {

        const isHttps = window.location.protocol.startsWith('https');
        const protocol = isHttps ? 'wss://' : 'ws://';
        const host = IsDev ? 'localhost:9090' : window.location.host;
        const url = protocol + host + basePathS + '/api/topics/' + searchRequest.topicName + '/messages';

        console.debug("connecting to \"" + url + "\"");

        // Abort previous connection
        if (currentWS != null)
            if (currentWS.readyState == WebSocket.OPEN || currentWS.readyState == WebSocket.CONNECTING)
                currentWS.close();

        currentWS = new WebSocket(url);
        const ws = currentWS;
        this.messageSearchPhase = "Connecting";
        this.messagesBytesConsumed = 0;
        this.messagesTotalConsumed = 0;

        currentWS.onopen = ev => {
            if (ws !== currentWS) return; // newer request has taken over
            // reset state for new request
            this.messagesFor = searchRequest.topicName;
            this.messages = [];
            this.messagesElapsedMs = null;
            // send new request
            currentWS.send(JSON.stringify(searchRequest));
        }
        currentWS.onclose = ev => {
            if (ws !== currentWS) return;
            api.stopMessageSearch();
            // double assignment makes sense: when the phase changes to null, some observing components will play a "fade out" animation, using the last (non-null) value
            console.debug(`ws closed: code=${ev.code} wasClean=${ev.wasClean}` + (ev.reason ? ` reason=${ev.reason}` : ''))
        }

        const onMessageHandler = (msgEvent: MessageEvent) => {
            if (ws !== currentWS) return;
            const msg = JSON.parse(msgEvent.data)

            switch (msg.type) {
                case 'phase':
                    this.messageSearchPhase = msg.phase;
                    break;

                case 'progressUpdate':
                    this.messagesBytesConsumed = msg.bytesConsumed;
                    this.messagesTotalConsumed = msg.messagesConsumed;
                    break;

                case 'done':
                    this.messagesElapsedMs = msg.elapsedMs;
                    this.messagesBytesConsumed = msg.bytesConsumed;
                    // this.MessageSearchCancelled = msg.isCancelled;
                    this.messageSearchPhase = "Done";
                    this.messageSearchPhase = null;
                    break;

                case 'error':
                    // error doesn't neccesarily mean the whole request is done
                    console.info("ws backend error: " + msg.message);
                    const notificationKey = `errorNotification-${Date.now()}`;
                    notification['error']({
                        key: notificationKey,
                        message: "Backend Error",
                        description: msg.message,
                        duration: 5,
                    })
                    break;

                case 'message':
                    const m = msg.message as TopicMessage;

                    const keyData = m.key.payload;
                    if (keyData != null && keyData != undefined && keyData != "" && m.key.encoding == 'binary') {
                        try {
                            m.key.payload = atob(m.key.payload); // unpack base64 encoded key
                        } catch (error) {
                            // Empty
                            // Only unpack if the key is base64 based
                        }
                    }

                    m.keyJson = JSON.stringify(m.key.payload);
                    m.valueJson = JSON.stringify(m.value.payload);

                    if (m.value.encoding == 'binary') {
                        m.value.payload = atob(m.value.payload);

                        const str = m.value.payload as string;
                        let hex = '';
                        for (let i = 0; i < str.length && i < 50; i++) {
                            let n = str.charCodeAt(i).toString(16);
                            if (n.length == 1) n = '0' + n;
                            hex += n + ' ';
                        }
                        m.valueBinHexPreview = hex;
                    }


                    //m = observable.object(m, undefined, { deep: false });

                    this.messages.push(m);
                    break;
            }
        };
        //currentWS.onmessage = m => transaction(() => onMessageHandler(m));
        currentWS.onmessage = onMessageHandler;
    },

    stopMessageSearch() {
        if (currentWS) {
            currentWS.close();
            currentWS = null;
        }

        if (this.messageSearchPhase != null) {
            this.messageSearchPhase = "Done";
            this.messagesBytesConsumed = 0;
            this.messagesTotalConsumed = 0;
            this.messageSearchPhase = null;
        }
    },

    refreshTopics(force?: boolean) {
        cachedApiRequest<GetTopicsResponse>('./api/topics', force)
            .then(v => {
                for (const t of v.topics) {
                    if (!t.allowedActions) continue;

                    // DEBUG: randomly remove some allowedActions
                    /*
                    const numToRemove = Math.round(Math.random() * t.allowedActions.length);
                    for (let i = 0; i < numToRemove; i++) {
                        const randomIndex = Math.round(Math.random() * (t.allowedActions.length - 1));
                        t.allowedActions.splice(randomIndex, 1);
                    }
                    */
                }
                this.topics = v.topics;
            }, addError);
    },

    async refreshTopicConfig(topicName: string, force?: boolean): Promise<void> {
        const promise = cachedApiRequest<TopicConfigResponse | null>(`./api/topics/${topicName}/configuration`, force)
            .then(v => this.topicConfig.set(topicName, addSynonymTypes(v) ?? null), addError); // 403 -> null
        return promise as Promise<void>;
    },

    async getTopicOffsetsByTimestamp(topicNames: string[], timestampUnixMs: number): Promise<TopicOffset[]> {
        const query = `topicNames=${encodeURIComponent(topicNames.join(','))}&timestamp=${timestampUnixMs}`;
        const response = await fetch('./api/topics-offsets?' + query, {
            method: 'GET',
            headers: [
                ['Content-Type', 'application/json']
            ]
        });

        if (!response.ok) {
            const text = await response.text();
            try {
                const errObj = JSON.parse(text) as ApiError;
                if (errObj && typeof errObj.statusCode !== "undefined" && typeof errObj.message !== "undefined") {
                    // if the shape matches, reformat it a bit
                    throw new Error(`${errObj.message} (${response.status} - ${response.statusText})`);
                }
            }
            catch { } // not json

            // use generic error text
            throw new Error(`${text} (${response.status} - ${response.statusText})`);
        }

        const str = await response.text();
        const data = (JSON.parse(str) as GetTopicOffsetsByTimestampResponse);
        return data.topicOffsets;
    },

    refreshTopicDocumentation(topicName: string, force?: boolean) {
        cachedApiRequest<TopicDocumentationResponse>(`./api/topics/${topicName}/documentation`, force)
            .then(v => {
                const text = v.documentation.markdown == null ? null : atob(v.documentation.markdown);
                v.documentation.text = text;
                this.topicDocumentation.set(topicName, v.documentation);
            }, addError);
    },

    refreshTopicPermissions(topicName: string, force?: boolean) {
        if (!IsBusiness) return; // permissions endpoint only exists in kowl-business
        if (this.userData?.user?.providerID == -1) return; // debug user
        cachedApiRequest<TopicPermissions | null>(`./api/permissions/topics/${topicName}`, force)
            .then(x => this.topicPermissions.set(topicName, x), addError);
    },

    refreshPartitions(topics: 'all' | string[] = 'all', force?: boolean): Promise<void> {
        if (Array.isArray(topics))
            // sort in order to maximize cache hits (todo: track/cache each topic individually instead)
            topics = topics.sort().map(t => encodeURIComponent(t));

        const url = topics == 'all'
            ? `./api/operations/topic-details`
            : `./api/operations/topic-details?topicNames=${topics.joinStr(",")}`;

        return cachedApiRequest<GetAllPartitionsResponse | null>(url, force)
            .then(response => {
                if (!response?.topics) return;
                transaction(() => {

                    const errors: {
                        topicName: string,
                        partitionErrors: { partitionId: number, error: string }[],
                        waterMarkErrors: { partitionId: number, error: string }[],
                    }[] = [];

                    for (const t of response.topics) {
                        if (t.error != null) {
                            console.error(`refreshAllTopicPartitions: error for topic ${t.topicName}: ${t.error}`);
                            continue;
                        }

                        // If any partition has any errors, don't set the result for that topic
                        const partitionErrors = [];
                        const waterMarkErrors = [];
                        for (const p of t.partitions) {
                            // topicName
                            p.topicName = t.topicName;

                            let partitionHasError = false;
                            if (p.partitionError) {
                                partitionErrors.push({ partitionId: p.id, error: p.partitionError });
                                partitionHasError = true;
                            } if (p.waterMarksError) {
                                waterMarkErrors.push({ partitionId: p.id, error: p.waterMarksError });
                                partitionHasError = true;
                            }
                            if (partitionHasError) continue;

                            // Add some local/cached properties to make working with the data easier
                            const validLogDirs = p.partitionLogDirs.filter(e => !e.error && e.size >= 0);
                            const replicaSize = validLogDirs.length > 0 ? validLogDirs.max(e => e.size) : 0;
                            p.replicaSize = replicaSize >= 0 ? replicaSize : 0;
                        }

                        // Set partition
                        this.topicPartitions.set(t.topicName, t.partitions);

                        if (partitionErrors.length == 0 && waterMarkErrors.length == 0) {

                        } else {
                            errors.push({
                                topicName: t.topicName,
                                partitionErrors: partitionErrors,
                                waterMarkErrors: waterMarkErrors,
                            })
                        }
                    }

                    if (errors.length > 0)
                        console.error('refreshAllTopicPartitions: response had errors', errors);
                });
            }, addError);
    },

    refreshPartitionsForTopic(topicName: string, force?: boolean) {
        cachedApiRequest<GetPartitionsResponse | null>(`./api/topics/${topicName}/partitions`, force)
            .then(response => {
                if (!response?.partitions) {
                    // Set null to indicate that we're not allowed to see the partitions
                    this.topicPartitions.set(topicName, null);
                    return;
                }

                let partitionErrors = 0, waterMarkErrors = 0;

                // Add some local/cached properties to make working with the data easier
                for (const p of response.partitions) {
                    // topicName
                    p.topicName = topicName;

                    if (p.partitionError) partitionErrors++;
                    if (p.waterMarksError) waterMarkErrors++;
                    if (partitionErrors || waterMarkErrors) continue;

                    // replicaSize
                    const validLogDirs = p.partitionLogDirs.filter(e => (e.error == null || e.error == "") && e.size >= 0);
                    const replicaSize = validLogDirs.length > 0 ? validLogDirs.max(e => e.size) : 0;
                    p.replicaSize = replicaSize >= 0 ? replicaSize : 0;
                }

                // Set partitions
                this.topicPartitions.set(topicName, response.partitions);

                if (partitionErrors > 0 || waterMarkErrors > 0)
                    console.warn(`refreshPartitionsForTopic: response has partition errors (topic=${topicName} partitionErrors=${partitionErrors}, waterMarkErrors=${waterMarkErrors})`);
            }, addError);
    },

    refreshTopicAcls(topicName: string, force?: boolean) {
        const query = aclRequestToQuery({ ...AclRequestDefault, resourceType: AclResourceType.AclResourceTopic, resourceName: topicName })
        cachedApiRequest<AclResponse | null>(`./api/acls?${query}`, force)
            .then(v => this.topicAcls.set(topicName, v))
    },

    refreshTopicConsumers(topicName: string, force?: boolean) {
        cachedApiRequest<GetTopicConsumersResponse>(`./api/topics/${topicName}/consumers`, force)
            .then(v => this.topicConsumers.set(topicName, v.topicConsumers), addError);
    },

    refreshAcls(request: AclRequest, force?: boolean) {
        const query = aclRequestToQuery(request);
        cachedApiRequest<AclResponse | null>(`./api/acls?${query}`, force)
            .then(v => this.ACLs = v ?? null, addError);
    },

    refreshSupportedEndpoints(force?: boolean) {
        cachedApiRequest<EndpointCompatibilityResponse>(`./api/kowl/endpoints`, force)
            .then(v => this.endpointCompatibility = v.endpointCompatibility, addError);
    },

    refreshCluster(force?: boolean) {
        cachedApiRequest<ClusterInfoResponse>(`./api/cluster`, force)
            .then(v => {
                transaction(() => {
                    // don't assign if the value didn't change
                    // we'd re-trigger all observers!
                    if (!comparer.structural(this.clusterInfo, v.clusterInfo))
                        this.clusterInfo = v.clusterInfo;

                    for (const b of v.clusterInfo.brokers)
                        if (b.config.error)
                            this.brokerConfigs.set(b.brokerId, b.config.error)
                        else
                            this.brokerConfigs.set(b.brokerId, b.config.configs);
                });

            }, addError);
    },

    refreshBrokerConfig(brokerId: number, force?: boolean) {
        cachedApiRequest<BrokerConfigResponse | ApiError>(`./api/brokers/${brokerId}/config`, force).then(v => {
            if ('message' in v) {
                this.brokerConfigs.set(brokerId, v.message); // error
            } else {
                this.brokerConfigs.set(brokerId, v.brokerConfigs);
            }
        }).catch(addError);
    },

    refreshConsumerGroup(groupId: string, force?: boolean) {
        cachedApiRequest<GetConsumerGroupResponse>(`./api/consumer-groups/${groupId}`, force)
            .then(v => {
                addFrontendFieldsForConsumerGroup(v.consumerGroup);
                this.consumerGroups.set(v.consumerGroup.groupId, v.consumerGroup);
            }, addError);
    },

    refreshConsumerGroups(force?: boolean) {
        cachedApiRequest<GetConsumerGroupsResponse>('./api/consumer-groups', force)
            .then(v => {
                for (const g of v.consumerGroups)
                    addFrontendFieldsForConsumerGroup(g);

                transaction(() => {
                    this.consumerGroups.clear();
                    for (const g of v.consumerGroups)
                        this.consumerGroups.set(g.groupId, g);
                });
            }, addError);
    },

    refreshConsumerGroupAcls(groupName: string, force?: boolean) {
        const query = aclRequestToQuery({ ...AclRequestDefault, resourceType: AclResourceType.AclResourceGroup, resourceName: groupName })
        cachedApiRequest<AclResponse | null>(`./api/acls?${query}`, force)
            .then(v => this.consumerGroupAcls.set(groupName, v))
    },

    async editConsumerGroupOffsets(groupId: string, topics: EditConsumerGroupOffsetsTopic[]):
        Promise<EditConsumerGroupOffsetsResponseTopic[]> {
        const request: EditConsumerGroupOffsetsRequest = {
            groupId: groupId,
            topics: topics
        };

        const response = await fetch('./api/consumer-groups/' + encodeURIComponent(groupId), {
            method: 'PATCH',
            headers: [
                ['Content-Type', 'application/json']
            ],
            body: toJson(request),
        });

        if (!response.ok) {
            const text = await response.text();
            try {
                const errObj = JSON.parse(text) as ApiError;
                if (errObj && typeof errObj.statusCode !== "undefined" && typeof errObj.message !== "undefined") {
                    // if the shape matches, reformat it a bit
                    throw new Error(`${errObj.message} (${response.status} - ${response.statusText})`);
                }
            }
            catch { } // not json

            // use generic error text
            throw new Error(`${text} (${response.status} - ${response.statusText})`);
        }

        const str = await response.text();
        const data = (JSON.parse(str) as EditConsumerGroupOffsetsResponse);
        if (data.error) throw data.error;
        return data.topics;
    },

    async deleteConsumerGroupOffsets(groupId: string, topics: DeleteConsumerGroupOffsetsTopic[]):
        Promise<DeleteConsumerGroupOffsetsResponseTopic[]> {
        const request: DeleteConsumerGroupOffsetsRequest = {
            groupId: groupId,
            topics: topics
        };

        const response = await fetch('./api/consumer-groups/' + encodeURIComponent(groupId), {
            method: 'DELETE',
            headers: [
                ['Content-Type', 'application/json']
            ],
            body: toJson(request),
        });

        if (!response.ok) {
            const text = await response.text();
            let errObj;
            try {
                errObj = JSON.parse(text) as ApiError;
                if (errObj && typeof errObj.statusCode !== "undefined" && typeof errObj.message !== "undefined") {
                    // if the shape matches, reformat it a bit
                    errObj = new Error(`${errObj.message} (${response.status} - ${response.statusText})`);
                }
            }
            catch { } // not json

            if (errObj) throw errObj;

            // use generic error text
            throw new Error(`${text} (${response.status} - ${response.statusText})`);
        }

        const str = await response.text();
        const data = (JSON.parse(str) as DeleteConsumerGroupOffsetsResponse);
        return data.topics;
    },


    refreshAdminInfo(force?: boolean) {
        cachedApiRequest<AdminInfo | null>(`./api/admin`, force)
            .then(info => {
                if (info == null) {
                    this.adminInfo = null;
                    return;
                }

                // normalize responses (missing arrays, or arrays with an empty string)
                // todo: not needed anymore, responses are always correct now
                for (const role of info.roles)
                    for (const permission of role.permissions)
                        for (const k of ['allowedActions', 'includes', 'excludes']) {
                            const ar: string[] = (permission as any)[k] ?? [];
                            (permission as any)[k] = ar.filter(x => x.length > 0);
                        }

                // resolve role of each binding
                for (const binding of info.roleBindings) {
                    binding.resolvedRole = info.roles.first(r => r.name == binding.roleName)!;
                    if (binding.resolvedRole == null) console.error("could not resolve roleBinding to role: " + toJson(binding));
                }

                // resolve bindings, and roles of each user
                for (const user of info.users) {
                    user.bindings = user.bindingIds.map(id => info.roleBindings.first(rb => rb.ephemeralId == id)!);
                    if (user.bindings.any(b => b == null)) console.error("one or more rolebindings could not be resolved for user: " + toJson(user));

                    user.grantedRoles = [];
                    for (const roleName in user.audits)
                        user.grantedRoles.push({
                            role: info.roles.first(r => r.name == roleName)!,
                            grantedBy: user.audits[roleName].map(bindingId => info.roleBindings.first(b => b.ephemeralId == bindingId)!),
                        });
                }

                this.adminInfo = info;
            }, addError);
    },

    refreshSchemaOverview(force?: boolean) {
        const rq = cachedApiRequest('./api/schemas', force) as Promise<SchemaOverviewResponse>;
        return rq
            .then(({ schemaOverview, isConfigured }) => [this.schemaOverview, this.schemaOverviewIsConfigured] = [schemaOverview, isConfigured])
            .catch(addError);
    },

    refreshSchemaDetails(subjectName: string, version: number | 'latest', force?: boolean) {
        if (version == null) version = 'latest';

        const rq = cachedApiRequest(`./api/schemas/subjects/${subjectName}/versions/${version}`, force) as Promise<SchemaDetailsResponse>;

        return rq
            .then(({ schemaDetails }) => { 
              if ( schemaDetails && typeof schemaDetails.schema === "string" && schemaDetails.type != SchemaType.PROTOBUF) {
                schemaDetails.schema = JSON.parse(schemaDetails.schema);
              }
              this.schemaDetails = schemaDetails
            })
            .catch(addError);
    },

    refreshPartitionReassignments(force?: boolean): Promise<void> {
        return cachedApiRequest<PartitionReassignmentsResponse | null>('./api/operations/reassign-partitions', force)
            .then(v => {
                if (v === null)
                    this.partitionReassignments = null;
                else
                    this.partitionReassignments = v.topics;
            }, addError);
    },

    async startPartitionReassignment(request: PartitionReassignmentRequest): Promise<AlterPartitionReassignmentsResponse> {
        const response = await fetch('./api/operations/reassign-partitions', {
            method: 'PATCH',
            headers: [
                ['Content-Type', 'application/json']
            ],
            body: toJson(request),
        });

        if (!response.ok) {
            const text = await response.text();
            try {
                const errObj = JSON.parse(text) as ApiError;
                if (errObj && typeof errObj.statusCode !== "undefined" && typeof errObj.message !== "undefined") {
                    // if the shape matches, reformat it a bit
                    throw new Error(`${errObj.message} (${response.status} - ${response.statusText})`);
                }
            }
            catch { } // not json

            // use generic error text
            throw new Error(`${text} (${response.status} - ${response.statusText})`);
        }

        const str = await response.text();
        const data = (JSON.parse(str) as AlterPartitionReassignmentsResponse);
        return data;
    },

    async setReplicationThrottleRate(brokerIds: number[], maxBytesPerSecond: number): Promise<PatchConfigsResponse> {

        maxBytesPerSecond = Math.ceil(maxBytesPerSecond);

        const configRequest: PatchConfigsRequest = { resources: [] };

        for (const b of brokerIds) {
            configRequest.resources.push({
                resourceType: ConfigResourceType.Broker,
                resourceName: String(b),
                configs: [
                    { name: 'leader.replication.throttled.rate', op: AlterConfigOperation.Set, value: String(maxBytesPerSecond) },
                    { name: 'follower.replication.throttled.rate', op: AlterConfigOperation.Set, value: String(maxBytesPerSecond) },
                ]
            });
        }

        return await this.changeConfig(configRequest);
    },

    async setThrottledReplicas(
        topicReplicas: {
            topicName: string,
            leaderReplicas: { brokerId: number, partitionId: number }[],
            followerReplicas: { brokerId: number, partitionId: number }[]
        }[]): Promise<PatchConfigsResponse> {

        const configRequest: PatchConfigsRequest = { resources: [] };

        for (const t of topicReplicas) {
            const res: ResourceConfig = { // Set which topics to throttle
                resourceType: ConfigResourceType.Topic,
                resourceName: t.topicName,
                configs: [],
            };

            const leaderReplicas = t.leaderReplicas.map(e => `${e.partitionId}:${e.brokerId}`).join(",");
            res.configs.push({ name: 'leader.replication.throttled.replicas', op: AlterConfigOperation.Set, value: leaderReplicas });
            const followerReplicas = t.followerReplicas.map(e => `${e.partitionId}:${e.brokerId}`).join(",");
            res.configs.push({ name: 'follower.replication.throttled.replicas', op: AlterConfigOperation.Set, value: followerReplicas });

            // individual request for each topic
            configRequest.resources.push(res);
        }

        return await this.changeConfig(configRequest);
    },

    async resetThrottledReplicas(topicNames: string[]): Promise<PatchConfigsResponse> {

        const configRequest: PatchConfigsRequest = { resources: [] };

        // reset throttled replicas for those topics
        for (const t of topicNames) {
            configRequest.resources.push({
                resourceType: ConfigResourceType.Topic,
                resourceName: t,
                configs: [
                    { name: 'leader.replication.throttled.replicas', op: AlterConfigOperation.Delete },
                    { name: 'follower.replication.throttled.replicas', op: AlterConfigOperation.Delete }
                ],
            });
        }

        return await this.changeConfig(configRequest);
    },

    async resetReplicationThrottleRate(brokerIds: number[]): Promise<PatchConfigsResponse> {

        const configRequest: PatchConfigsRequest = { resources: [] };

        // We currently only set replication throttle on each broker, instead of cluster-wide (same effect, but different kind of 'ConfigSource')
        // So we don't remove the cluster-wide setting, only the ones we've set (the per-broker) settings

        // remove throttle configs from all brokers (DYNAMIC_DEFAULT_BROKER_CONFIG)
        // configRequest.resources.push({
        //     resourceType: ConfigResourceType.Broker,
        //     resourceName: "", // empty = all brokers
        //     configs: [
        //         { name: 'leader.replication.throttled.rate', op: AlterConfigOperation.Delete },
        //         { name: 'follower.replication.throttled.rate', op: AlterConfigOperation.Delete },
        //     ]
        // });

        // remove throttle configs from each broker individually (DYNAMIC_BROKER_CONFIG)
        for (const b of brokerIds) {
            configRequest.resources.push({
                resourceType: ConfigResourceType.Broker,
                resourceName: String(b),
                configs: [
                    { name: 'leader.replication.throttled.rate', op: AlterConfigOperation.Delete },
                    { name: 'follower.replication.throttled.rate', op: AlterConfigOperation.Delete },
                ]
            });
        }

        return await this.changeConfig(configRequest);
    },

    async changeConfig(request: PatchConfigsRequest): Promise<PatchConfigsResponse> {
        const response = await fetch('./api/operations/configs', {
            method: 'PATCH',
            headers: [
                ['Content-Type', 'application/json']
            ],
            body: toJson(request),
        });

        if (!response.ok) {
            const text = await response.text();
            try {
                const errObj = JSON.parse(text) as ApiError;
                if (errObj && typeof errObj.statusCode !== "undefined" && typeof errObj.message !== "undefined") {
                    // if the shape matches, reformat it a bit
                    throw new Error(`${errObj.message} (${response.status} - ${response.statusText})`);
                }
            }
            catch { } // not json

            // use generic error text
            throw new Error(`${text} (${response.status} - ${response.statusText})`);
        }

        const str = await response.text();
        const data = (JSON.parse(str) as PatchConfigsResponse);
        return data;
    }
}

function addFrontendFieldsForConsumerGroup(g: GroupDescription) {
    g.lagSum = g.topicOffsets.sum(o => o.summedLag);

    if (g.allowedActions) {
        if (g.allowedActions.includes('all')) {
            // All perms
        } else {
            // Not all perms, set helper props
            g.noEditPerms = !g.allowedActions?.includes('editConsumerGroup');
            g.noDeletePerms = !g.allowedActions?.includes('deleteConsumerGroup');
        }
    }
    g.isInUse = g.state.toLowerCase() != 'empty';

    if (!Features.deleteGroup || !Features.patchGroup)
        g.noEditSupport = true;
}

export const brokerMap = computed(() => {
    const brokers = api.clusterInfo?.brokers;
    if (brokers == null) return null;

    const map = new Map<number, Broker>();
    for (const b of brokers)
        map.set(b.brokerId, b);

    return map;
}, { name: 'brokerMap', equals: comparer.structural });


function addSynonymTypes(topicConfigResponse: TopicConfigResponse | null): TopicDescription | null {
    topicConfigResponse?.topicDescription.configEntries.forEach(configEntry => {
        configEntry.synonyms.forEach(synonym => {
            synonym.type = configEntry.type
        })
    })
    return topicConfigResponse?.topicDescription ?? null;
}

export function aclRequestToQuery(request: AclRequest): string {
    const filters = ObjToKv(request).filter(kv => !!kv.value);
    const query = filters.map(x => `${x.key}=${x.value}`).join('&');
    return query;
}

export async function partialTopicConfigs(configKeys: string[], topics?: string[]): Promise<PartialTopicConfigsResponse> {
    const keys = configKeys.map(k => encodeURIComponent(k)).join(',');
    const topicNames = topics?.map(t => encodeURIComponent(t)).join(',');
    const query = topicNames
        ? `topicNames=${topicNames}&configKeys=${keys}`
        : `configKeys=${keys}`;

    const response = await fetch('./api/topics-configs?' + query);

    if (!response.ok) {
        const text = await response.text();
        try {
            const errObj = JSON.parse(text) as ApiError;
            if (errObj && typeof errObj.statusCode !== "undefined" && typeof errObj.message !== "undefined") {
                // if the shape matches, reformat it a bit
                throw new Error(`${errObj.message} (${response.status} - ${response.statusText})`);
            }
        }
        catch { } // not json

        // use generic error text
        throw new Error(`${text} (${response.status} - ${response.statusText})`);
    }

    const str = await response.text();
    const data = (JSON.parse(str) as PartialTopicConfigsResponse);
    return data;
}

export interface MessageSearchRequest {
    topicName: string,
    startOffset: number,
    partitionId: number,
    maxResults: number, // should also support '-1' soon, so we can do live tailing
    filterInterpreterCode: string, // js code, base64 encoded
}


function addError(err: Error) {
    api.errors.push(err);
    // notification['error']({
    // 	message: 'REST: ' + err.name,
    // 	description: err.message,
    // 	duration: 7
    // })
}


type apiStoreType = typeof apiStore;
export const api = observable(apiStore, { messages: observable.shallow }) as apiStoreType;

/*
autorun(r => {
    console.log(toJson(api))
    touch(api)
}, { delay: 50, name: 'api observer' });
*/
