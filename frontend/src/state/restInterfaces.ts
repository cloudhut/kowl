export interface ApiError {
    statusCode: number;
    message: string;
}


export const TopicActions = ['seeTopic', 'viewPartitions', 'viewMessages', 'useSearchFilter', 'viewConsumers', 'viewConfig'] as const;
export type TopicAction = 'all' | typeof TopicActions[number];

export interface Topic {
    topicName: string;
    isInternal: boolean;
    partitionCount: number;
    replicationFactor: number;
    cleanupPolicy: string;
    logDirSummary: TopicLogDirSummary;
    allowedActions: TopicAction[] | undefined;
}

export interface TopicLogDirSummary {
    totalSizeBytes: number; // how much space this topic takes up (files in its log dir)
    replicaErrors: {
        brokerId: number;
        error: string | null;
    }[] | null;
    hint: string | null;
}

export interface GetTopicsResponse {
    topics: Topic[];
}



export interface Partition {
    id: number;

    // When set, all props from replicas to partitionLogDirs are null
    partitionError: string | null;
    replicas: number[]; // brokerIds of all brokers that host the leader or a replica of this partition
    offlineReplicas: number[] | null;
    inSyncReplicas: number[]; // brokerId (can only be one?) of the leading broker
    leader: number; // id of the "leader" broker for this partition
    partitionLogDirs: {
        error: string, // empty when no error
        brokerId: number,
        partitionId: number, // redundant?
        size: number, // size (in bytes) of log dir on that broker
    }[];

    // When set, waterMarkLow/High are not set
    waterMarksError: string | null;
    waterMarkLow: number;
    waterMarkHigh: number;

    // added by frontend:
    replicaSize: number; // largest known/reported size of any replica; used for estimating how much traffic a reassignment would cause
    topicName: string; // used for finding the actual topic this partition belongs to (in case we pass around a partition reference on its own)
}

export interface GetPartitionsResponse {
    topicName: string;
    error: string | null; // only set if metadata request has failed for the whole topic
    partitions: Partition[];
}

export interface GetAllPartitionsResponse {
    topics: {
        topicName: string;
        error: string | null;
        partitions: Partition[];
    }[];
}



export interface TopicConsumer {
    groupId: string
    summedLag: number
}

export interface GetTopicConsumersResponse {
    topicName: string;
    topicConsumers: TopicConsumer[];
}


export type MessageDataType = 'none' | 'avro' | 'protobuf' | 'json' | 'xml' | 'text' | 'consumerOffsets' | 'binary' | 'msgpack';
export type CompressionType = 'uncompressed' | 'gzip' | 'snappy' | 'lz4' | 'zstd' | 'unknown';
export interface Payload {
    payload: any, // json obj
    encoding: MessageDataType, // actual format of the message (before the backend converted it to json)
    schemaId: number,
    size: number,
}

export interface TopicMessage {
    partitionID: number,
    offset: number,
    timestamp: number,

    compression: CompressionType,
    isTransactional: boolean,

    headers: {
        key: string,
        value: Payload,
    }[]
    key: Payload,
    value: Payload,

    isValueNull: boolean, // todo: rename to isTombstone

    // Added by the frontend
    valueJson: string,
    valueBinHexPreview: string,
    keyJson: string,
}

export interface ListMessageResponse {
    elapsedMs: number,
    fetchedMessages: number,
    isCancelled: boolean,
    messages: TopicMessage[],
}

export interface GetTopicMessagesResponse {
    kafkaMessages: ListMessageResponse,
}



export interface KafkaError {
    code: number,
    message: string,
    description: string
}

export interface ConfigEntry {
    name: string,
    value: string | null,
    source: string,
    type: string,
    isExplicitlySet: boolean,
    isDefaultValue: boolean,
    isReadOnly: boolean,
    isSensitive: boolean,
    // documentation: string, // remvoed for now, we have documentation locally in the frontend
    synonyms: ConfigEntrySynonym[]
}

interface ConfigEntrySynonym {
    name: string,
    value: string | null,
    source: string,

    // added by frontend
    type: string | null,
}

export interface TopicDescription {
    topicName: string
    configEntries: ConfigEntry[]
    error: KafkaError | null
}
export interface TopicConfigResponse {
    topicDescription: TopicDescription
}
export interface PartialTopicConfigsResponse {
    topicDescriptions: TopicDescription[];
}
export interface TopicDocumentation {
    // if false: topic documentation is not configured
    isEnabled: boolean;
    // empty: actually empty
    // null:  no .md docu file found for this topic
    markdown: string | null; // base64

    // added by frontend:
    text: string | null; // atob(markdown)
}
export interface TopicDocumentationResponse {
    topicName: string;
    documentation: TopicDocumentation;
}




export interface GroupMemberAssignment {
    topicName: string;
    partitionIds: number[];

}
export interface GroupMemberDescription {
    id: string; // unique ID assigned to the member after login
    clientId: string; // custom id reported by the member
    clientHost: string; // address/host of the connection
    assignments: GroupMemberAssignment[]; // topics+partitions that the worker is assigned to

}


export const GroupActions = ['seeConsumerGroup', 'editConsumerGroup', 'deleteConsumerGroup'] as const;
export type GroupAction = 'all' | typeof GroupActions[number];

export interface GroupDescription {
    groupId: string; // name of the group
    state: string; // Dead, Initializing, Rebalancing, Stable
    protocol: string;
    protocolType: string; // Will be "consumer" if we can decode the members; otherwise ".members" will be empty, which happens for "sr" (for schema registry) for example
    members: GroupMemberDescription[]; // members (consumers) that are currently present in the group
    coordinatorId: number;
    topicOffsets: GroupTopicOffsets[];
    allowedActions: GroupAction[] | null;

    // Added by frontend
    lagSum: number; // sum of lag for all topic offsets

    // reasons for why the group can't be editted
    noEditSupport: boolean;
    isInUse: boolean;
    noEditPerms: boolean;
    noDeletePerms: boolean;
}

export interface GroupTopicOffsets {
    topic: string;
    summedLag: number; // summed lag of all partitions (non consumed partitions are not considered)
    partitionCount: number;
    partitionsWithOffset: number; // number of partitions that have an active group offset
    partitionOffsets: GroupPartitionOffset[];
}

// PartitionOffset describes the kafka lag for a partition for a single consumer group
export interface GroupPartitionOffset {
    partitionId: number;
    groupOffset: number;

    error: string | undefined; // Error will be set when the high water mark could not be fetched
    highWaterMark: number;
    lag: number;
}


export interface EditConsumerGroupOffsetsRequest {
    groupId: string;
    topics: EditConsumerGroupOffsetsTopic[];
}

export interface EditConsumerGroupOffsetsTopic {
    topicName: string;
    partitions: {
        partitionId: number;
        offset: number; // -1 latest, -2 earliest
    }[];
}

export interface EditConsumerGroupOffsetsResponse {
    error: string | undefined;
    topics: EditConsumerGroupOffsetsResponseTopic[]
}

export interface EditConsumerGroupOffsetsResponseTopic {
    topicName: string,
    partitions: {
        partitionID: number,
        error: string,
    }[],
}




export interface DeleteConsumerGroupOffsetsRequest {
    groupId: string;
    topics: DeleteConsumerGroupOffsetsTopic[];
}
export interface DeleteConsumerGroupOffsetsTopic {
    topicName: string;
    partitions: {
        partitionId: number;
    }[];
}

export interface DeleteConsumerGroupOffsetsResponse {
    topics: DeleteConsumerGroupOffsetsResponseTopic[]
}

export interface DeleteConsumerGroupOffsetsResponseTopic {
    topicName: string,
    partitions: {
        partitionID: number,
        error: string | undefined,
    }[],
}


export interface GetTopicOffsetsByTimestampRequest {
    topics: GetTopicOffsetsByTimestampRequestTopic[];
    timestamp: number; // unix ms
}
export interface GetTopicOffsetsByTimestampRequestTopic {
    topicName: string;
    partitionIds: number[];
}

export interface GetTopicOffsetsByTimestampResponse {
    topicOffsets: TopicOffset[];
}
export interface TopicOffset {
    topicName: string;
    partitions: PartitionOffset[];
}

export interface PartitionOffset {
    error: string | undefined;
    partitionId: number;

    // will return the first message after the given timestamp
    // if there is no message at or after this timestamp, the offset will be -1
    offset: number;

    // unix ms
    // if offset is not -1, this will tell us the timestamp of that message
    timestamp: number;
}




export interface TopicLag {
    topic: string; // name
    summedLag: number;

    partitionCount: number; // number of partitions the topic has
    partitionsWithOffset: number; // number of partitions that have an active offset in this group

    // only lists partitions that have a commited offset (independent of whether or not a member is currently assigned to it)
    partitionLags: PartitionLag[]
}

export interface PartitionLag {
    partitionId: number;
    offset: number;
    lag: number;
}

export interface GetConsumerGroupsResponse {
    consumerGroups: GroupDescription[];
}
export interface GetConsumerGroupResponse {
    consumerGroup: GroupDescription;
}



export interface ClusterInfoResponse {
    clusterInfo: ClusterInfo;
}
export interface ClusterInfo {
    controllerId: number;
    brokers: Broker[];
    kafkaVersion: string;
}
export interface Broker {
    brokerId: number;
    logDirSize: number; // bytes of the whole directory
    address: string;
    rack: string | null;

    config: BrokerConfig;
}
export interface BrokerConfig {
    configs: ConfigEntry[];
    error: string | undefined;
}



export interface EndpointCompatibilityResponse {
    endpointCompatibility: EndpointCompatibility;
}

export interface EndpointCompatibility {
    kafkaVersion: string;
    endpoints: EndpointCompatibilityEntry[];
}

export interface EndpointCompatibilityEntry {
    endpoint: string;
    method: string;
    isSupported: boolean;
}

// Response when requesting configuration of a single broker
export interface BrokerConfigResponse { // | ApiError
    brokerConfigs: ConfigEntry[];
}


// Current user
export interface User {
    id: string,
    internalIdentifier: string,
    providerID: number,
    providerName: string,
    meta: {
        email: string,
        name: string,
        avatarUrl: string,
    },
}
export interface Seat {
    id: string, // id of seat
    licenseId: string, // shouldn't that be censored??
    user: User, // user representation of firestore? should be removed...
    lastActivity: string, // is a datetime string, should probably be a "UnixMillis"
}
export interface UserData {
    user: User;
    seat: Seat;
    canManageKowl: boolean;
    canListAcls: boolean;
    canReassignPartitions: boolean;
    canPatchConfigs: boolean;
}
export type UserPermissions = Exclude<keyof UserData, 'user' | 'seat'>;



export interface AdminInfo {
    roles: Role[];
    roleBindings: RoleBinding[];
    users: UserDetails[];
}

export interface UserDetails {
    internalIdentifier: string;
    oauthUserId: string;
    loginProviderId: number;
    loginProvider: string;
    bindingIds: string[]; // rolebindings
    audits: {
        [roleName: string]: string[]; // roleName to (RoleBinding.ephemeralID)[]
    };

    // Added by frontend:
    bindings: RoleBinding[];
    grantedRoles: {
        role: Role;
        grantedBy: RoleBinding[];
    }[];
}

export interface PermissionAudit {
    roleName: string; // Role.name
    grantedBy: string; // RoleBinding.ephemeralId
}

export interface RoleBinding {
    ephemeralId: string;
    metadata: { [key: string]: string; };
    subjects: Subject[];
    roleName: string;

    // Added by frontend:
    resolvedRole: Role;
}

export interface Role {
    name: string;
    permissions: Permission[];
}

export interface Permission {
    resourceName: string;
    resourceId: number;

    // Those 3 may be missing or contain a single empty string.
    // The frontend fixes / normalizes those cases to '[]'.
    allowedActions: string[];
    includes: string[];
    excludes: string[];
}

export interface Subject {
    name: string;

    organization: string;

    subjectKind: number;
    subjectKindName: string;

    provider: number;
    providerName: string;
}


export interface TopicPermissions {
    canSeeTopic: boolean;
    canViewTopicPartitions: boolean;
    canSeeTopicConfig: boolean;
    canUseSearchFilters: boolean;
    canViewTopicMessages: boolean;
    canViewTopicConsumers: boolean;
}


//
// ACLs

// https://github.com/twmb/franz-go/blob/master/generate/definitions/enums#L47
export enum AclResourceType {
    AclResourceUnknown,
    AclResourceAny,
    AclResourceTopic,
    AclResourceGroup,
    AclResourceCluster,
    AclResourceTransactionalID,
    AclDelegationToken,
}

// https://github.com/twmb/franz-go/blob/master/generate/definitions/enums#L59
export enum AclResourcePatternTypeFilter {
    AclPatternUnknown,
    AclPatternAny,
    AclPatternMatch,
    AclPatternLiteral,
    AclPatternPrefixed
}

// https://github.com/twmb/franz-go/blob/master/generate/definitions/enums#L81
export enum AclOperation {
    AclOperationUnknown,
    AclOperationAny,
    AclOperationAll,
    AclOperationRead,
    AclOperationWrite,
    AclOperationCreate,
    AclOperationDelete,
    AclOperationAlter,
    AclOperationDescribe,
    AclOperationClusterAction,
    AclOperationDescribeConfigs,
    AclOperationAlterConfigs,
    AclOperationIdempotentWrite
}

// https://github.com/twmb/franz-go/blob/master/generate/definitions/enums#L71
export enum AclPermissionType {
    AclPermissionUnknown,
    AclPermissionAny,
    AclPermissionDeny,
    AclPermissionAllow
}

// list all:
//   /api/acls?resourceType=1&resourcePatternTypeFilter=1&operation=1&permissionType=1
export interface AclRequest {
    resourceType: AclResourceType;
    resourceName?: string;
    resourcePatternTypeFilter: AclResourcePatternTypeFilter;
    principal?: string;
    host?: string;
    operation: AclOperation;
    permissionType: AclPermissionType;
}

export const AclRequestDefault = {
    resourceType: AclResourceType.AclResourceAny,
    resourceName: "",
    resourcePatternTypeFilter: AclResourcePatternTypeFilter.AclPatternAny,
    principal: "",
    host: "",
    operation: AclOperation.AclOperationAny,
    permissionType: AclPermissionType.AclPermissionAny,
} as const;

export interface AclResponse {
    aclResources: AclResource[];
    isAuthorizerEnabled: boolean;
}

export enum ResourcePatternType {
    'UNKNOWN', 'MATCH', 'LITERAL', 'PREFIXED'
}
export interface AclResource {
    resourceType: string;
    resourceName: string;
    resourcePatternType: ResourcePatternType;
    acls: AclRule[];
}

export interface AclRule {
    principal: string;
    host: string;
    operation: string;
    permissionType: string;
}


export interface SchemaOverviewResponse {
    schemaOverview: SchemaOverview;
    isConfigured: boolean;
}

export interface SchemaOverview {
    mode: string;
    compatibilityLevel: string;
    subjects: string[];
    requestErrors: SchemaOverviewRequestError[];
}

export interface SchemaOverviewRequestError {
    requestDescription: string;
    errorMessage: string;
}

export interface SchemaDetailsResponse {
    schemaDetails: SchemaDetails
}

export enum SchemaType {
    AVRO="AVRO",
    JSON="JSON",
    PROTOBUF= "PROTOBUF",
}

export interface SchemaDetails {
    string: string;
    schemaId: number;
    version: number;
    compatibility: string;
    type: SchemaType;
    schema: Schema | JsonSchema;
    registeredVersions: number[];
}

export interface Schema {
    doc: string;
    name: string;
    namespace: string;
    type: string;
    fields: SchemaField[];
}

export enum JsonFieldType {
    STRING="string",
    NUMBER="number",
    INTEGER= "integer",
    OBJECT="object",
    ARRAY="array",
    BOOLEAN="boolean",
    NULL="null"
}

export interface JsonSchema {
  $id: string;
  type: JsonFieldType;
  title: string;
  description: string;
  properties?: Record<string, JsonField>;
}

export interface JsonField {
  type: string;
  items?: JsonField;
  properties?: Record<string, JsonField>;
}

export interface SchemaField {
    name: string;
    type: string | Record<string, unknown> | null | undefined;
    doc?: string | null | undefined;
    default?: string | Record<string, unknown> | null | undefined;
}


// Partition Reassignments - Get
export interface PartitionReassignmentsResponse {
    topics: PartitionReassignments[];
}
export interface PartitionReassignments {
    topicName: string;
    partitions: PartitionReassignmentsPartition[];
}
export interface PartitionReassignmentsPartition {
    partitionId: number;
    addingReplicas: number[];
    removingReplicas: number[];
    replicas: number[];
}

// PartitionReassignments - Patch
export interface PartitionReassignmentRequest {
    topics: TopicAssignment[];
}
export type TopicAssignment = {
    topicName: string; // name of topic to change
    partitions: { // partitions to reassign
        partitionId: number;
        replicas: number[] | null;
        // Entries are brokerIds.
        // Since the replicationFactor of a partition tells us the total number
        // of 'instances' of a partition (leader + follower replicas) the length of the array is always 'replicationFactor'.
        // The first entry in the array is the brokerId that will host the leader replica
        // Since Kafka rebalances the leader partitions across the brokers periodically, it is not super important which broker is the leader.
        //
        // Can also be null to cancel a pending reassignment.
    }[];
};

export interface AlterPartitionReassignmentsResponse {
    reassignPartitionsResponses: {
        topicName: string;
        partitions: AlterPartitionReassignmentsPartitionResponse[];
    }[];
}
export interface AlterPartitionReassignmentsPartitionResponse {
    partitionId: number;
    errorCode: string;
    errorMessage: string | null;
}


// Change broker config
// PATCH api/operations/configs
export enum ConfigResourceType {
    Unknown = 0,
    Topic = 2,
    Broker = 4,
    BrokerLogger = 8,
}

// export enum ConfigSource {
//     Unknown = 0,
//     DynamicTopicConfig = 1,
//     DynamicBrokerConfig = 2,
//     DynamicDefaultBrokerConfig = 3,
//     StaticBrokerConfig = 4,
//     DefaultConfig = 5,
//     DynamicBrokerLoggerConfig = 6,
// }

export enum AlterConfigOperation {
    Set = 0, // set a config key
    Delete = 1, // remove/unset a config key
    Append = 2, // add a value to a list
    Subtract = 3, // remove a value from a list
}

export interface IncrementalAlterConfigsRequestResourceConfig {
    // name of key to modify (e.g segment.bytes)
    name: string;

    // set(0) value must not be null
    // delete(1) delete a config key
    // append(2) append value to list of values, the config entry must be a list
    // subtract(3) remove an entry from a list of values
    op: AlterConfigOperation;

    // value to set the key to
    value?: string;
}

// Example
// To throttle replication rate for reassignments (bytes per second):
// - On the leader
//      --add-config 'leader.replication.throttled.rate=10000'
//      --entity-type broker
//      --entity-name brokerId
//
// - On the follower
//      --add-config 'follower.replication.throttled.rate=10000'
//      --entity-type broker
//      --entity-name brokerId

export interface ResourceConfig {
    // ResourceType is an enum that represents TOPIC, BROKER or BROKER_LOGGER
    resourceType: ConfigResourceType;

    // ResourceName is the name of config to alter.
    //
    // If the requested type is a topic, this corresponds to a topic name.
    //
    // If the requested type if a broker, this should either be empty or be
    // the ID of the broker this request is issued to. If it is empty, this
    // updates all broker configs. If a specific ID, this updates just the
    // broker. Using a specific ID also ensures that brokers reload config
    // or secret files even if the file path has not changed. Lastly, password
    // config options can only be defined on a per broker basis.
    //
    // If the type is broker logger, this must be a broker ID.
    resourceName: string

    // key/value config pairs to set on the resource.
    configs: IncrementalAlterConfigsRequestResourceConfig[];
}
export interface PatchConfigsRequest {
    resources: ResourceConfig[];
}

export interface PatchConfigsResponse {
    patchedConfigs: {
        error: string | null;
        resourceName: string;
        resourceType: ConfigResourceType;
    }[];
}