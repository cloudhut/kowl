import { Col, Descriptions, Row, Select, Statistic, Table, Tag } from 'antd';
import Card from '../../misc/Card';
import { observer } from 'mobx-react';
import React from 'react';
import { appGlobal } from '../../../state/appGlobal';
import { api } from '../../../state/backendApi';
import { PageComponent, PageInitHelper } from '../Page';
import { DefaultSkeleton, Label, OptionGroup, QuickTable, toSafeString } from '../../../utils/tsxUtils';
import { motion } from 'framer-motion';
import { animProps } from '../../../utils/animationProps';
import { KowlJsonView } from '../../misc/KowlJsonView';
import { sortField } from '../../misc/common';
import { JsonField, JsonFieldType, JsonSchema, Schema, SchemaField, SchemaType } from '../../../state/restInterfaces';
import { uiSettings } from '../../../state/ui';
import { title } from 'process';

export interface SchemaDetailsProps {
    subjectName: string;
    query: {
        version: number;
    };
}

function renderSchemaType(value: any, record: SchemaField, index: number) {
    return toSafeString(value);
}

function convertJsonField(name: string, field: JsonField): SchemaField {

    switch (field.type) {
        case JsonFieldType.ARRAY: {
            let items = undefined;
            if ( field.items){
              const jsonField = convertJsonField(name, field.items);
              items = jsonField.type;
            }

            return {
              name,
              type: {
                type: field.type,
                items,
              }
            };
        }
        case JsonFieldType.OBJECT: {
            let fields: Record<string, unknown> | undefined = undefined;
            if ( field.properties) {
                fields = {};
                for ( const name in field.properties) {
                    const jsonField = convertJsonField(name, field.properties[name]);
                    fields[name] = jsonField.type; 
                }
            }
            return {
                name,
                type: {
                    type: field.type,
                    properties: field.properties
                }
            };
        }
        case undefined: {
          if ( field.properties ) {
              return convertJsonField(name, { ...field, type: JsonFieldType.OBJECT }); 
          }
          if ( field.items) {
              return convertJsonField(name, { ...field, type: JsonFieldType.ARRAY });  
          }
          break;
        }
        default: {
           break;
        }
    }
    return {
        name,
        type: field.type,
    };
}

@observer
class SchemaDetailsView extends PageComponent<SchemaDetailsProps> {
    initPage(p: PageInitHelper): void {
        const subjectName = this.props.subjectName;
        const version = this.props.query.version;

        p.title = subjectName;
        p.addBreadcrumb('Schema Registry', '/schema-registry');
        p.addBreadcrumb(subjectName, `/schema-registry/${subjectName}?version=${version}`);
        this.refreshData(false);
        appGlobal.onRefresh = () => this.refreshData(true);
    }

    refreshData(force?: boolean) {
        const version: number | 'latest' = this.props.query.version ?? 'latest';
        api.refreshSchemaDetails(this.props.subjectName, version, force);
    }

    componentDidUpdate({ query: { version } }: SchemaDetailsProps) {
        if (this.props.query.version !== version) {
            this.refreshData(true);
        }
    }

    render() {
        if (!api.schemaDetails) return DefaultSkeleton;

        const {
            schemaId,
            version,
            compatibility,
            type: schemaType,
            schema
        } = api.schemaDetails;
        let { type, name, namespace, doc, fields }  = schema as Schema;
        if (  schemaType === SchemaType.JSON ) {
            const jsonSchema = schema as JsonSchema;
            ({type, title: name, description: doc, $id: namespace} = jsonSchema);
            fields = [];
            if ( jsonSchema.properties ) {
                for (const p in jsonSchema.properties) {
                    const property = jsonSchema.properties[p];
                    fields.push ( convertJsonField(p, property) );
                }
            }
        }

        const versions = api.schemaDetails?.registeredVersions ?? [];

        const defaultVersion = this.props.query.version ?? (versions.length > 0 ? versions[versions.length - 1] : 'latest');

        return (
            <motion.div {...animProps} key={'b'} style={{ margin: '0 1rem' }}>
                <Card>
                    <Row>
                        <Statistic title="Subject" value={this.props.subjectName}></Statistic>
                        <Statistic title="Schema ID" value={schemaId}></Statistic>
                        <Statistic title="Version" value={version}></Statistic>
                        <Statistic title="Compatibility" value={compatibility}></Statistic>
                    </Row>
                </Card>
                <Card>
                    <div style={{ display: 'flex', alignItems: 'flex-start', columnGap: '1.5em', marginBottom: '1em' }}>
                        <Label text="Version">
                            <Select style={{ minWidth: '200px' }}
                                defaultValue={defaultVersion}
                                onChange={(version) => appGlobal.history.push(`/schema-registry/${this.props.subjectName}?version=${version}`)}
                                disabled={versions.length == 0}
                            >
                                {versions.map(v => <Select.Option value={v} key={v}>Version {v} {v == versions[versions.length - 1] ? '(latest)' : null}</Select.Option>)}
                            </Select>
                        </Label>

                        <Label text='Details' style={{ alignSelf: 'stretch' }}>
                            <div style={{ display: 'inline-flex', flexWrap: 'wrap', minHeight: '32px', alignItems: 'center', rowGap: '.3em' }}>
                                {Object.entries({
                                    "Type": type,
                                    "Name": name,
                                    "Namespace": namespace,
                                }).map(([k, v]) => {
                                    if (!k || v === undefined || v === null) return null;
                                    return <Tag color='blue' key={k}><span style={{ color: '#2d5b86' }}>{k}:</span> {toSafeString(v)}</Tag>
                                })}
                                {!!doc && <a href={doc}>
                                    <Tag color='blue' style={{ cursor: 'pointer' }}><span style={{ color: '#2d5b86' }}>Documentation:</span> <a style={{ textDecoration: 'underline' }} href={doc}>{doc}</a></Tag>
                                </a>}
                            </div>

                        </Label>
                    </div>

                    <div style={{ marginBottom: '1.5em' }}>
                        <OptionGroup label=''
                            options={{
                                "Show Fields": 'fields',
                                "Show raw JSON": 'json',
                            }}
                            value={uiSettings.schemaDetails.viewMode}
                            onChange={s => uiSettings.schemaDetails.viewMode = s}
                        />
                    </div>

                    <div>
                        {uiSettings.schemaDetails.viewMode == 'json' &&
                            <KowlJsonView
                                shouldCollapse={false}
                                collapsed={false}
                                src={api.schemaDetails || {}}
                                style={{
                                    border: 'solid thin lightgray',
                                    borderRadius: '.25em',
                                    padding: '1em 1em 1em 2em',
                                    marginBottom: '1.5rem',
                                }}
                            />
                        }

                        {uiSettings.schemaDetails.viewMode == 'fields' &&
                            <Table
                                size="small"
                                columns={[
                                    { title: 'Name', dataIndex: 'name', className: 'whiteSpaceDefault', }, // sorter: sortField('name')
                                    { title: 'Type', dataIndex: 'type', className: 'whiteSpaceDefault', render: renderSchemaType }, //  sorter: sortField('type'),
                                    { title: 'Default', dataIndex: 'default', className: 'whiteSpaceDefault' },
                                    { title: 'Documentation', dataIndex: 'doc', className: 'whiteSpaceDefault' },
                                ]}
                                rowKey="name"
                                dataSource={fields}
                                pagination={false}
                                style={{
                                    maxWidth: '100%',
                                    marginTop: '1.5rem',
                                    marginBottom: '1.5rem',
                                }}
                            />
                        }

                    </div>
                </Card>
            </motion.div>
        );
    }
}

export default SchemaDetailsView;

