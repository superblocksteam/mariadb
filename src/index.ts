import {
  Column,
  DatasourceMetadataDto,
  DBActionConfiguration,
  MariaDBDatasourceConfiguration,
  ExecutionOutput,
  IntegrationError,
  RawRequest,
  Table,
  TableType,
  ResolvedActionConfigurationProperty
} from '@superblocksteam/shared';
import {
  ActionConfigurationResolutionContext,
  DatabasePlugin,
  normalizeTableColumnNames,
  PluginExecutionProps,
  resolveActionConfigurationPropertyUtil,
  CreateConnection,
  DestroyConnection
} from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';
import { Connection, createConnection } from 'mariadb';

const TEST_CONNECTION_TIMEOUT = 5000;

export default class MariaDBPlugin extends DatabasePlugin {
  pluginName = 'MariaDB';

  async resolveActionConfigurationProperty({
    context,
    actionConfiguration,
    files,
    property,
    escapeStrings
  }: // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ActionConfigurationResolutionContext): Promise<ResolvedActionConfigurationProperty> {
    return this.tracer.startActiveSpan(
      'plugin.resolveActionConfigurationProperty',
      { attributes: this.getTraceTags(), kind: 1 /* SpanKind.SERVER */ },
      async (span) => {
        const resolvedActionConfigurationProperty = resolveActionConfigurationPropertyUtil(
          super.resolveActionConfigurationProperty,
          {
            context,
            actionConfiguration,
            files,
            property,
            escapeStrings
          },
          false /* useOrderedParameters */
        );
        span.end();
        return resolvedActionConfigurationProperty;
      }
    );
  }

  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<MariaDBDatasourceConfiguration>): Promise<ExecutionOutput> {
    const connection = await this.createConnection(datasourceConfiguration);
    try {
      const query = actionConfiguration.body;
      const ret = new ExecutionOutput();
      if (!query || isEmpty(query)) {
        return ret;
      }
      const rows = (await this.executeQuery(() => {
        return connection.query(query, context.preparedStatementContext);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      })) as Record<string, any>;
      ret.output = normalizeTableColumnNames(rows);
      return ret;
    } catch (err) {
      throw new IntegrationError(`${this.pluginName} query failed, ${err.message}`);
    } finally {
      if (connection) {
        this.destroyConnection(connection);
      }
    }
  }

  getRequest(actionConfiguration: DBActionConfiguration): RawRequest {
    return actionConfiguration?.body;
  }

  dynamicProperties(): string[] {
    return ['body'];
  }

  async metadata(datasourceConfiguration: MariaDBDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    const connection = await this.createConnection(datasourceConfiguration);
    try {
      const tableQuery =
        'select COLUMN_NAME as name,' +
        '       TABLE_NAME as table_name,' +
        '       COLUMN_TYPE as column_type' +
        ' from information_schema.columns' +
        ' where table_schema = database()' +
        ' order by table_name, ordinal_position';

      const tableResult = await this.executeQuery(() => {
        return connection.query(tableQuery);
      });

      const entities = tableResult.reduce((acc, attribute) => {
        const entityName = attribute.table_name;
        const entityType = TableType.TABLE;

        const entity = acc.find((o) => o.name === entityName);
        if (entity) {
          const columns = entity.columns;
          entity.columns = [...columns, new Column(attribute.name, attribute.column_type)];
          return [...acc];
        }

        const table = new Table(entityName, entityType);
        table.columns.push(new Column(attribute.name, attribute.column_type));

        return [...acc, table];
      }, []);
      return {
        dbSchema: { tables: entities }
      };
    } catch (err) {
      throw new IntegrationError(`Failed to connect to ${this.pluginName}, ${err.message}`);
    } finally {
      if (connection) {
        this.destroyConnection(connection);
      }
    }
  }

  @DestroyConnection
  private async destroyConnection(connection: Connection): Promise<void> {
    await connection.end();
  }

  @CreateConnection
  private async createConnection(
    datasourceConfiguration: MariaDBDatasourceConfiguration,
    connectionTimeoutMillis = 30000
  ): Promise<Connection> {
    if (!datasourceConfiguration) {
      throw new IntegrationError(`Datasource not found for ${this.pluginName} step`);
    }
    try {
      const endpoint = datasourceConfiguration.endpoint;
      const auth = datasourceConfiguration.authentication;
      if (!endpoint) {
        throw new IntegrationError(`Endpoint not specified for ${this.pluginName} step`);
      }
      if (!auth) {
        throw new IntegrationError(`Authentication not specified for ${this.pluginName} step`);
      }
      if (!auth.custom?.databaseName?.value) {
        throw new IntegrationError(`Database not specified for ${this.pluginName} step`);
      }
      const connection = await createConnection({
        host: endpoint.host,
        user: auth.username,
        password: auth.password,
        database: auth.custom.databaseName.value,
        port: endpoint.port,
        ssl: datasourceConfiguration.connection?.useSsl ? { rejectUnauthorized: false } : false,
        connectTimeout: connectionTimeoutMillis,
        allowPublicKeyRetrieval: !(datasourceConfiguration.connection?.useSsl ?? false)
      });
      this.attachLoggerToConnection(connection, datasourceConfiguration);

      this.logger.debug(
        `${this.pluginName} connection created. ${datasourceConfiguration.endpoint?.host}:${datasourceConfiguration.endpoint?.port}`
      );
      return connection;
    } catch (err) {
      throw new IntegrationError(`Failed to connect to ${this.pluginName}, ${err.message}`);
    }
  }

  private attachLoggerToConnection(connection: Connection, datasourceConfiguration: MariaDBDatasourceConfiguration) {
    if (!datasourceConfiguration.endpoint) {
      return;
    }

    const datasourceEndpoint = `${datasourceConfiguration.endpoint?.host}:${datasourceConfiguration.endpoint?.port}`;

    connection.on('error', (err: Error) => {
      this.logger.debug(`${this.pluginName} connection error. ${datasourceEndpoint}`, err.stack);
    });

    connection.on('end', () => {
      this.logger.debug(`${this.pluginName} connection ended. ${datasourceEndpoint}`);
    });
  }

  async test(datasourceConfiguration: MariaDBDatasourceConfiguration): Promise<void> {
    const connection = await this.createConnection(datasourceConfiguration, TEST_CONNECTION_TIMEOUT);
    try {
      await this.executeQuery(() => {
        return connection.query('SELECT NOW()');
      });
    } catch (err) {
      throw new IntegrationError(`Test ${this.pluginName} connection failed, ${err.message}`);
    } finally {
      if (connection) {
        this.destroyConnection(connection);
      }
    }
  }
}
