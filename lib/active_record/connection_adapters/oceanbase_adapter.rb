# frozen_string_literal: true

require 'active_record/connection_adapters/abstract_mysql_adapter'
require 'active_record/connection_adapters/mysql/database_statements'

require 'active_record/tasks/mysql_database_tasks'

module ActiveRecord
  class Base # :nodoc:
    class << self
      # Establishes a connection to the database that's used by all Active Record objects.
      def oceanbase_connection(config)
        config         = config.symbolize_keys
        config[:flags] ||= 0

        if config[:flags].kind_of? Array
          config[:flags].push 'FOUND_ROWS'
        else
          config[:flags] |= Mysql2::Client::FOUND_ROWS
        end

        client = Mysql2::Client.new(config)
        ConnectionAdapters::OceanbaseAdapter.new(client, logger, nil, config)
      rescue OceanbaseAdapter::Error => error
        if error.message.include?('Unknown database')
          raise ActiveRecord::NoDatabaseError
        else
          raise
        end
      end
    end
  end

  module ConnectionAdapters
    # Oceanbase 驱动
    class OceanbaseAdapter < AbstractMysqlAdapter
      ADAPTER_NAME = 'Oceanbase'

      include MySQL::DatabaseStatements

      def initialize(connection, logger, connection_options, config)
        super
        @prepared_statements = false unless config.key?(:prepared_statements)
        configure_connection
      end

      def supports_json?
        !mariadb? && version >= '5.7.8'
      end

      def supports_comments?
        true
      end

      def supports_comments_in_create?
        true
      end

      def supports_savepoints?
        true
      end

      # Oceanbase 不支持 GET_LOCK/RELEASE_LOCK 函数
      def get_advisory_lock(lock_name, timeout = 0)
        true
      end

      def release_advisory_lock(lock_name)
        true
      end

      # HELPER METHODS ===========================================

      def each_hash(result)
        # :nodoc:
        if block_given?
          result.each(as: :hash, symbolize_keys: true) do |row|
            yield row
          end
        else
          to_enum(:each_hash, result)
        end
      end

      def error_number(exception)
        exception.error_number if exception.respond_to?(:error_number)
      end

      def new_column_from_field(table_name, field)
        type_metadata     = fetch_type_metadata(field[:Type], field[:Extra])

        if type_metadata.type == :datetime && /\ACURRENT_TIMESTAMP(?:\(\))?\z/i.match?(field[:Default])
          default          = nil
          default_function = 'CURRENT_TIMESTAMP'
        else
          default          = field[:Default]
          default_function = nil
        end

        # oceanbase 在获取字段类型时，Default/Collation 会是 "NULL"
        default           = nil if default == 'NULL'
        field[:Collation] = nil if field[:Collation] == 'NULL'

        new_column(field[:Field], default, type_metadata, field[:Null] == 'YES',
                   table_name, default_function, field[:Collation], comment: field[:Comment].presence)
      end

      #--
      # QUOTING ==================================================
      #++

      def quote_string(string)
        @connection.escape(string)
      end

      #--
      # CONNECTION MANAGEMENT ====================================
      #++

      def active?
        @connection.ping
      end

      def reconnect!
        super
        disconnect!
        connect
      end

      alias reset! reconnect!

      # Disconnects from the database if already connected.
      # Otherwise, this method does nothing.
      def disconnect!
        super
        @connection.close
      end

      private

      def connect
        @connection = Mysql2::Client.new(@config)
        configure_connection
      end

      def configure_connection
        @connection.query_options[:as] = :array

        variables = @config.fetch(:variables, {}).stringify_keys

        # By default, MySQL 'where id is null' selects the last inserted id; Turn this off.
        variables['sql_auto_is_null'] = 0

        # Increase timeout so the server doesn't disconnect us.
        wait_timeout              = self.class.type_cast_config_to_integer(@config[:wait_timeout])
        wait_timeout              = 2147483 unless wait_timeout.is_a?(Integer)
        variables['wait_timeout'] = wait_timeout

        defaults            = [':default', :default].to_set

        # Make MySQL reject illegal values rather than truncating or blanking them, see
        # http://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_strict_all_tables
        # If the user has provided another value for sql_mode, don't replace it.
        if sql_mode = variables.delete('sql_mode')
          sql_mode = quote(sql_mode)
        elsif !defaults.include?(strict_mode?)
          if strict_mode?
            sql_mode = "CONCAT(@@sql_mode, ',STRICT_ALL_TABLES')"
          else
            sql_mode = "REPLACE(@@sql_mode, 'STRICT_TRANS_TABLES', '')"
            sql_mode = "REPLACE(#{sql_mode}, 'STRICT_ALL_TABLES', '')"
            sql_mode = "REPLACE(#{sql_mode}, 'TRADITIONAL', '')"
          end
          sql_mode = "CONCAT(#{sql_mode}, ',NO_AUTO_VALUE_ON_ZERO')"
        end
        if sql_mode
          sql_mode_assignment = "@@SESSION.sql_mode = #{sql_mode} "
          execute "SET #{sql_mode_assignment}"
        end

        # NAMES does not have an equals sign, see
        # http://dev.mysql.com/doc/refman/5.7/en/set-statement.html#id944430
        # (trailing comma because variable_assignments will always have content)
        if @config[:encoding]
          encoding = "NAMES #{@config[:encoding]}"
          encoding << " COLLATE #{@config[:collation]}" if @config[:collation]
          execute "SET #{encoding}"
        end

        # Gather up all of the SET variables...
        variable_assignments = variables.map do |k, v|
          if defaults.include?(v)
            "@@SESSION.#{k} = DEFAULT" # Sets the value to the global or compile default
          elsif !v.nil?
            "@@SESSION.#{k} = #{quote(v)}"
          end
          # or else nil; compact to clear nils out
        end.compact

        # OceanBase 不支持 set 同时设置多变量
        variable_assignments.each do |assignment|
          execute "SET #{assignment}"
        end
      end

      def full_version
        @full_version ||= @connection.server_info[:version]
      end
    end
  end
end
