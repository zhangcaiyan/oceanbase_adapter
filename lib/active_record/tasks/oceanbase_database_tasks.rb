# frozen_string_literal: true

require 'active_record/tasks/mysql_database_tasks'
module ActiveRecord
  module Tasks
    class OceanbaseDatabaseTasks < MySQLDatabaseTasks
    end
  end
end

