module ActiveRecord
  module ConnectionAdapters
    class RefreshConnectionManagement
      DEFAULT_OPTIONS = {max_requests: 1, schema_cache_target: ActiveRecord::Base}

      def initialize(app, options = {})
        @app = app
        @options = DEFAULT_OPTIONS.merge(options)
        @mutex = Mutex.new

        reset_remain_count
      end

      def call(env)
        testing = env.key?('rack.test')

        if @prev_schema_cache
          schema_cache_target.connection.schema_cache = @prev_schema_cache
        end
        response = @app.call(env)

        response[2] = ::Rack::BodyProxy.new(response[2]) do
          # disconnect all connections on the connection pool
          clear_connections unless testing
        end

        response
      rescue Exception
        clear_connections unless testing
        raise
      end

      private

      def preserve_schema_cache
        @prev_schema_cache = schema_cache_target.connection.schema_cache
        @prev_schema_cache.connection = nil
      end

      def clear_connections
        preserve_schema_cache
        if ActiveRecord::VERSION::MAJOR >= 6
          if should_clear_all_connections?
            ActiveRecord::Base.connection_handlers.each_value do |connection_handler|
              connection_handler.clear_all_connections!
            end
          else
            ActiveRecord::Base.connection_handlers.each_value do |connection_handler|
              connection_handler.clear_active_connections!
            end
          end
        else
          if should_clear_all_connections?
            ActiveRecord::Base.clear_all_connections!
          else
            ActiveRecord::Base.clear_active_connections!
          end
        end
      end

      def should_clear_all_connections?
        return true if max_requests <= 1

        @mutex.synchronize do
          @remain_count -= 1
          (@remain_count <= 0).tap do |clear|
            reset_remain_count if clear
          end
        end
      end

      def reset_remain_count
        @remain_count = max_requests
      end

      def max_requests
        @options[:max_requests]
      end

      def schema_cache_target
        unless @schema_cache_target
          @schema_cache_target = @options[:schema_cache_target]
          if @schema_cache_target.is_a?(String)
            @schema_cache_target = Kernel.const_get(@schema_cache_target)
          end
        end
        @schema_cache_target
      end
    end
  end
end
