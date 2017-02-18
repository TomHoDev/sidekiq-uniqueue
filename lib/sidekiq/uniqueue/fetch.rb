module Sidekiq
  module Uniqueue
    class Fetch

      def initialize(options)
        @strictly_ordered_queues = !!options[:strict]
        @queues = options[:queues].map { |q| "queue:#{q}" }
        @unique_queues = @queues.uniq
      end

      #this is not ideal, but we will need to upgrade our redis server
      #to a version that supports lua 'unpack' before we can us brpop
      #redis.call('brpop', unpack(KEYS)) is where we want to end up
      def retrieve_work
        Sidekiq.redis do |conn|
          queues_cmd.each do |queue|
            Sidekiq::Uniqueue.confirm_unique_queue_validity(conn, queue)
            message = conn.evalsha pop_unique_eval_sha(conn), [queue]
            return UnitOfWork.new(queue, message) if message
          end
          sleep(5)
        end
      end

      def pop_unique_eval_sha(conn)
        @pop_unique_eval_sha  ||= conn.script :load, <<-LUA
          local queue_name = KEYS[1]
          local uniqueue_set_name = queue_name..':uniqueue_set'
          local uniqueue_list_name = queue_name..':uniqueue_list'
          local results = redis.call('rpop', queue_name)
          local unique_payload = redis.call('rpop', uniqueue_list_name)
          if unique_payload then
            redis.call('srem', uniqueue_set_name, unique_payload)
          end
          return results
        LUA
      end

      # By leaving this as a class method, it can be pluggable and used by the Manager actor. Making it
      # an instance method will make it async to the Fetcher actor
      def self.bulk_requeue(inprogress, options)
        return if inprogress.empty?

        Sidekiq.logger.debug { "Re-queueing terminated jobs" }

        #it puts them on the end of the queue instead of the beginning
        #TODO:  write dedicate lua script to put things on the front of the line
        inprogress.each do |unit_of_work|
          unit_of_work.requeue
        end
        Sidekiq.logger.info("Pushed #{inprogress.size} messages back to Redis")
      rescue => ex
        Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{ex.message}")
      end

      UnitOfWork = Struct.new(:queue, :message) do
        def acknowledge
          # nothing to do
        end

        def queue_name
          queue.gsub(/.*queue:/, '')
        end

        def payload
          @payload ||= Sidekiq.load_json(message)
        end

        def requeue
          Sidekiq::Client.default.send :raw_push, Array(payload)
        end

      end

      # Creating the Redis#brpop command takes into account any
      # configured queue weights. By default Redis#brpop returns
      # data from the first queue that has pending elements. We
      # recreate the queue command each time we invoke Redis#brpop
      # to honor weights and avoid queue starvation.
      def queues_cmd
        queues = @strictly_ordered_queues ? @unique_queues.dup : @queues.shuffle.uniq
        #queues << Sidekiq::Fetcher::TIMEOUT
      end

    end
  end
end
