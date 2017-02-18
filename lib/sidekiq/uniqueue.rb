require "sidekiq"
require "sidekiq/uniqueue/version"
require "sidekiq/uniqueue/client"
require "sidekiq/uniqueue/fetch"
require "sidekiq/uniqueue/api"

module Sidekiq
  module Uniqueue

    def self.uniqueue_payload(payload)
      Array(payload['class']) + Array(payload['args'])
    end

    def self.queue_and_set_length_equal_eval_sha(conn)
      @queue_and_set_length_equal_eval_sha ||= conn.script :load, <<-LUA
        local queue_name = KEYS[1]
        local uniqueue_set_name = queue_name..':uniqueue_set'
        local uniqueue_list_name = queue_name..':uniqueue_list'

        local queue_size = redis.call('llen', queue_name)
        local uniqueue_set_size = redis.call('scard', uniqueue_set_name)
        local uniqueue_list_size = redis.call('llen', uniqueue_list_name)

        if queue_size == uniqueue_set_size then
          if queue_size == uniqueue_list_size then
            return true
          end
        end
        return false
      LUA
    end

    #if the queue and set sizes differ, something is very wrong and we should fail loudly
    def self.confirm_unique_queue_validity(conn, queue)
      response = conn.evalsha queue_and_set_length_equal_eval_sha(conn), [queue]
      return true if response == 1
      #TODO raise specific exception
      raise "Make sure your queues are empty before you start using uniqueue: #{queue}"
      exit(1)
    end

    def self.confirm_validaty_of_all_queues
      Sidekiq.redis do |conn|
        Array(Sidekiq.options[:queues]).each do |queue|
          queue_name = "queue:#{queue}"
          confirm_unique_queue_validity(conn, queue_name)
        end
      end
    end


  end
end

Sidekiq.options[:fetch] = Sidekiq::Uniqueue::Fetch