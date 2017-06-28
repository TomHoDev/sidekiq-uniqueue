require "sidekiq/api"
module Sidekiq

  class Queue

    def clear
      Sidekiq.redis do |conn|
        conn.evalsha del_queue_eval_sha(conn), [@rname]
        conn.srem("queues".freeze, name)
      end
    end

    def del_queue_eval_sha(conn)
      @pop_unique_eval_sha  ||= conn.script :load, <<-LUA
        local queue_name = KEYS[1]
        local uniqueue_set_name = queue_name..':uniqueue_set'
        local uniqueue_list_name = queue_name..':uniqueue_list'

        redis.call('del', queue_name)
        redis.call('del', uniqueue_set_name)
        redis.call('del', uniqueue_list_name)

      LUA
    end
  end

  class Job
    def delete
      #noop - TODO: we can get this working later
    end
  end

end