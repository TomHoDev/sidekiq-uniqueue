module Sidekiq
  class Client

    def raw_push_with_unique(payloads)
      @redis_pool.with do |conn|
        #no multi since we are using exec
        atomic_push(conn, payloads)
      end
      true
    end

    alias_method :raw_push_without_unique, :raw_push
    alias_method :raw_push, :raw_push_with_unique

    def atomic_push_with_unique(conn, payloads)

      if payloads.first['at']
        conn.zadd('schedule'.freeze, payloads.map do |hash|
          at = hash.delete('at'.freeze).to_s
          [at, Sidekiq.dump_json(hash)]
        end)
      else
        q = payloads.first['queue']
        conn.sadd('queues'.freeze, q)
        payloads.each do |payload|
          push_unique_payload(conn, payload, "queue:#{q}")
        end
      end
    end


    alias_method :atomic_push_without_unique, :atomic_push
    alias_method :atomic_push, :atomic_push_with_unique

    #for now we will enqueue discretely, we could look into having lua handle multipayloads later
    def push_unique_payload(conn, payload, queue)
      uniqueue_payload = Sidekiq::Uniqueue.uniqueue_payload(payload)
      serialized_work = Sidekiq.dump_json(payload)
      serialized_unique = Sidekiq.dump_json(uniqueue_payload)
      conn.evalsha push_unique_eval_sha(conn), [queue], [serialized_work, serialized_unique]
    end

    def push_unique_eval_sha(conn)
      @push_unique_eval_sha ||= conn.script :load, <<-LUA
        local queue_name = KEYS[1]
        local uniqueue_set_name = queue_name..':uniqueue_set'
        local uniqueue_list_name = queue_name..':uniqueue_list'
        local not_in_set = redis.call('sadd', uniqueue_set_name , ARGV[2])
        if not_in_set == 1 then
          redis.call('lpush', uniqueue_list_name, ARGV[2])
          return redis.call('lpush', queue_name, ARGV[1])
        end
        return false
      LUA
    end

  end
end