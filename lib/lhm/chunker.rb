# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/sql_helper'

module Lhm
  class Chunker
    include Command
    include SqlHelper

    attr_reader :connection

    # Copy from origin to destination in chunks of size `stride`. Sleeps for
    # `throttle` milliseconds between each stride.
    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @throttler = options[:throttler]
      @start = options[:start] || select_start
      @limit = options[:limit] || select_limit
      @autoincrementing = options[:autoincrementing].nil? ? true : options[:autoincrementing]
    end

    # Copies chunks of size `stride`, starting from `start` up to id `limit`.
    def up_to(&block)
      return if @start.nil? || @limit.nil? # no records in table

      if @autoincrementing
        1.upto(traversable_chunks_size) do |n|
          yield(bottom(n), top(n))
        end
      else
        lowest = @start

        while lowest < @limit
          highest = select_next_highest_id(lowest) or break
          yield(lowest, highest)
          lowest = highest + 1
        end
      end
    end

    def traversable_chunks_size
      @limit && @start ? ((@limit - @start + 1) / @throttler.stride.to_f).ceil : 0
    end

    def bottom(chunk)
      (chunk - 1) * @throttler.stride + @start
    end

    def top(chunk)
      [chunk * @throttler.stride + @start - 1, @limit].min
    end

    def copy(lowest, highest)
      "insert ignore into `#{ destination_name }` (#{ columns }) " +
      "select #{ select_columns } from `#{ origin_name }` " +
      "#{ conditions } #{ origin_name }.`id` between #{ lowest } and #{ highest }"
    end

    def select_start
      start = connection.select_value("select min(id) from #{ origin_name }")
      start ? start.to_i : nil
    end

    def select_limit
      limit = connection.select_value("select max(id) from #{ origin_name }")
      limit ? limit.to_i : nil
    end

    def select_next_highest_id(lowest)
      next_max_id = connection.select_value(<<-SQL.chomp
        select max(id) from (
          select id from #{ origin_name }
          where id > #{ lowest }
          order by id
          limit #{ @throttler.stride }
        ) as ids
        SQL
      )
      next_max_id ? next_max_id.to_i : nil
    end

  private

    def conditions
      @migration.conditions ? "#{@migration.conditions} and" : "where"
    end

    def destination_name
      @migration.destination.name
    end

    def origin_name
      @migration.origin.name
    end

    def columns
      @columns ||= @migration.intersection.joined
    end

    def select_columns
      @select_columns ||= @migration.intersection.typed(origin_name)
    end

    def validate
      if @start && @limit && @start > @limit
        error("impossible chunk options (limit must be greater than start)")
      end
    end

    def execute
      up_to do |lowest, highest|
        affected_rows = @connection.update(copy(lowest, highest))

        if @throttler && affected_rows > 0
          @throttler.run
        end

        print "."
      end
      print "\n"
    end
  end
end
