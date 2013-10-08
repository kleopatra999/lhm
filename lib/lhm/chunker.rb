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
      @next_row = options[:start] || select_start
      @limit = options[:limit] || select_limit
    end

    # Copies chunks of size `stride`, starting from `start` up to id `limit`.
    def up_to(&block)
      while @next_row < @limit
        yield(@next_row, top)
        @next_row = top + 1
      end
    end

    def top(chunk)
      @next_row + @throttler.stride
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
      if @next_row && @limit && @next_row > @limit
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
