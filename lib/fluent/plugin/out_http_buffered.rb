# encoding: utf-8

require "fluent/plugin/output"

module Fluent::Plugin
  # Main Output plugin class
  class HttpBufferedOutput < Output
    Fluent::Plugin.register_output('http_buffered', self)

    # Endpoint URL ex. localhost.local/api/
    config_param :endpoint_url, :string
    # statuses under which to retry
    config_param :http_retry_statuses, :string, default: ''
    # read timeout for the http call
    config_param :http_read_timeout, :float, default: 60.0
    # open timeout for the http call
    config_param :http_open_timeout, :float, default: nil
    # keep_alive_timeout for the http call
    config_param :http_keep_alive_timeout, :float, default: 2.0
    # content-type header for the http call
    config_param :http_content_type, :string, default: 'application/xml'
    # number of connections to try and keep alive in a pool
    config_param :http_pool_size, :integer, default: 5
    # to perform validation on entries before sending them
    config_param :record_contains, :string, default: ''

    def initialize
      super
      require 'net/http'
      require 'connection_pool'
    end

    def configure(conf)
      super

      date = Time.now
      @formatted_url = date.strftime(@endpoint_url)

      # Check if the URL is valid
      unless @formatted_url =~ /^#{URI.regexp}$/
        fail Fluent::ConfigError, 'url invalid'
      end
      # Create the URI object from the URL
      begin
        @uri = URI.parse(@formatted_url)
      rescue URI::InvalidURIError
        raise Fluent::ConfigError, 'url invalid'
      end

      @http_pool = ConnectionPool.new(size: @http_pool_size, timeout: @http_open_timeout + @http_read_timeout) {
        $log.debug "HTTPBufferedOut: Creating new HTTP object for the pool"
        # Create a new HTTP connection object using the uri
        http = Net::HTTP.new(@uri.host, @uri.port)
        http.read_timeout = @http_read_timeout
        http.open_timeout = @http_open_timeout
        http.keep_alive_timeout = @http_keep_alive_timeout

        http
      }

      # Parse http statuses
      @statuses = @http_retry_statuses.split(',').map { |status| status.to_i }

      @statuses = [] if @statuses.nil?

      # Parse the record contains
      @contains = @record_contains.split(',').map { |match| match.strip }

      @contains = [] if @contains.nil?
    end

    def start
      super
    end

    def shutdown
      super
      @http_pool.shutdown { |http|
        begin
          $log.debug "HTTPBufferedOut: Closing HTTP connection"
          http.finish
        rescue Exception => e
          $log.error "HTTPBufferedOut: Error occured closing HTTP connection"
          $log.debug "HTTPBufferedOut: #{e.class} => #{e.message}"
          $log.trace "HTTPBufferedOut: #{e.backtrace.join("\n\t")}"
        end
      }
    end

    def write(chunk)
      data = []
      chunk.each do |time, record|
        # Don't add records to the data to send if it's nil/empty, or if it
        # doesn't contain the configured strings
        if !record.nil? && @contains.all? { |match| record.include?(match) }
          data << record
        else
          $log.debug "HTTPBufferedOut: Empty or invalid record. Not adding to the array of data to flush"
          $log.trace "HTTPBufferedOut: Record => '#{record}'"
        end
      end

      # If the chunk was empty, or none of the records passed validation, go
      # ahead and return a successful flush
      if data.size() == 0
        $log.debug "HTTPBufferedOut: Empty array of data. Returning successful flush"
        return
      end

      # Check if the formatted url has changed
      date = Time.now
      formatted = date.strftime(@endpoint_url)

      # Update the uri and http object if the date formatted endpoint has changed
      unless formatted.eql?(@formatted_url)
        $log.debug "HTTPBufferedOut: Strftime formatted url has changed since the last flush. Updating URI..."
        @formatted_url = formatted
        @uri = URI.parse(@formatted_url)
      end

      @http_pool.with do |http|
        unless http.started?
          $log.debug "HTTPBufferedOut: Starting new HTTP connection"
          http = http.start
        else
          $log.debug "HTTPBufferedOut: Reusing an existing HTTP connection"
        end
        begin
          response = http.post2(@uri.request_uri, data.join("\n"), initheader = { 'Content-Type' => @http_content_type })

          if @statuses.include? response.code.to_i
            # Raise an exception so that fluent retries
            fail "Server returned bad status: #{response.code}"
          end
        rescue Net::ReadTimeout, Net::OpenTimeout => e
          $log.warn "Net::Timeout exception: #{e.class} => '#{e.message}'"
          raise
        rescue IOError, EOFError, SystemCallError => e
          # server didn't respond
          $log.warn "Net::HTTP.POST exception: #{e.class} => '#{e.message}'"
          raise
        end
      end
    end

    def compat_parameters_default_chunk_key
      return ""
    end
  end
end

