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
    config_param :http_read_timeout, :float, default: 2.0
    # open timeout for the http call
    config_param :http_open_timeout, :float, default: 2.0

    def initialize
      super
      require 'net/http'
      require 'uri'
    end

    def configure(conf)
      super

      # Allows for time formatted endpoints
      date = Time.now
      @endpoint_url = date.strftime(@endpoint_url)

      # Check if endpoint URL is valid
      unless @endpoint_url =~ /^#{URI.regexp}$/
        fail Fluent::ConfigError, 'endpoint_url invalid'
      end

      begin
        @uri = URI.parse(@endpoint_url)
      rescue URI::InvalidURIError
        raise Fluent::ConfigError, 'endpoint_url invalid'
      end

      # Parse http statuses
      @statuses = @http_retry_statuses.split(',').map { |status| status.to_i }

      @statuses = [] if @statuses.nil?

      @http = Net::HTTP.new(@uri.host, @uri.port)
      @http.read_timeout = @http_read_timeout
      @http.open_timeout = @http_open_timeout
    end

    def start
      super
    end

    def shutdown
      super
      begin
        @http.finish
      rescue
      end
    end

    def write(chunk)
      data = []
      chunk.each do |time, record|
        data << record
      end

      request = create_request(data)

      begin
        response = @http.start do |http|
          request = create_request(data)
          http.request request
        end

        if @statuses.include? response.code.to_i
          # Raise an exception so that fluent retries
          fail "Server returned bad status: #{response.code}"
        end
      rescue Net::ReadTimeout, Net::OpenTimeout => e
        $log.warn "Net::Timeout exception: #{e.class} => '#{e.message}'"
        raise
      rescue IOError, EOFError, SystemCallError => e
        # server didn't respond
        $log.warn "Net::HTTP.#{request.method.capitalize} exception: #{e.class} => '#{e.message}'"
        raise
      ensure
        begin
          @http.finish
        rescue
        end
      end
    end

    def compat_parameters_default_chunk_key
      return ""
    end

    protected

      def create_request(data)
        request = Net::HTTP::Post.new(@uri.request_uri)

        # Headers
        request['Content-Type'] = 'application/xml'

        # Body
        request.body = data.join("\n")#JSON.dump(data)

        request
      end
  end
end

