# frozen_string_literal: true

# KclR json log formatter with datadog tracing
module Kcl
  class LogFormatter < Logger::Formatter
    def call(severity, time, _progname, msg)
      payload = extract(msg)

      hash = {
        pid: Process.pid,
        level: severity,
        time: format_datetime(time).strip,
        **payload,
        **tags
      }

      "#{hash.to_json}\n"
    end

    private

      def extract(msg)
        return msg if msg.is_a?(Hash)

        { message: msg2str(msg) }
      end

      def tags
        if defined?(Datadog)
          {
            dd: {
              span_id: Datadog.tracer.active_correlation.span_id,
              trace_id: Datadog.tracer.active_correlation.trace_id
            }
          }
        else
          {}
        end
      end
  end
end
