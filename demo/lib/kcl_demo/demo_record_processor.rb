require 'pry'

module KclDemo
  class DemoRecordProcessor < Kcl::RecordProcessor
    # @implement
    def after_initialize(initialization_input)
      Kcl.logger.info(message: "Initialization", input: initialization_input)
    end

    # @implement
    def process_records(records_input)
      # レコードのリストを取得
      return if records_input.records.empty?

      Kcl.logger.info(message: "Processing records...")

      records_input.records.each do |record|
        Kcl.logger.info(message: "record", record: record)
      end

      # チェックポイントを記録
      last_sequence_number = records_input.records[-1].sequence_number
      Kcl.logger.info(
        message: "Checkpoint progress",
        last_sequence_number: last_sequence_number,
        millis_behind_latest: records_input.millis_behind_latest
      )
      records_input.record_checkpointer.update_checkpoint(last_sequence_number)
    end

    # @implement
    def shutdown(shutdown_input)
      Kcl.logger.info(message: "Shutdown", reason: shutdown_input.shutdown_reason)

      if shutdown_input.shutdown_reason == Kcl::Workers::ShutdownReason::TERMINATE
        shutdown_input.record_checkpointer.update_checkpoint(nil)
      end
    end
  end
end
