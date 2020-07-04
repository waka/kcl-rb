RSpec.shared_context 'use_record_processor' do
  class MockRecordProcessor < Kcl::RecordProcessor
    def after_initialize(initialization_input)
    end

    def process_records(records_input)
      return if records_input.records.size == 0
      records_input.records.each do |record|
        process_record(record)
      end
      last_sequence_number = records_input.records[-1].sequence_number
      records_input.record_checkpointer.update_checkpoint(last_sequence_number)
    end

    def process_record(record)
      puts record
    end

    def shutdown(shutdown_input)
      if shutdown_input.shutdown_reason == Kcl::Workers::ShutdownReason::TERMINATE
        shutdown_input.record_checkpointer.update_checkpoint(nil)
      end
    end
  end
end
