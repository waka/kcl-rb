module KclDemo
  class DemoRecordProcessorFactory < Kcl::RecordProcessorFactory
    def create_processor
      KclDemo::DemoRecordProcessor.new
    end
  end
end
