module Kcl
  class RecordProcessorFactory
    def create_processor
      raise NotImplementedError.new("You must implement #{self.class}##{__method__}")
    end
  end
end
