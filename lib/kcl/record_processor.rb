class Kcl::RecordProcessor
  def after_initialize(initialization_input)
    raise NotImplementedError.new("You must implement #{self.class}##{__method__}")
  end

  def process_records(records_input)
    raise NotImplementedError.new("You must implement #{self.class}##{__method__}")
  end

  def shutdown(shutdown_input)
    raise NotImplementedError.new("You must implement #{self.class}##{__method__}")
  end
end
