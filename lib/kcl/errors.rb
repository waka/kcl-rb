module Kcl::Errors
  class IllegalArgumentError < StandardError; end
  class CheckpointNotFoundError < StandardError; end
  class SequenceNumberNotFoundError < StandardError; end
  class LeaseNotAquiredError < StandardError; end
end
