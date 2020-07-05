require 'bigdecimal'

module Kcl::Types
  class ExtendedSequenceNumber
    attr_reader :sequence_number, :sub_sequence_number

    TRIM_HORIZON_VALUE = BigDecimal(-2)
    LATEST_VALUE       = BigDecimal(-1)
    AT_TIMESTAMP_VALUE = BigDecimal(-3)

    # @return [Kcl::Types::ExtendedSequenceNumber]
    def self.latest
      @_latest ||= self.new(Kcl::Checkpoints::Sentinel::LATEST)
    end

    # @return [Kcl::Types::ExtendedSequenceNumber]
    def self.shard_end
      @_shard_end ||= self.new(Kcl::Checkpoints::Sentinel::SHARD_END)
    end

    # @return [Kcl::Types::ExtendedSequenceNumber]
    def self.trim_horizon
      @_trim_horizon ||= self.new(Kcl::Checkpoints::Sentinel::TRIM_HORIZON)
    end

    # @param [String] str
    # @return [Boolean]
    def self.digits_or_sentinel?(str)
      digits?(str) || sentinel?(str)
    end

    # @param [String] str
    # @return [Boolean]
    def self.sentinel?(str)
      case str
      when Kcl::Checkpoints::Sentinel::TRIM_HORIZON,
        Kcl::Checkpoints::Sentinel::LATEST,
        Kcl::Checkpoints::Sentinel::SHARD_END,
        Kcl::Checkpoints::Sentinel::AT_TIMESTAMP
        true
      else
        false
      end
    end

    # @param [String] str
    # @return [Boolean]
    def self.digits?(str)
      return false if str.nil? || str.empty?
      (str =~ /\A[0-9]+\z/) != nil
    end

    # @param [String] sequence_number
    # @param [Number] sub_sequence_number
    def initialize(sequence_number, sub_sequence_number = 0)
      @sequence_number     = sequence_number
      @sub_sequence_number = sub_sequence_number
    end

    # @return [BigDecimal]
    def value
      if self.class.digits?(@sequence_number)
        return BigDecimal(@sequence_number)
      end

      case @sequence_number
      when Kcl::Checkpoints::Sentinel::LATEST
        LATEST_VALUE
      when Kcl::Checkpoints::Sentinel::TRIM_HORIZON
        TRIM_HORIZON_VALUE
      when Kcl::Checkpoints::Sentinel::AT_TIMESTAMP
        AT_TIMESTAMP_VALUE
      else
        raise Kcl::Errors::IllegalArgumentError.new(
          'Expected a string of digits, TRIM_HORIZON, LATEST or AT_TIMESTAMP but received ' + @sequence_number
        )
      end
    end

    # @param [Kcl::Types::ExtendedSequenceNumber] extended_sequence_number
    # @return [Boolean]
    def equals(extended_sequence_number)
      if @sequence_number != extended_sequence_number.sequence_number
        return false
      end
      @sub_sequence_number == extended_sequence_number.sub_sequence_number
    end
  end
end
