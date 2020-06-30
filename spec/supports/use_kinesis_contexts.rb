RSpec.shared_context 'use_kinesis' do
  let(:stub_kinesis_client) { Aws::Kinesis::Client.new(stub_responses: true) }

  before do
    allow(Aws::Kinesis::Client).to receive(:new).and_return(stub_kinesis_client)
  end
end
