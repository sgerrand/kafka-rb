# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
require 'spec_helper'

describe Kafka::Consumer do
  let(:consumer) { described_class.new(:offset => 0) }
  let(:socket)   { double(TCPSocket) }

  before(:each) do
    allow(TCPSocket).to receive(:new).and_return(socket) # don't use a real socket
  end

  describe "Kafka Consumer" do

    it "should have a Kafka::RequestType::FETCH" do
      Kafka::RequestType::FETCH.should eql(1)
      consumer.should respond_to(:request_type)
    end

    it "should have a topic and a partition" do
      consumer.should respond_to(:topic)
      consumer.should respond_to(:partition)
    end

    it "should have a polling option, and a default value" do
      described_class::DEFAULT_POLLING_INTERVAL.should eql(2)
      consumer.should respond_to(:polling)
      consumer.polling.should eql(2)
    end

    it "should set a topic and partition on initialize" do
      consumer = described_class.new({ :host => "localhost", :port => 9092, :topic => "testing" })
      consumer.topic.should eql("testing")
      consumer.partition.should eql(0)
      consumer = described_class.new({ :topic => "testing", :partition => 3 })
      consumer.partition.should eql(3)
    end

    it "should set default host and port if none is specified" do
      subject.host.should eql("localhost")
      subject.port.should eql(9092)
    end

    it "should not have a default offset but be able to set it" do
      subject.offset.should be_nil
      consumer = described_class.new({ :offset => 1111 })
      consumer.offset.should eql(1111)
    end

    it "should have a max size" do
      described_class::MAX_SIZE.should eql(1048576)
      consumer.max_size.should eql(1048576)
    end

    it "should return the size of the request" do
      consumer.topic = "someothertopicname"
      consumer.encoded_request_size.should eql([38].pack("N"))
    end

    it "should encode a request to consume" do
      bytes = [Kafka::RequestType::FETCH].pack("n") + ["test".length].pack("n") + "test" + [0].pack("N") + [0].pack("q").reverse + [described_class::MAX_SIZE].pack("N")
      consumer.encode_request(Kafka::RequestType::FETCH, "test", 0, 0, described_class::MAX_SIZE).should eql(bytes)
    end

    it "should read the response data" do
      bytes = [0].pack("n") + [1120192889].pack("N") + "ale"
      socket.should_receive(:read).and_return([9].pack("N"))
      socket.should_receive(:read).with(9).and_return(bytes)
      consumer.read_data_response.should eql(bytes[2,7])
    end

    it "should send a consumer request" do
      allow(consumer).to receive(:encoded_request_size).and_return(666)
      allow(consumer).to receive(:encode_request).and_return("someencodedrequest")
      consumer.should_receive(:write).with("someencodedrequest").exactly(:once).and_return(true)
      consumer.should_receive(:write).with(666).exactly(:once).and_return(true)
      consumer.send_consume_request.should eql(true)
    end

    it "should consume messages" do
      consumer.should_receive(:send_consume_request).and_return(true)
      consumer.should_receive(:read_data_response).and_return("")
      consumer.consume.should eql([])
    end

    it "should loop and execute a block with the consumed messages" do
      allow(consumer).to receive(:consume).and_return([double(Kafka::Message)])
      messages = []
      messages.should_receive(:<<).exactly(:once).and_return([])
      consumer.loop do |message|
        messages << message
        break # we don't wanna loop forever on the test
      end
    end

    it "should loop (every N seconds, configurable on polling attribute), and execute a block with the consumed messages" do
      consumer = described_class.new({ :polling => 1 })
      allow(consumer).to receive(:consume).and_return([double(Kafka::Message)])
      messages = []
      messages.should_receive(:<<).exactly(:twice).and_return([])
      executed_times = 0
      consumer.loop do |message|
        messages << message
        executed_times += 1
        break if executed_times >= 2 # we don't wanna loop forever on the test, only 2 seconds
      end

      executed_times.should eql(2)
    end

    it "should fetch initial offset if no offset is given" do
      subject.should_receive(:fetch_latest_offset).exactly(:once).and_return(1000)
      subject.should_receive(:send_consume_request).and_return(true)
      subject.should_receive(:read_data_response).and_return("")
      subject.consume
      subject.offset.should eql(1000)
    end

    it "should encode an offset request" do
      bytes = [Kafka::RequestType::OFFSETS].pack("n") + ["test".length].pack("n") + "test" + [0].pack("N") + [-1].pack("q").reverse + [described_class::MAX_OFFSETS].pack("N")
      consumer.encode_request(Kafka::RequestType::OFFSETS, "test", 0, -1, described_class::MAX_OFFSETS).should eql(bytes)
    end

    it "should parse an offsets response" do
      bytes = [0].pack("n") + [1].pack('N') + [21346].pack('q').reverse
      socket.should_receive(:read).and_return([14].pack("N"))
      socket.should_receive(:read).and_return(bytes)
      consumer.read_offsets_response.should eql(21346)
    end
  end
end
