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

describe Kafka::MultiProducer do
  let(:socket) { double(TCPSocket) }

  before(:each) do
    allow(TCPSocket).to receive(:new).and_return(socket) # don't use a real socket
  end

  describe "Kafka Producer" do
    it "should set default host and port if none is specified" do
      subject.host.should eql("localhost")
      subject.port.should eql(9092)
    end

    it "should have compression" do
      subject.should respond_to :compression
      described_class.new(:compression => Kafka::Message::SNAPPY_COMPRESSION).compression.should == Kafka::Message::SNAPPY_COMPRESSION
      described_class.new.compression.should == Kafka::Message::NO_COMPRESSION
    end

    it "sends single messages" do
      message = Kafka::Message.new("ale")
      encoded = Kafka::Encoder.produce("test", 0, message)

      subject.should_receive(:write).with(encoded).and_return(encoded.length)
      subject.push("test", message, :partition => 0).should == encoded.length
    end

    it "sends multiple messages" do
      messages = [Kafka::Message.new("ale"), Kafka::Message.new("beer")]
      reqs = [
        Kafka::ProducerRequest.new("topic", messages[0]),
        Kafka::ProducerRequest.new("topic", messages[1]),
      ]
      encoded = Kafka::Encoder.multiproduce(reqs)

      subject.should_receive(:write).with(encoded).and_return(encoded.length)
      subject.multi_push(reqs).should == encoded.length
    end

    it "should compress messages" do
      subject.compression = Kafka::Message::SNAPPY_COMPRESSION
      allow(socket).to receive(:write).and_return 0
      messages = [Kafka::Message.new("ale"), Kafka::Message.new("beer")]

      encoded = Kafka::Encoder.produce("test", 0, messages[0])
      Kafka::Encoder.should_receive(:produce).with("test", 0, messages[0], subject.compression).and_return encoded
      subject.push("test", messages[0], :partition => 0)

      reqs = [
          Kafka::ProducerRequest.new("topic", messages[0]),
          Kafka::ProducerRequest.new("topic", messages[1]),
      ]
      encoded = Kafka::Encoder.multiproduce(reqs)
      Kafka::Encoder.should_receive(:multiproduce).with(reqs, subject.compression)
      subject.multi_push(reqs)
    end
  end
end
