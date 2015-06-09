# encoding: utf-8

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

describe Kafka::Producer do
  let(:socket) { double(TCPSocket) }

  before(:each) do
    allow(TCPSocket).to receive(:new).and_return(socket) # don't use a real socket
  end

  describe "Kafka Producer" do
    it "should have a topic and a partition" do
      subject.should respond_to(:topic)
      subject.should respond_to(:partition)
    end

    it "should have compression" do
      subject.should respond_to :compression
      described_class.new(:compression => 1).compression.should == 1
      described_class.new.compression.should == 0
    end

    it "should set a topic and partition on initialize" do
      subject = described_class.new({ :host => "localhost", :port => 9092, :topic => "testing" })
      subject.topic.should eql("testing")
      subject.partition.should eql(0)
      subject = described_class.new({ :topic => "testing", :partition => 3 })
      subject.partition.should eql(3)
    end

    it "should set default host and port if none is specified" do
      subject.host.should eql("localhost")
      subject.port.should eql(9092)
    end
  end

  it "should send messages" do
    subject.should_receive(:write).and_return(32)
    message = Kafka::Message.new("ale")
    subject.push(message).should eql(32)
  end

  describe "Message Batching" do
    it "should batch messages and send them at once" do
      message1 = Kafka::Message.new("one")
      message2 = Kafka::Message.new("two")
      subject.should_receive(:push).with([message1, message2]).exactly(:once).and_return(nil)
      subject.batch do |messages|
        messages << message1
        messages << message2
      end
    end
  end
end
