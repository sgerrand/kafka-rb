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

describe Kafka::IO do
  let(:io)     { Class.new { extend Kafka::IO } }
  let(:socket) { double(TCPSocket) }

  before(:each) do
    allow(TCPSocket).to receive(:new).and_return(socket) # don't use a real socket
    io.connect("somehost", 9093)
  end

  describe "default methods" do
    let(:data)   { "some data" }
    let(:length) { 200 }

    it "has a socket, a host and a port" do
      [:socket, :host, :port].each do |m|
        io.should respond_to(m.to_sym)
      end
    end

    it "raises an exception if no host and port is specified" do
      lambda {
        io.connect
      }.should raise_error(ArgumentError)
    end
    
    it "should remember the port and host on connect" do
      io.connect("somehost", 9093)
      io.host.should eql("somehost")
      io.port.should eql(9093)
    end

    it "should write to a socket" do
      socket.should_receive(:write).with(data).and_return(9)
      io.write(data).should eql(9)
    end

    it "should read from a socket" do
      socket.should_receive(:read).with(length).and_return("foo")
      io.read(length)
    end

    it "should disconnect on a timeout when reading from a socket (to aviod protocol desync state)" do
      socket.should_receive(:read).with(length).and_raise(Errno::EAGAIN)
      io.should_receive(:disconnect)
      lambda { io.read(length) }.should raise_error(Kafka::SocketError)
    end

    it "should disconnect" do
      io.should respond_to(:disconnect)
      socket.should_receive(:close).and_return(nil)
      io.disconnect
    end

    it "should reconnect" do
      TCPSocket.should_receive(:new)
      io.reconnect
    end

    it "should disconnect on a broken pipe error" do
      [Errno::ECONNABORTED, Errno::EPIPE, Errno::ECONNRESET].each do |error|
        socket.should_receive(:write).exactly(:once).and_raise(error)
        socket.should_receive(:close).exactly(:once).and_return(nil)
        lambda { io.write("some data to send") }.should raise_error(Kafka::SocketError)
      end
    end
  end
end
