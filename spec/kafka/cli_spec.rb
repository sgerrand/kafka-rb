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
require 'kafka/cli'

describe Kafka::CLI do
  before(:each) do
    described_class.instance_variable_set("@config", {})
    allow(described_class).to receive(:puts)
  end

  describe "should read from env" do
    describe "kafka host" do
      it "should read KAFKA_HOST from env" do
        described_class.read_env("KAFKA_HOST" => "google.com")
        described_class.config[:host].should == "google.com"
      end

      it "kafka port" do
        described_class.read_env("KAFKA_PORT" => "1234")
        described_class.config[:port].should == 1234
      end

      it "kafka topic" do
        described_class.read_env("KAFKA_TOPIC" => "news")
        described_class.config[:topic].should == "news"
      end

      it "kafka compression" do
        described_class.read_env("KAFKA_COMPRESSION" => "no")
        described_class.config[:compression].should == Kafka::Message::NO_COMPRESSION

        described_class.read_env("KAFKA_COMPRESSION" => "gzip")
        described_class.config[:compression].should == Kafka::Message::GZIP_COMPRESSION

        described_class.read_env("KAFKA_COMPRESSION" => "snappy")
        described_class.config[:compression].should == Kafka::Message::SNAPPY_COMPRESSION
      end
    end
  end

  describe "should read from command line" do
    it "kafka host" do
      described_class.parse_args(%w(--host google.com))
      described_class.config[:host].should == "google.com"

      described_class.parse_args(%w(-h google.com))
      described_class.config[:host].should == "google.com"
    end

    it "kafka port" do
      described_class.parse_args(%w(--port 1234))
      described_class.config[:port].should == 1234

      described_class.parse_args(%w(-p 1234))
      described_class.config[:port].should == 1234
    end

    it "kafka topic" do
      described_class.parse_args(%w(--topic news))
      described_class.config[:topic].should == "news"

      described_class.parse_args(%w(-t news))
      described_class.config[:topic].should == "news"
    end

    it "kafka compression" do
      allow(described_class).to receive(:publish?).and_return true
      described_class.parse_args(%w(--compression no))
      described_class.config[:compression].should == Kafka::Message::NO_COMPRESSION
      described_class.parse_args(%w(-c no))
      described_class.config[:compression].should == Kafka::Message::NO_COMPRESSION

      described_class.parse_args(%w(--compression gzip))
      described_class.config[:compression].should == Kafka::Message::GZIP_COMPRESSION
      described_class.parse_args(%w(-c gzip))
      described_class.config[:compression].should == Kafka::Message::GZIP_COMPRESSION

      described_class.parse_args(%w(--compression snappy))
      described_class.config[:compression].should == Kafka::Message::SNAPPY_COMPRESSION
      described_class.parse_args(%w(-c snappy))
      described_class.config[:compression].should == Kafka::Message::SNAPPY_COMPRESSION
    end

    it "message" do
      allow(described_class).to receive(:publish?).and_return true
      described_class.parse_args(%w(--message YEAH))
      described_class.config[:message].should == "YEAH"

      described_class.parse_args(%w(-m YEAH))
      described_class.config[:message].should == "YEAH"
    end

  end

  describe "config validation" do
    it "should assign a default port" do
      allow(described_class).to receive(:exit)
      allow(described_class).to receive(:puts)
      described_class.validate_config
      described_class.config[:port].should == Kafka::IO::PORT
    end
  end

  it "should assign a default host" do
    allow(described_class).to receive(:exit)
    described_class.validate_config
    described_class.config[:host].should == Kafka::IO::HOST
  end


  it "read compression method" do
    described_class.string_to_compression("no").should == Kafka::Message::NO_COMPRESSION
    described_class.string_to_compression("gzip").should == Kafka::Message::GZIP_COMPRESSION
    described_class.string_to_compression("snappy").should == Kafka::Message::SNAPPY_COMPRESSION
    lambda { described_class.push(:string_to_compression,nil) }.should raise_error
  end

end
