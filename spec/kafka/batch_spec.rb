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

describe Kafka::Batch do
  describe "batch messages" do
    it "holds all messages to be sent" do
      subject.should respond_to(:messages)
      subject.messages.class.should eql(Array)
    end

    it "supports queueing/adding messages to be send" do
      subject.messages << double(Kafka::Message.new("one"))
      subject.messages << double(Kafka::Message.new("two"))
      subject.messages.length.should eql(2)
    end
  end
end