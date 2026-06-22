#
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing,
#  * software distributed under the License is distributed on an
#  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  * KIND, either express or implied.  See the License for the
#  * specific language governing permissions and limitations
#  * under the License.
#  */
#
mkdir -p /hive-jars &&
curl -f -L -o /hive-jars/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar &&
curl -f -L -o /hive-jars/hadoop-aws-3.3.6.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar &&
curl -f -L -o /hive-jars/postgresql-42.7.7.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar &&
mkdir -p /connect-jars &&
curl -f -L -o /connect-jars/hadoop-auth-3.4.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.4.1/hadoop-auth-3.4.1.jar &&
curl -f -L -o /connect-jars/hadoop-aws-3.4.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar &&
curl -f -L -o /connect-jars/hadoop-common-3.4.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar &&
curl -f -L -o /connect-jars/hive-storage-api-2.7.2.jar https://repo1.maven.org/maven2/org/apache/hive/hive-storage-api/2.7.2/hive-storage-api-2.7.2.jar &&
curl -f -L -o /connect-jars/s3-transfer-manager-2.42.39.jar https://repo1.maven.org/maven2/software/amazon/awssdk/s3-transfer-manager/2.42.39/s3-transfer-manager-2.42.39.jar


