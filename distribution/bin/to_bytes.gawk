# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Use gawk because gnu awk can't extract regexp groups; gawk has `match`
BEGIN {
  suffixes[""]=1
  suffixes["K"]=1024
  suffixes["M"]=1024**2
  suffixes["G"]=1024**3
}

match($0, /([0-9.]*)([kKmMgG]?)/, a) {
  printf("%d", a[1] * suffixes[toupper(a[2])])
}