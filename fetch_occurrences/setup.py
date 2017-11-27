#
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
#

"""Setup.py module for the workflow's worker utilities.
All the workflow related code is gathered in a package that will be built as a
source distribution, staged in the staging area for the workflow being run and
then installed in the workers when they start running.
This behavior is triggered by specifying the --setup_file command line option
when running the workflow for remote execution.
"""

import setuptools

if __name__ == '__main__':
    setuptools.setup(
        name='occurrences',
        version='0.0.1',
        description='Floracast dataflow pipeline.',
        packages=setuptools.find_packages(),
        url="https://bitbucket.org/heindl/dataflow",
        author="mph",
        author_email="matt@floracast.com",
        install_requires=[
            "apache-beam==2.2.0",
            "google-cloud==0.30.0",
            "google-cloud-bigquery==0.28.0",
            "tensorflow==1.4.0",
            "tensorflow-transform==0.3.1",
            "six==1.10.0"
        ]
    )
