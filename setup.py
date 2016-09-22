# -*- coding: utf-8 -*-
#!/usr/bin/env python

# (c) [2014] LinkedIn Corp. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
 
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

import setuptools
from sys import version_info

# Project uses OrderedDict which is part of Python Standard Library
# since version 2.7. On older versions, this is provided by simplejson.
install_requires = []
if version_info[:2] <= (2, 7):
    install_requires.append('simplejson >= 2.0.9')

setuptools.setup(
        name='avro_json_serializer',
        version='0.5',
        description='Avro Json Serializer',
        author='Roman Inozemtsev',
        author_email='rinozemtsev@linkedin.com',
        install_requires = install_requires,
        packages = ['avro_json_serializer'],
        license = 'Apache 2.0'
)
