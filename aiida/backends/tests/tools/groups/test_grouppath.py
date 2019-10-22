# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for creating graphs (using graphviz)"""
# pylint: disable=redefined-outer-name,unused-argument
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from aiida import orm
from aiida.backends.testbase import AiidaTestCase
from aiida.tools.groups import GroupPaths


class TestGroups(AiidaTestCase):
    """Test backend entities and their collections"""

    def setUp(self):
        """Setup the database with a number of Groups."""
        for label in ['f1/f2/f3a', 'f1/f2/f3b', 'f1/f2/f3-c/f4a']:
            orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.USER.value)

    def test_simple(self):
        """Test the core functionality of the `GroupPaths` class."""
        grouppaths = GroupPaths()
        self.assertIn('f1', grouppaths)
        self.assertIn('f2', grouppaths['f1'])
        self.assertIn('f2', grouppaths.f1)
        self.assertIn('f3a', grouppaths['f1/f2'])
        self.assertIsInstance(grouppaths.f1.f2.f3a, orm.Group)
        self.assertIsInstance(grouppaths.f1.f2.f3__c, GroupPaths)
        self.assertEqual(len(grouppaths.f1.f2), 3)
        self.assertEqual(sorted(grouppaths.f1.f2), [('f3-c', False), ('f3a', True), ('f3b', True)])
