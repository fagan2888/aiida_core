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

import pytest

from aiida import orm
from aiida.tools.groups import GroupPaths


@pytest.fixture(scope='session')
def fixture_environment():
    """Setup a complete AiiDA test environment, with configuration, profile, database and repository."""
    from aiida.manage.fixtures import fixture_manager
    with fixture_manager() as manager:
        yield manager


@pytest.fixture(scope='function')
def new_database(fixture_environment):
    """Clear the database after each test."""
    yield
    fixture_environment.reset_db()


@pytest.fixture(scope='function')
def db_with_groups(new_database):
    """Setup the database with a number of Groups."""
    for label in ['f1/f2/f3a', 'f1/f2/f3b', 'f1/f2/f3-c/f4a']:
        orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.USER.value)


def test_simple(db_with_groups):
    """Test the core functionality of the `GroupPaths` class."""
    grouppaths = GroupPaths()
    assert 'f1' in grouppaths
    assert 'f2' in grouppaths.f1
    assert 'f2' in grouppaths['f1']
    assert 'f3a' in grouppaths['f1/f2']
    assert isinstance(grouppaths.f1.f2.f3a, orm.Group)
    assert isinstance(grouppaths.f1.f2.f3__c, GroupPaths)
    assert len(grouppaths.f1.f2) == 3
    assert sorted(grouppaths.f1.f2) == [('f3-c', False), ('f3a', True), ('f3b', True)]
