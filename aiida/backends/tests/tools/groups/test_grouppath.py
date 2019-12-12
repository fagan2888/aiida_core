# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Tests for GroupPath"""
# pylint: disable=redefined-outer-name,unused-argument
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import pytest

from aiida import orm
from aiida.tools.groups.grouppaths import GroupAttr, GroupPath, InvalidPath


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


def test_basic(new_database):
    """Setup the database with a number of Groups."""
    for label in ['a', 'a/b', 'a/c/d', 'a/c/e/g', 'a/f']:
        orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.USER.value)
    for path in ['/a', 'a/', '/a/', 'a//b']:
        with pytest.raises(InvalidPath):
            GroupPath(path=path)
    group_path = GroupPath()
    assert group_path.path == ''
    assert group_path.delimiter == '/'
    assert group_path.parent is None
    assert group_path.is_virtual
    assert not group_path.has_group
    assert group_path.get_group() is None
    assert group_path.get_nodes() is None
    assert (group_path / 'a').path == 'a'
    assert (group_path / 'a' / 'b').path == 'a/b'
    assert (group_path / 'a/b').path == 'a/b'
    assert group_path['a/b'].path == 'a/b'
    assert 'a' in group_path
    assert 'x' not in group_path
    assert group_path['a'].has_group
    assert group_path.get_group() is None
    assert isinstance(group_path['a'].get_group(), orm.Group)
    assert group_path['a'].get_nodes() is not None
    assert group_path['a'].get_or_create_group()[1] is False
    assert len(group_path) == 1
    assert sorted([(c.path, c.is_virtual) for c in group_path.children]) == [('a', False)]
    child = next(group_path.children)
    assert child.root == group_path
    assert child.parent == group_path
    assert len(child) == 3
    assert sorted([(c.path, c.is_virtual) for c in child]) == [('a/b', False), ('a/c', True), ('a/f', False)]
    assert sorted([c.path for c in group_path.walk()]) == ['a', 'a/b', 'a/c', 'a/c/d', 'a/c/e', 'a/c/e/g', 'a/f']
    assert group_path['a'].has_group
    group_path['a'].delete_group()
    assert not group_path['a'].has_group


def test_attr(new_database):
    """Setup the database with a number of Groups."""
    for label in ['a', 'a/b', 'a/c/d', 'a/c/e/g', 'a/f']:
        orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.USER.value)
    group_path = GroupPath()
    # print(group_path.attr.a.c._group_path)
    # print(group_path.attr.a.c._attr_to_child)
    assert isinstance(group_path.attr.a.c.d, GroupAttr)
    assert isinstance(group_path.attr.a.c['d'], GroupAttr)
    assert isinstance(group_path.attr.a.c.d.get_path(), GroupPath)
    assert group_path.attr.a.c.d.get_path().path == 'a/c/d'
    with pytest.raises(AttributeError):
        group_path.attr.a.c.x  # pylint: disable=pointless-statement


def test_cmndline():
    """Test ``verdi group path``"""
    from aiida.cmdline.commands.cmd_group import group_path
    from click.testing import CliRunner
    for label in ['a', 'a/b', 'a/c/d', 'a/c/e/g', 'a/f']:
        orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.USER.value)
    cli_runner = CliRunner()
    result = cli_runner.invoke(group_path)
    assert result.exit_code == 0, result.exception
    # print(result.output)
    assert result.output == (
        """\
Path     Virtual      Children
-------  ---------  ----------
a        False               3
a/b      False               0
a/c      True                2
a/c/d    False               0
a/c/e    True                1
a/c/e/g  False               0
a/f      False               0
"""
    )
