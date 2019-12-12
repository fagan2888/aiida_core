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
    """Test the basic functionality of ``GroupPath``."""
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
    assert group_path.group is None
    assert (group_path / 'a').path == 'a'
    assert (group_path / 'a' / 'b').path == 'a/b'
    assert (group_path / 'a/b').path == 'a/b'
    assert group_path['a/b'].path == 'a/b'
    assert 'a' in group_path
    assert 'x' not in group_path
    assert not group_path['a'].is_virtual
    assert group_path.group is None
    assert isinstance(group_path['a'].group, orm.Group)
    assert group_path['a'].group is not None
    assert group_path['a'].get_or_create_group()[1] is False
    assert len(group_path) == 1
    assert sorted([(c.path, c.is_virtual) for c in group_path.children]) == [('a', False)]
    child = next(group_path.children)
    assert child.parent == group_path
    assert len(child) == 3
    assert sorted([(c.path, c.is_virtual) for c in child]) == [('a/b', False), ('a/c', True), ('a/f', False)]
    assert sorted([c.path for c in group_path.walk()]) == ['a', 'a/b', 'a/c', 'a/c/d', 'a/c/e', 'a/c/e/g', 'a/f']
    assert not group_path['a'].is_virtual
    group_path['a'].delete_group()
    assert group_path['a'].is_virtual
    assert GroupPath('a/b/c') == GroupPath('a/b/c')


def test_type_string(new_database):
    """Test that only the type_string instantiated in ``GroupPath`` is returned."""
    for label in ['a', 'a/b', 'a/c/d', 'a/c/e/g']:
        orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.USER.value)
    for label in ['a/c/e', 'a/f']:
        orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.UPFGROUP_TYPE.value)
    group_path = GroupPath()
    assert sorted([c.path for c in group_path.walk()]) == ['a', 'a/b', 'a/c', 'a/c/d', 'a/c/e', 'a/c/e/g']
    group_path = GroupPath(type_string=orm.GroupTypeString.UPFGROUP_TYPE.value)
    assert sorted([c.path for c in group_path.walk()]) == ['a', 'a/c', 'a/c/e', 'a/f']
    assert GroupPath('a/b/c') != GroupPath('a/b/c', type_string=orm.GroupTypeString.UPFGROUP_TYPE.value)


def test_attr(new_database):
    """Test ``GroupAttr``."""
    for label in ['a', 'a/b', 'a/c/d', 'a/c/e/g', 'a/f', 'bad space', 'bad@char', '_badstart']:
        orm.Group.objects.get_or_create(label)
    group_path = GroupPath()
    # print(group_path.browse.a.c)
    # print(group_path.browse.a.c())
    assert isinstance(group_path.browse.a.c.d, GroupAttr)
    assert isinstance(group_path.browse.a.c.d(), GroupPath)
    assert group_path.browse.a.c.d().path == 'a/c/d'
    assert not set(group_path.browse.__dir__()).intersection(['bad space', 'bad@char', '_badstart'])
    with pytest.raises(AttributeError):
        group_path.browse.a.c.x  # pylint: disable=pointless-statement

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
Path    Virtual      Children
------  ---------  ----------
a       False               3
"""
    )


def test_cmndline_all():
    """Test ``verdi group path -l``"""
    from aiida.cmdline.commands.cmd_group import group_path
    from click.testing import CliRunner
    for label in ['a', 'a/b', 'a/c/d', 'a/c/e/g', 'a/f']:
        orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.USER.value)
    cli_runner = CliRunner()
    result = cli_runner.invoke(group_path, ['-l'])
    assert result.exit_code == 0, result.exception
    print(result.output)
    assert result.output == (
        """\
Path     Virtual      Children
-------  ---------  ----------
a        False               3
a/f      False               0
a/c      True                2
a/c/e    True                1
a/c/e/g  False               0
a/c/d    False               0
a/b      False               0
"""
    )
