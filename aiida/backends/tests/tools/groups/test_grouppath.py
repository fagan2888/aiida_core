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

from textwrap import dedent

from click.testing import CliRunner
import pytest

from aiida import orm
from aiida.cmdline.commands.cmd_group import group_path_ls
from aiida.tools.groups.grouppaths import GroupAttr, GroupPath, InvalidPath


def test_basic(clear_database):
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
    assert [(c.path, c.is_virtual) for c in group_path.children] == [('a', False)]
    child = next(group_path.children)
    assert child.parent == group_path
    assert len(child) == 3
    assert [(c.path, c.is_virtual) for c in sorted(child)] == [('a/b', False), ('a/c', True), ('a/f', False)]
    assert [c.path for c in sorted(group_path.walk())] == ['a', 'a/b', 'a/c', 'a/c/d', 'a/c/e', 'a/c/e/g', 'a/f']
    assert not group_path['a'].is_virtual
    group_path['a'].delete_group()
    assert group_path['a'].is_virtual
    assert GroupPath('a/b/c') == GroupPath('a/b') / 'c'


def test_type_string(clear_database):
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


def test_attr(clear_database):
    """Test ``GroupAttr``."""
    for label in ['a', 'a/b', 'a/c/d', 'a/c/e/g', 'a/f', 'bad space', 'bad@char', '_badstart']:
        orm.Group.objects.get_or_create(label)
    group_path = GroupPath()
    assert isinstance(group_path.browse.a.c.d, GroupAttr)
    assert isinstance(group_path.browse.a.c.d(), GroupPath)
    assert group_path.browse.a.c.d().path == 'a/c/d'
    assert not set(group_path.browse.__dir__()).intersection(['bad space', 'bad@char', '_badstart'])
    with pytest.raises(AttributeError):
        group_path.browse.a.c.x  # pylint: disable=pointless-statement


def test_cmdline_group_path_ls(clear_database):
    """Test ``verdi group path ls``"""
    for label in ['a', 'a/b', 'a/c/d', 'a/c/e/g', 'a/f']:
        group, _ = orm.Group.objects.get_or_create(label, type_string=orm.GroupTypeString.USER.value)
        group.description = 'A description of {}'.format(label)
    orm.Group.objects.get_or_create('a/x', type_string=orm.GroupTypeString.UPFGROUP_TYPE.value)

    cli_runner = CliRunner()

    result = cli_runner.invoke(group_path_ls)
    assert result.exit_code == 0, result.exception
    assert result.output == 'a\n'

    result = cli_runner.invoke(group_path_ls, ['a'])
    assert result.exit_code == 0, result.exception
    assert result.output == 'a/b\na/c\na/f\n'

    result = cli_runner.invoke(group_path_ls, ['a/c'])
    assert result.exit_code == 0, result.exception
    assert result.output == 'a/c/d\na/c/e\n'

    for tag in ['-R', '--recursive']:
        result = cli_runner.invoke(group_path_ls, [tag])
        assert result.exit_code == 0, result.exception
        assert result.output == 'a\na/b\na/c\na/c/d\na/c/e\na/c/e/g\na/f\n'

        result = cli_runner.invoke(group_path_ls, [tag, 'a/c'])
        assert result.exit_code == 0, result.exception
        assert result.output == 'a/c/d\na/c/e\na/c/e/g\n'

    for tag in ['-l', '--long']:
        result = cli_runner.invoke(group_path_ls, [tag])
        assert result.exit_code == 0, result.exception
        assert result.output == dedent(
            """\
            Path      Sub-Groups
            ------  ------------
            a                  4
            """
        )

        result = cli_runner.invoke(group_path_ls, [tag, '-d', 'a'])
        assert result.exit_code == 0, result.exception
        assert result.output == dedent(
            """\
            Path      Sub-Groups  Description
            ------  ------------  --------------------
            a/b                0  A description of a/b
            a/c                2  -
            a/f                0  A description of a/f
            """
        )

        result = cli_runner.invoke(group_path_ls, [tag, '-R'])
        assert result.exit_code == 0, result.exception
        assert result.output == dedent(
            """\
            Path       Sub-Groups
            -------  ------------
            a                   4
            a/b                 0
            a/c                 2
            a/c/d               0
            a/c/e               1
            a/c/e/g             0
            a/f                 0
            """
        )

    for tag in ['-g', '--groups-only']:
        result = cli_runner.invoke(group_path_ls, [tag, '-l', '-R', '--with-description'])
        assert result.exit_code == 0, result.exception
        assert result.output == dedent(
            """\
            Path       Sub-Groups  Description
            -------  ------------  ------------------------
            a                   4  A description of a
            a/b                 0  A description of a/b
            a/c/d               0  A description of a/c/d
            a/c/e/g             0  A description of a/c/e/g
            a/f                 0  A description of a/f
            """
        )
