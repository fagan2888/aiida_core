# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Provides functionality for managing large numbers of AiiDA Groups, via label delimitation."""
# pylint: disable=fixme,missing-docstring
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import re
from typing import Iterable, List, Optional  # pylint: disable=unused-import
import warnings

from aiida import orm
from aiida.common.exceptions import NotExistent

__all__ = ('GroupPath', 'InvalidPath')

# TODO check 'official' regex for attributes
RegexAttribute = re.compile('^[a-zA-Z][\\_a-zA-Z0-9]*$')

# TODO document methods


class InvalidPath(Exception):
    pass


class GroupPath:
    """A class to provide label delimited access to groups."""

    def __init__(self, path='', type_string=orm.GroupTypeString.USER.value, parent=None):
        # type: (str, Optional[str], Optional[GroupPath], str)
        self._delimiter = '/'
        if type_string is not None and not isinstance(type_string, str):
            raise AssertionError('type_string must be None or str: {}'.format(type_string))
        # TODO assert type_string is valid GroupTypeString value?
        # TODO check that GroupTypeString.USER is the correct default, or None
        self._type_string = type_string
        self._path_string = self._validate_path(path)
        self._path_list = self._path_string.split(self._delimiter) if path else []
        if parent is not None and not isinstance(parent, GroupPath):
            raise TypeError('parent is not None or GroupPath: {}'.format(parent))
        self._parent = parent

    def _validate_path(self, path):
        if path == self._delimiter:
            return ''
        if (self._delimiter * 2 in path or path.startswith(self._delimiter) or path.endswith(self._delimiter)):
            raise InvalidPath(str(path))
        return path

    def __repr__(self):
        # type: () -> str
        return "GroupPath('{}') @ {}".format(self.path, id(self))

    def __eq__(self, other):
        if not isinstance(other, GroupPath):
            return False
        if other.path == self.path and other.type_string == self.type_string:
            return True
        return False

    @property
    def path(self):
        # type: () -> str
        return self._path_string

    @property
    def path_list(self):
        # type: () -> List[str]
        return self._path_list[:]

    @property
    def delimiter(self):
        # type: () -> str
        return self._delimiter

    @property
    def type_string(self):
        # type: () -> str
        return self._type_string

    @property
    def parent(self):
        # type: () -> Optional[GroupPath]
        return self._parent

    def __truediv__(self, path):
        # type: (str) -> GroupPath
        if not isinstance(path, str):
            raise TypeError('path is not a string: {}'.format(path))
        path = self._validate_path(path)
        child = self
        for key in path.split(self.delimiter):
            child = GroupPath(
                path=child.path + self.delimiter + key if child.path else key,
                type_string=self.type_string,
                parent=child
            )
        return child

    def __getitem__(self, path):
        # type: (str) -> GroupPath
        return self.__truediv__(path)

    @property
    def group(self):
        # type: () -> Optional[orm.Group]
        try:
            if self.type_string is not None:
                return orm.Group.objects.get(label=self.path)
            return orm.Group.objects.get(label=self.path, type_string=self.type_string)
        except NotExistent:
            return None

    @property
    def is_virtual(self):
        # type: () -> bool
        # TODO can we query for Group without loading (to improve speed)
        return self.group is None

    def get_or_create_group(self):
        # type: () -> (orm.Group, bool)
        if self.type_string is not None:
            return orm.Group.objects.get_or_create(label=self.path, type_string=self.type_string)
        return orm.Group.objects.get_or_create(label=self.path)

    def delete_group(self):
        group = self.group
        if group is not None:
            orm.Group.objects.delete(group.id)

    @property
    def children(self):
        # type: () -> Iterable[GroupPath]
        query = orm.QueryBuilder()
        query.append(
            orm.Group,
            filters={'type_string': self.type_string} if self.type_string is not None else {},
            project='label'
        )
        yielded = []
        for (label,) in query.iterall():
            path = label.split(self._delimiter)
            if len(path) <= len(self._path_list):
                continue
            path_string = self._delimiter.join(path[:len(self._path_list) + 1])
            if (path_string not in yielded and path[:len(self._path_list)] == self._path_list):
                yielded.append(path_string)
                try:
                    yield GroupPath(path=path_string, type_string=self.type_string, parent=self)
                except InvalidPath:
                    # TODO raise warning or exception? (maybe set what to do in init?)
                    warnings.warn('invalid path encountered: {}'.format(path_string))

    def __iter__(self):
        # type: () -> Iterable[GroupPath]
        return self.children

    def __len__(self):
        # type: () -> int
        return sum(1 for _ in self.children)

    def __contains__(self, key):
        # type: (str) -> bool
        for child in self.children:
            if child._path_list[-1] == key:  # pylint: disable=protected-access
                return True
        return False

    def walk(self):
        # type: () -> Iterable[GroupPath]
        for child in self:
            yield child
            for sub_child in child.walk():
                yield sub_child

    @property
    def browse(self):
        return GroupAttr(self)


class GroupAttr:

    def __init__(self, group_path):
        self._group_path = group_path  # type: GroupPath
        # TODO how to deal with key -> attribute clashes # pylint: disable=fixme
        self._attr_to_child = {c.path_list[-1]: c for c in self._group_path.children if RegexAttribute.match(c.path_list[-1])}

    def __repr__(self):
        # type: () -> str
        return "GroupAttr('{}') @ {}".format(self._group_path.path, id(self))

    def __call__(self):
        return self._group_path

    def __dir__(self):
        """Return a list of available attributes."""
        return list(self._attr_to_child.keys())

    def __getattr__(self, attr):
        """Return the requested attribute name."""
        if attr == 'get_child':
            return self.get_child
        try:
            child = self._attr_to_child[attr]
        except KeyError:
            raise AttributeError(attr)
        return GroupAttr(child)
