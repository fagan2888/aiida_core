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

from typing import Iterable, List, Optional  # pylint: disable=unused-import
import warnings

import six

from aiida import orm
from aiida.common.exceptions import NotExistent

__all__ = ('GroupPath', 'InvalidPath')

# TODO document methods


class InvalidPath(Exception):
    pass


class GroupPath:
    """A class to provide label delimited access to groups."""

    def __init__(self, path='', parent=None, delimiter='/'):
        # type: (str, Optional[GroupPath], str)
        self._delimiter = delimiter
        # TODO validator
        self._path_string = self._validate_path(path)
        self._path_list = self._path_string.split(delimiter) if path else []
        if parent is not None and not isinstance(parent, GroupPath):
            raise TypeError('parent is not None or GroupPath: {}'.format(parent))
        self._parent = parent

        # TODO additional filters for get/create/query groups, e.g. type_string
        # set in init or allow kwargs on methods?

    def _validate_path(self, path):
        if path == self._delimiter:
            return ''
        if (self._delimiter * 2 in path or path.startswith(self._delimiter) or path.endswith(self._delimiter)):
            raise InvalidPath(str(path))
        return path

    def __repr__(self):
        # type: () -> str
        return "GroupPath('{}') @ {}".format(self.path, id(self))

    @property
    def path(self):
        # type: () -> str
        return self._path_string

    @property
    def delimiter(self):
        # type: () -> str
        return self._delimiter

    @property
    def parent(self):
        # type: () -> Optional[GroupPath]
        return self._parent

    @property
    def root(self):
        # type: () -> GroupPath
        # TODO check for circular recursion?
        if self.parent is None:
            return self
        return self.parent.root

    def __truediv__(self, path):
        # type: (str) -> GroupPath
        if not isinstance(path, six.string_types):
            raise TypeError('path is not a string: {}'.format(path))
        path = self._validate_path(path)
        parent = self
        for key in path.split(self.delimiter):
            parent = GroupPath(
                path=parent.path + self.delimiter + key if parent.path else key,
                parent=parent,
                delimiter=self._delimiter,
            )
        return parent

    def __getitem__(self, path):
        # type: (str) -> GroupPath
        return self.__truediv__(path)

    def get_group(self):
        # type: () -> Optional[orm.Group]
        try:
            return orm.Group.objects.get(label=self.path)
        except NotExistent:
            return None

    @property
    def is_virtual(self):
        # type: () -> bool
        # TODO can we query for Group without loading (to improve speed)
        return self.get_group() is None

    @property
    def has_group(self):
        # type: () -> bool
        return not self.is_virtual

    def get_or_create_group(self, **kwargs):
        # type: () -> (orm.Group, bool)
        return orm.Group.objects.get_or_create(label=self.path, **kwargs)

    def delete_group(self):
        group = self.get_group()
        if group is not None:
            orm.Group.objects.delete(group.id)

    def get_nodes(self):
        # type: () -> Optional[Iterable]
        group = self.get_group()
        return None if group is None else group.nodes

    @property
    def children(self):
        # type: () -> Iterable[GroupPath]
        query = orm.QueryBuilder()
        query.append(orm.Group, project='label')
        yielded = []
        for (label,) in query.iterall():
            path = label.split(self._delimiter)
            if len(path) <= len(self._path_list):
                continue
            path_string = self._delimiter.join(path[:len(self._path_list) + 1])
            if (path_string not in yielded and path[:len(self._path_list)] == self._path_list):
                yielded.append(path_string)
                try:
                    yield GroupPath(path=path_string, parent=self, delimiter=self._delimiter)
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

    # TODO attribute access
