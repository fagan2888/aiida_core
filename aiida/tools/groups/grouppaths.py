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
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import re

from aiida import orm

__all__ = ('GroupPaths',)


class GroupPaths(object):
    """A class to provide attribute/key access to AiiDA Groups with delimited labels."""

    def __init__(self, delim='/', frozen=True, path=()):
        """Attribute/key access to AiiDA Groups with delimited label.

        For example, a group stored with label 'a/b/c' would be accessed by::

            groups = GroupPaths()
            groups.a.b.c

        :param delim: delimiter to split group labels by
        :type delim: str
        :param frozen: whether to allow groups to be created/destroyed, using key methods
        :type frozen: bool
        :param path: the initial path
        :type path: tuple[str]

        """
        assert len(str(delim)) == 1, 'delimiter must be of length 1'
        assert isinstance(path, (tuple, list)), 'path must be a list or tuple'
        assert all(isinstance(p, str) for p in path), 'path must be a list or tuple of strings'
        self._delim = str(delim)
        self._frozen = frozen
        self._path = list(path)
        self._type = orm.GroupTypeString.USER.value

    def __repr__(self):
        """Representation of the object."""
        return "{}('{}{}' [type {}])".format(
            self.__class__.__name__,
            self._delim.join(self._path),
            self._delim,
            self._type,
        )

    def _iter_group_labels(self):
        """Return all group labels."""
        query = orm.QueryBuilder()
        filters = {'type_string': {'==': self._type}}
        query.append(orm.Group, filters=filters, tag='group', project='label')
        for items in query.iterall():
            yield items[0]

    def _iter_keys(self):
        """Yield next path element of delimited group labels."""
        yielded = []
        for label in self._iter_group_labels():
            path = label.split(self._delim)
            if len(path) <= len(self._path):
                continue
            key = path[len(self._path)]
            is_group = len(self._path) + 1 == len(path)
            if (key, is_group) not in yielded and path[:len(self._path)] == self._path:
                yielded.append((key, is_group))
                yield key, is_group, label

    def __iter__(self):
        """Iterate over keys, yielding a tuple (key, is_group)

        :param key: A key of the GroupPath object
        :type key: str
        :type is_group: whether the key value is an `aiida.orm.Group` or `GroupPath`
        :type is_group: bool
        """
        for key, is_group, _ in self._iter_keys():
            yield (key, is_group)

    def __contains__(self, key):
        """Test if instance contains a key."""
        for items in self._iter_keys():
            if key == items[0]:
                return True
        return False

    def __len__(self):
        """Return number of keys."""
        return len(list(self._iter_keys()))

    @staticmethod
    def _sanitize_attr(path):
        """Return a path element string, that can be used as an attribute.

        NOTE: this can cause key clashes
        """
        # TODO how to deal with key -> attribute clashes # pylint: disable=fixme
        new_path = re.sub(r'[^\_a-zA-Z0-9]', '__', path)
        if re.match(r'^[0-9]', new_path):
            # attribute can't start with a number
            new_path = 'i' + new_path
        return new_path

    def __dir__(self):
        """Return a list of available attributes."""
        return [self._sanitize_attr(p[0]) for p in self._iter_keys()]

    def __getattr__(self, attr):
        """Return the requested attribute name."""
        for next_path, is_group, label in self._iter_keys():
            if attr != self._sanitize_attr(next_path):
                continue

            if is_group:
                return orm.Group.objects.get(label=label)

            return self.__class__(delim=self._delim, frozen=self._frozen, path=self._path + [next_path])

        raise AttributeError(
            "No Group, of type '{}', exists with label starting: {}".format(
                self._type, self._delim.join(self._path + [str(attr)])
            )
        )

    def __getitem__(self, key):
        """Return the requested item key.

        Note: if the key contains delimiters, the key path will be traversed.
        """
        key_path = key.split(self._delim)
        init_key = key_path[0]
        next_keys = key_path[1:]

        for next_path, is_group, label in self._iter_keys():
            if init_key != next_path:
                continue
            # TODO how to deal with label clashes, e.g. 'a/b' and 'a/b/c' # pylint: disable=fixme
            if is_group and not next_keys:
                return orm.Group.objects.get(label=label, type_string=self._type)

            child_group = self.__class__(delim=self._delim, frozen=self._frozen, path=self._path + [next_path])
            if next_keys:
                return child_group[self._delim.join(next_keys)]
            return child_group

        raise KeyError(
            "No Group, of type '{}', exists with label starting: {}".format(
                self._type, self._delim.join(self._path + [str(key)])
            )
        )

    def __setitem__(self, key, description):
        """Set a groups description, creating the group if it does not already exist."""
        if self._frozen:
            raise KeyError('Can not create a new group with a frozen {}'.format(self.__class__.__name__))

        new_label = self._delim.join(self._path + [str(key)])
        group, _ = orm.Group.objects.get_or_create(new_label, type_string=self._type)
        # if not created:
        #     raise KeyError("The label already exists: {}".format(new_label))
        group.description = str(description)

    def __delitem__(self, key):
        """Delete an existing group."""
        if self._frozen:
            raise KeyError('Can not delete a group with a frozen {}'.format(self.__class__.__name__))
        label = self._delim.join(self._path + [str(key)])
        group = orm.Group.objects.get(label=label, type_string=self._type)
        orm.Group.objects.delete(group.id)
