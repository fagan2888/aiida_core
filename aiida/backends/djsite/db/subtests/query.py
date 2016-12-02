# -*- coding: utf-8 -*-
from aiida.backends.djsite.db.testbase import AiidaTestCase
from aiida.backends.tests.query import TestQueryBuilder



__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__authors__ = "The AiiDA team."
__version__ = "0.7.0"

class TestQueryBuilderDjango(AiidaTestCase, TestQueryBuilder):


    def test_clsf_django(self):
        """
        This tests the classifications of the QueryBuilder u. the django backend.
        """
        from aiida.backends.querybuild.dummy_model import (
            DbNode, DbUser, DbComputer,
            DbGroup,
        )
        from aiida.orm.querybuilder import QueryBuilder
        from aiida.orm.utils import (DataFactory, CalculationFactory)
        from aiida.orm.data.structure import StructureData
        from aiida.orm.implementation.django.node import Node
        from aiida.orm import Group, User, Node, Computer, Data, Calculation
        from aiida.common.exceptions import InputValidationError
        qb = QueryBuilder()
        
        with self.assertRaises(InputValidationError):
            qb._get_ormclass(None, 'data')
        with self.assertRaises(InputValidationError):
            qb._get_ormclass(None, 'data.Data')
        with self.assertRaises(InputValidationError):
            qb._get_ormclass(None, '.')

        for cls, clstype, query_type_string in (
                qb._get_ormclass(StructureData, None),
                qb._get_ormclass(None, 'data.structure.StructureData.'),
        ):
            self.assertEqual(clstype, 'data.structure.StructureData.')
            self.assertTrue(issubclass(cls, DbNode))
            self.assertEqual(clstype, 'data.structure.StructureData.')
            self.assertEqual(query_type_string,
                             StructureData._query_type_string)

        for cls, clstype, query_type_string in (
                qb._get_ormclass(Node, None),
                qb._get_ormclass(DbNode, None),
                qb._get_ormclass(None, '')
        ):
            self.assertEqual(clstype, Node._plugin_type_string)
            self.assertEqual(query_type_string, Node._query_type_string)
            self.assertTrue(issubclass(cls, DbNode))

        for cls, clstype, query_type_string in (
                qb._get_ormclass(DbGroup, None),
                qb._get_ormclass(Group, None),
                qb._get_ormclass(None, 'group'),
                qb._get_ormclass(None, 'Group'),
        ):
            self.assertEqual(clstype, 'group')
            self.assertEqual(query_type_string, None)
            self.assertTrue(issubclass(cls, DbGroup))

        for cls, clstype, query_type_string in (
                qb._get_ormclass(DbUser, None),
                qb._get_ormclass(DbUser, None),
                qb._get_ormclass(None, "user"),
                qb._get_ormclass(None, "User"),
        ):
            self.assertEqual(clstype, 'user')
            self.assertEqual(query_type_string, None)
            self.assertTrue(issubclass(cls, DbUser))

        for cls, clstype, query_type_string in (
                qb._get_ormclass(DbComputer, None),
                qb._get_ormclass(Computer, None),
                qb._get_ormclass(None, 'computer'),
                qb._get_ormclass(None, 'Computer'),
        ):
            self.assertEqual(clstype, 'computer')
            self.assertEqual(query_type_string, None)
            self.assertTrue(issubclass(cls, DbComputer))

        for cls, clstype, query_type_string in (
                qb._get_ormclass(Data, None),
                qb._get_ormclass(None, 'data.Data.'),
        ):
            self.assertEqual(clstype, Data._plugin_type_string)
            self.assertEqual(query_type_string, Data._query_type_string)
            self.assertTrue(issubclass(cls, DbNode))