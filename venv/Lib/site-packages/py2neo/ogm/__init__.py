#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


__all__ = [
    "Property",
    "Label",
    "Related",
    "RelatedTo",
    "RelatedFrom",
    "RelatedObjects",
    "ModelType",
    "Model", "Model",
    "ModelMatch",
    "ModelMatcher",
    "Repository",
]

from collections import OrderedDict

from english.casing import Words

from py2neo.collections import PropertyDict
from py2neo.compat import metaclass, deprecated
from py2neo.cypher import cypher_escape
from py2neo.data import Node
from py2neo.database import Graph
from py2neo.matching import NodeMatch, NodeMatcher


OUTGOING = 1
UNDIRECTED = 0
INCOMING = -1


class Property(object):
    """ Property definition for a :class:`.Model`.

    Attributes:
        key: The name of the node property within the database.
        default: The default value for the property, if it would
                 otherwise be :const:`None`.
    """

    def __init__(self, key=None, default=None):
        """ Initialise a property definition.

        Args:
            key: The name of the node property within the database. If
                 omitted, the name of the class attribute is used.
            default: The default value for the property, if it would
                     otherwise be :const:`None`.
        """
        self.key = key
        self.default = default

    def __get__(self, instance, owner):
        value = instance.__node__[self.key]
        if value is None:
            value = self.default
        return value

    def __set__(self, instance, value):
        instance.__node__[self.key] = value

    def __repr__(self):
        args = OrderedDict()
        if self.key is not None:
            args["key"] = self.key
        if self.default is not None:
            args["default"] = self.default
        return "%s(%s)" % (self.__class__.__name__,
                           ", ".join("%s=%r" % arg for arg in args.items()))


class Label(object):
    """ Label definition for a :class:`.Model`.

    Labels are toggleable tags applied to an object that can be used as type
    information or other forms of classification.
    """

    def __init__(self, name=None):
        self.name = name

    def __get__(self, instance, owner):
        return instance.__node__.has_label(self.name)

    def __set__(self, instance, value):
        if value:
            instance.__node__.add_label(self.name)
        else:
            instance.__node__.remove_label(self.name)

    def __repr__(self):
        args = OrderedDict()
        if self.name is not None:
            args["name"] = self.name
        return "%s(%s)" % (self.__class__.__name__,
                           ", ".join("%s=%r" % arg for arg in args.items()))


def _resolve_class(model, current_module_name):
    if isinstance(model, type):
        return model
    module_name, _, class_name = model.rpartition(".")
    if not module_name:
        module_name = current_module_name
    module = __import__(module_name, fromlist=".")
    return getattr(module, class_name)


class Related(object):
    """ Descriptor for a set of related objects in a :class:`.Model`.

    Attributes:
        related_class: The class of object to which these relationships
                       connect. This class is used to coerce nodes to and
                       from :class:`Model` instances.
        relationship_type: The underlying relationship type for these
                           relationships. Note that the relationship
                           type should be unique for each class of related
                           object as the `related_class` is only used for
                           object coercion and not as part of the underlying
                           database query.
    """

    direction = UNDIRECTED

    def __init__(self, related_class, relationship_type=None):
        """ Initialise a property definition.

        Args:
            related_class: The class of object to which these relationships
                           connect.
            relationship_type: The underlying relationship type for these
                               relationships.
        """
        self.related_class = related_class
        self.relationship_type = relationship_type

    def __get__(self, instance, owner):
        cls = _resolve_class(self.related_class, instance.__class__.__module__)
        return instance.__ogm__.related(self.direction, self.relationship_type, cls)


class RelatedTo(Related):
    """ Descriptor for a set of related objects for a :class:`.Model`
    that are connected by outgoing relationships.
    """

    direction = OUTGOING


class RelatedFrom(Related):
    """ Descriptor for a set of related objects for a :class:`.Model`
    that are connected by incoming relationships.
    """

    direction = INCOMING


class RelatedObjects(object):
    """ A set of similarly-typed and similarly-related objects,
    relative to a central node.
    """

    def __init__(self, subject, node, direction, relationship_type, related_class):
        assert isinstance(direction, int) and not isinstance(direction, bool)
        self.subject = subject
        self.node = node
        self.relationship_type = relationship_type
        self.related_class = related_class
        self.__related_objects = None
        if direction > 0:
            self.__match_args = {"nodes": (self.node, None), "r_type": relationship_type}
            self.__start_node = False
            self.__end_node = True
            self.__relationship_pattern = "(a)-[_:%s]->(b)" % cypher_escape(relationship_type)
        elif direction < 0:
            self.__match_args = {"nodes": (None, self.node), "r_type": relationship_type}
            self.__start_node = True
            self.__end_node = False
            self.__relationship_pattern = "(a)<-[_:%s]-(b)" % cypher_escape(relationship_type)
        else:
            self.__match_args = {"nodes": {self.node, None}, "r_type": relationship_type}
            self.__start_node = True
            self.__end_node = True
            self.__relationship_pattern = "(a)-[_:%s]-(b)" % cypher_escape(relationship_type)

    def __iter__(self):
        for obj, _ in self._related_objects:
            yield obj

    def __len__(self):
        return len(self._related_objects)

    def __contains__(self, obj):
        if not isinstance(obj, Model):
            raise TypeError("Related objects must be Model instances")
        for related_object, _ in self._related_objects:
            if related_object == obj:
                return True
        return False

    @property
    def _related_objects(self):
        if self.__related_objects is None:
            self.__related_objects = []
            if self.node.graph:
                self.node.graph.update(lambda tx: self.__db_pull__(tx))
        return self.__related_objects

    def triples(self):
        """ Iterate through the related objects, yielding for each a
        triple of `(subject, (type, properties), object)`. This is the
        easiest way to see the full details of the underlying
        relationships.

            >>> from py2neo.ogm import *
            >>> from py2neo.ogm.models.movies import *
            >>> repo = Repository()
            >>> keanu = repo.match(Person).where(name="Keanu Reeves").first()
            >>> list(keanu.acted_in.triples())
            [(<Person name='Keanu Reeves'>, ('ACTED_IN', {'roles': ['Julian Mercer']}), <Movie title="Something's Gotta Give">),
             (<Person name='Keanu Reeves'>, ('ACTED_IN', {'roles': ['Shane Falco']}), <Movie title='The Replacements'>),
             (<Person name='Keanu Reeves'>, ('ACTED_IN', {'roles': ['Johnny Mnemonic']}), <Movie title='Johnny Mnemonic'>),
             (<Person name='Keanu Reeves'>, ('ACTED_IN', {'roles': ['Kevin Lomax']}), <Movie title="The Devil's Advocate">),
             (<Person name='Keanu Reeves'>, ('ACTED_IN', {'roles': ['Neo']}), <Movie title='The Matrix Revolutions'>),
             (<Person name='Keanu Reeves'>, ('ACTED_IN', {'roles': ['Neo']}), <Movie title='The Matrix Reloaded'>),
             (<Person name='Keanu Reeves'>, ('ACTED_IN', {'roles': ['Neo']}), <Movie title='The Matrix'>)]

        """
        for obj, properties in self._related_objects:
            yield self.subject, (self.relationship_type, properties), obj

    def add(self, obj, properties=None, **kwproperties):
        """ Add or update a related object.

        :param obj: the :py:class:`.Model` to relate
        :param properties: dictionary of properties to attach to the relationship (optional)
        :param kwproperties: additional keyword properties (optional)
        """
        if not isinstance(obj, Model):
            raise TypeError("Related objects must be Model instances")
        related_objects = self._related_objects
        properties = dict(properties or {}, **kwproperties)
        added = False
        for i, (related_object, p) in enumerate(related_objects):
            if related_object == obj:
                related_objects[i] = (obj, PropertyDict(p, **properties))
                added = True
        if not added:
            related_objects.append((obj, properties))

    def clear(self):
        """ Remove all related objects from this set.
        """
        self._related_objects[:] = []

    def get(self, obj, key, default=None):
        """ Return a relationship property associated with a specific related object.

        :param obj: related object
        :param key: relationship property key
        :param default: default value, in case the key is not found
        :return: property value
        """
        if not isinstance(obj, Model):
            raise TypeError("Related objects must be Model instances")
        for related_object, properties in self._related_objects:
            if related_object == obj:
                return properties.get(key, default)
        return default

    def remove(self, obj):
        """ Remove a related object.

        :param obj: the :py:class:`.Model` to separate
        """
        if not isinstance(obj, Model):
            raise TypeError("Related objects must be Model instances")
        related_objects = self._related_objects
        related_objects[:] = [(related_object, properties)
                              for related_object, properties in related_objects
                              if related_object != obj]

    @deprecated("RelatedObjects.update is deprecated, "
                "please use RelatedObjects.add instead")
    def update(self, obj, properties=None, **kwproperties):
        """ Add or update a related object.

        :param obj: the :py:class:`.Model` to relate
        :param properties: dictionary of properties to attach to the relationship (optional)
        :param kwproperties: additional keyword properties (optional)
        """
        self.add(obj, properties, **kwproperties)

    def __db_pull__(self, tx):
        related_objects = {}
        for r in tx.graph.match(**self.__match_args):
            nodes = []
            n = self.node
            a = r.start_node
            b = r.end_node
            if a == b:
                nodes.append(a)
            else:
                if self.__start_node and a != n:
                    nodes.append(r.start_node)
                if self.__end_node and b != n:
                    nodes.append(r.end_node)
            for node in nodes:
                related_object = self.related_class.wrap(node)
                related_objects[node] = (related_object, PropertyDict(r))
        self._related_objects[:] = related_objects.values()

    def __db_push__(self, tx):
        related_objects = self._related_objects
        # 1. merge all nodes (create ones that don't)
        for related_object, _ in related_objects:
            tx.merge(related_object)
        # 2a. remove any relationships not in list of nodes
        subject_id = self.node.identity
        tx.run("MATCH %s WHERE id(a) = $x AND NOT id(b) IN $y DELETE _" % self.__relationship_pattern,
               x=subject_id, y=[obj.__node__.identity for obj, _ in related_objects])
        # 2b. merge all relationships
        for related_object, properties in related_objects:
            tx.run("MATCH (a) WHERE id(a) = $x MATCH (b) WHERE id(b) = $y "
                   "MERGE %s SET _ = $z" % self.__relationship_pattern,
                   x=subject_id, y=related_object.__node__.identity, z=properties)


class OGM(object):

    def __init__(self, subject, node):
        self.subject = subject
        self.node = node
        self._related = {}

    def all_related(self):
        """ Return an iterator through all :class:`.RelatedObjects`.
        """
        return iter(self._related.values())

    def related(self, direction, relationship_type, related_class):
        """ Return :class:`.RelatedObjects` for given criteria.
        """
        key = (direction, relationship_type)
        if key not in self._related:
            self._related[key] = RelatedObjects(self.subject, self.node, direction,
                                                relationship_type, related_class)
        return self._related[key]


class ModelType(type):

    def __new__(mcs, name, bases, attributes):
        for attr_name, attr in list(attributes.items()):
            if isinstance(attr, Property):
                if attr.key is None:
                    attr.key = attr_name
                if attr.__doc__ is attr.__class__.__doc__:
                    attr.__doc__ = repr(attr)
            elif isinstance(attr, Label):
                if attr.name is None:
                    attr.name = Words(attr_name).camel(upper_first=True)
                if attr.__doc__ is attr.__class__.__doc__:
                    attr.__doc__ = repr(attr)
            elif isinstance(attr, Related):
                if attr.relationship_type is None:
                    attr.relationship_type = Words(attr_name).upper("_")
                if attr.__doc__ is attr.__class__.__doc__:

                    def related_repr(obj):
                        try:
                            args = ":class:`%s`" % obj.related_class.__qualname__
                        except AttributeError:
                            args = ":class:`.%s`" % obj.related_class
                        if obj.relationship_type is not None:
                            args += ", relationship_type=%r" % obj.relationship_type
                        return "%s(%s)" % (obj.__class__.__name__, args)

                    attr.__doc__ = related_repr(attr)

        attributes.setdefault("__primarylabel__", name)

        primary_key = attributes.get("__primarykey__")
        if primary_key is None:
            for base in bases:
                if primary_key is None and hasattr(base, "__primarykey__"):
                    primary_key = getattr(base, "__primarykey__")
                    break
            else:
                primary_key = "__id__"
            attributes["__primarykey__"] = primary_key

        return super(ModelType, mcs).__new__(mcs, name, bases, attributes)


@metaclass(ModelType)
class Model(object):
    """ The base class for all OGM object classes.

    *Changed in 2020.0: this used to be called GraphObject, but was
    renamed to avoid ambiguity. The old name is still available as an
    alias.*
    """

    __primarylabel__ = None
    __primarykey__ = None

    __ogm = None

    def __eq__(self, other):
        if self is other:
            return True
        try:
            self_node = self.__node__
            other_node = other.__node__
            if any(x is None for x in [self_node.graph, other_node.graph, self_node.identity, other_node.identity]):
                if self.__primarylabel__ != other.__primarylabel__:
                    return False
                if (self.__primarykey__ == other.__primarykey__ and
                        self.__primaryvalue__ == other.__primaryvalue__):
                    if self.__primarykey__ == "__id__" and self.__primaryvalue__ is None:
                        # If __id__ is the primary key but the value
                        # isn't yet set, assume the objects are
                        # not equal.
                        #
                        # See https://github.com/technige/py2neo/issues/839
                        #
                        return False
                    else:
                        return True
                else:
                    return False
            return (type(self) is type(other) and
                    self_node.graph == other_node.graph and
                    self_node.identity == other_node.identity)
        except (AttributeError, TypeError):
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def __ogm__(self):
        if self.__ogm is None:
            self.__ogm = OGM(self, Node(self.__primarylabel__))
        node = self.__ogm.node
        if not hasattr(node, "__primarylabel__"):
            setattr(node, "__primarylabel__", self.__primarylabel__)
        if not hasattr(node, "__primarykey__"):
            setattr(node, "__primarykey__", self.__primarykey__)
        return self.__ogm

    @classmethod
    def wrap(cls, node):
        """ Convert a :class:`.Node` into a :class:`.Model`.

        :param node:
        :return:
        """
        if node is None:
            return None
        inst = Model()
        inst.__ogm = OGM(inst, node)
        inst.__class__ = cls
        return inst

    @classmethod
    def match(cls, repository, primary_value=None):
        """ Select one or more nodes from the database, wrapped as instances of this class.

        :param repository: the :class:`.Repository` in which to match
        :param primary_value: value of the primary property (optional)
        :rtype: :class:`.ModelMatch`
        """
        return ModelMatcher(cls, repository).match(primary_value)

    def __repr__(self):
        return "<%s %s=%r>" % (self.__class__.__name__, self.__primarykey__, self.__primaryvalue__)

    @property
    def __primaryvalue__(self):
        node = self.__node__
        primary_key = self.__primarykey__
        if primary_key == "__id__":
            return node.identity
        else:
            return node[primary_key]

    @property
    def __node__(self):
        """ The :class:`.Node` wrapped by this :class:`.Model`.
        """
        return self.__ogm__.node

    def __db_create__(self, tx):
        self.__db_merge__(tx)

    def __db_delete__(self, tx):
        ogm = self.__ogm__
        tx.delete(ogm.node)
        for related_objects in ogm.all_related():
            related_objects.clear()

    def __db_exists__(self, tx):
        return tx.exists(self.__node__)

    def __db_merge__(self, tx, primary_label=None, primary_key=None):
        ogm = self.__ogm__
        node = ogm.node
        if primary_label is None:
            primary_label = getattr(node, "__primarylabel__", None)
        if primary_key is None:
            primary_key = getattr(node, "__primarykey__", "__id__")
        if node.graph is None:
            if primary_key == "__id__":
                node.add_label(primary_label)
                tx.create(node)
            else:
                tx.merge(node, primary_label, primary_key)
            for related_objects in ogm.all_related():
                related_objects.__db_push__(tx)

    def __db_pull__(self, tx):
        ogm = self.__ogm__
        if ogm.node.graph is None:
            matcher = ModelMatcher(self.__class__, tx.graph)
            matcher._match_class = NodeMatch
            ogm.node = matcher.match(self.__primaryvalue__).first()
        tx.pull(ogm.node)
        for related_objects in ogm.all_related():
            related_objects.__db_pull__(tx)

    def __db_push__(self, tx):
        ogm = self.__ogm__
        node = ogm.node
        if node.graph is not None:
            tx.push(node)
        else:
            primary_key = getattr(node, "__primarykey__", "__id__")
            if primary_key == "__id__":
                tx.create(node)
            else:
                tx.merge(node)
        for related_objects in ogm.all_related():
            related_objects.__db_push__(tx)


# Alias for backward compatibility
GraphObject = Model


class ModelMatch(NodeMatch):
    """ A selection of :class:`.Model` instances that match a
    given set of criteria.
    """

    _object_class = Model

    def __iter__(self):
        """ Iterate through items drawn from the underlying repository
        that match the given criteria.
        """
        wrap = self._object_class.wrap
        for node in super(ModelMatch, self).__iter__():
            yield wrap(node)

    def first(self):
        """ Return the first item that matches the given criteria.
        """
        return self._object_class.wrap(super(ModelMatch, self).first())


class ModelMatcher(NodeMatcher):

    _match_class = ModelMatch

    @classmethod
    def _coerce_to_graph(cls, obj):
        if isinstance(obj, Repository):
            return obj.graph
        elif isinstance(obj, Graph):
            return obj
        else:
            raise TypeError("Cannot coerce object %r to Graph" % obj)

    def __init__(self, object_class, repository):
        NodeMatcher.__init__(self, self._coerce_to_graph(repository))
        self._object_class = object_class
        self._match_class = type("%sMatch" % self._object_class.__name__,
                                 (ModelMatch,), {"_object_class": object_class})

    def match(self, primary_value=None):
        cls = self._object_class
        if cls.__primarykey__ == "__id__":
            match = NodeMatcher.match(self, cls.__primarylabel__)
            if primary_value is not None:
                match = match.where("id(_) = %d" % primary_value)
        elif primary_value is None:
            match = NodeMatcher.match(self, cls.__primarylabel__)
        else:
            match = NodeMatcher.match(self, cls.__primarylabel__).where(**{cls.__primarykey__: primary_value})
        return match


class Repository(object):
    """ Storage container for :class:`.Model` instances.

    The constructor for this class has an identical signature to that
    for the :class:`.Graph` class. For example::

        >>> from py2neo.ogm import Repository
        >>> from py2neo.ogm.models.movies import Movie
        >>> repo = Repository("bolt://neo4j@localhost:7687", password="password")
        >>> repo.match(Movie, "The Matrix").first()
        <Movie title='The Matrix'>

    *New in version 2020.0. In earlier versions, a :class:`.Graph` was
    required to co-ordinate all reads and writes to the remote
    database. This class completely replaces that, removing the need
    to import from any other packages when using OGM.*
    """

    @classmethod
    def wrap(cls, graph):
        """ Wrap an existing :class:`.Graph` object as a
        :class:`.Repository`.
        """
        obj = object.__new__(Repository)
        obj.graph = graph
        return obj

    def __init__(self, profile=None, name=None, **settings):
        self.graph = Graph(profile, name=name, **settings)

    def __repr__(self):
        return "<Repository profile=%r>" % (self.graph.service.profile,)

    def reload(self, obj):
        """ Reload data from the remote graph into the local object.
        """
        self.graph.pull(obj)

    def save(self, *objects):
        """ Save data from the local object into the remote graph.
        """

        def push_all(tx):
            for obj in objects:
                tx.push(obj)

        self.graph.update(push_all)

    def delete(self, obj):
        """ Delete the object in the remote graph.
        """
        self.graph.delete(obj)

    def exists(self, obj):
        """ Check whether the object exists in the remote graph.
        """
        return self.graph.exists(obj)

    def match(self, model, primary_value=None):
        """ Select one or more objects from the remote graph.

        :param model: the :class:`.Model` subclass to match
        :param primary_value: value of the primary property (optional)
        :rtype: :class:`.ModelMatch`
        """
        return ModelMatcher(model, self).match(primary_value)

    def get(self, model, primary_value=None):
        """ Match and return a single object from the remote graph.

        :param model: the :class:`.Model` subclass to match
        :param primary_value: value of the primary property (optional)
        :rtype: :class:`.Model`
        """
        return self.match(model, primary_value).first()

    @deprecated("Repository.create is a compatibility alias, "
                "please use Repository.save instead")
    def create(self, obj):
        self.graph.create(obj)

    @deprecated("Repository.merge is a compatibility alias, "
                "please use Repository.save instead")
    def merge(self, obj):
        self.graph.merge(obj)

    @deprecated("Repository.pull is a compatibility alias, "
                "please use Repository.load instead")
    def pull(self, obj):
        self.graph.pull(obj)

    @deprecated("Repository.push is a compatibility alias, "
                "please use Repository.save instead")
    def push(self, obj):
        self.graph.push(obj)
