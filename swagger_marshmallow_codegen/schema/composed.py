from collections import OrderedDict
from marshmallow import Schema, ValidationError, SchemaOpts
from marshmallow import marshalling
from prestring.utils import reify


class ComposedOpts(SchemaOpts):
    def __init__(self, meta, **kwargs):
        super().__init__(meta, **kwargs)

        self.schema_classes = getattr(meta, "schema_classes", ())
        if not isinstance(self.schema_classes, (tuple, list)):
            raise ValueError("`schema_classes` must be a list or tuple.")

        self.discriminator = getattr(meta, "discriminator", None)
        if self.discriminator is not None:
            if "fieldname" not in self.discriminator:
                raise ValueError("`discriminator` must need `fieldname` value")


def _detect_include_self(schema_classes):
    include_self = False
    new_schema_classes = []
    for c in schema_classes:
        if c == "self":
            include_self = True
            continue
        new_schema_classes.append(c)
    return new_schema_classes, include_self


class OneOfSchema(Schema):
    OPTIONS_CLASS = ComposedOpts
    schema_classes = None
    include_self = None

    def __init__(self, *args, **kwargs):
        strict = kwargs.pop("strict", None)
        many = kwargs.pop("many", None)

        # xxx: class level
        if self.__class__.include_self is None:
            cls = self.__class__
            cls.schema_classes, cls.include_self = _detect_include_self(cls.schema_classes)

        schema_classes = self.opts.schema_classes or self.__class__.schema_classes

        self.schemas = [cls(*args, strict=False, many=False, **kwargs) for cls in schema_classes]
        super().__init__(strict=strict, many=many)

        finder = SchemaFinder(self.schemas, self.opts.discriminator)
        self._marshal = ComposedMarshaller(
            self._marshal, finder, self.final_check, include_self=self.__class__.include_self
        )
        self._unmarshal = ComposedUnmarshaller(
            self._unmarshal, finder, self.final_check, include_self=self.__class__.include_self
        )

    def final_check(self, *, data, schemas, results, errors, compacted):
        if len(compacted) == 1:
            return compacted[0], {}
        elif len(compacted) > 1:
            for other in compacted[1:]:
                compacted[0].update(other)

            satisfied = []
            for i, err in enumerate(errors):
                if not err:
                    satisfied.append(schemas[i].__class__.__name__)
            return compacted[0], {
                "_schema": ["satisfied both of {}, not only one".format(satisfied)]
            }
        else:
            for other in results[1:]:
                results[0].update(other)
            candidates = [s.__class__.__name__ for s in self.schemas]
            if self.__class__.include_self:
                candidates.append("self")
            return data if not results else results[0], {
                "_schema": ["not matched, any of {}".format(candidates)],
            }


class AnyOfSchema(Schema):
    OPTIONS_CLASS = ComposedOpts
    schema_classes = None

    def __init__(self, *args, **kwargs):
        strict = kwargs.pop("strict", None)
        many = kwargs.pop("many", None)
        schema_classes = self.opts.schema_classes or self.__class__.schema_classes
        self.schemas = [cls(*args, strict=False, many=False, **kwargs) for cls in schema_classes]
        super().__init__(strict=strict, many=many)

        finder = SchemaFinder(self.schemas, self.opts.discriminator)
        self._marshal = ComposedMarshaller(self._marshal, finder, self.final_check)
        self._unmarshal = ComposedUnmarshaller(self._unmarshal, finder, self.final_check)

    def final_check(self, *, data, schemas, results, errors, compacted):
        if len(compacted) >= 1:
            for other in compacted[1:]:
                compacted[0].update(other)
            return compacted[0], {}
        else:
            for other in results[1:]:
                results[0].update(other)
            return data if not results else results[0], {
                "_schema":
                ["not matched, any of {}".format([s.__class__.__name__ for s in self.schemas])]
            }


def run_many(data, fn, **kwargs):
    results = []
    errors = {}
    for i, d in enumerate(data):
        r, err = fn(d, **kwargs)
        results.append(r)
        if err:
            errors[i] = err
    return results, errors


class SchemaFinder:
    def __init__(self, schemas, discriminator):
        self.schemas = schemas
        if discriminator is None:
            self.discriminator_name = None
            self.discriminator_mapping = None
        else:
            self.discriminator_name = discriminator["fieldname"]
            if "mapping" not in discriminator:
                self.discriminator_mapping = {s.__class__.__name__: s for s in schemas}
            else:
                mapping = {s.__class__: s for s in schemas}
                self.discriminator_mapping = {
                    name: mapping[cls]
                    for name, cls in discriminator["mapping"].items()
                }

    @reify
    def signature_mapping(self):
        d = OrderedDict()
        for s in self.schemas:
            sig = tuple(sorted([name for name, f in s.fields.items() if f.required]))
            d[sig] = s
        return d

    def find_matched_schemas(self, data):
        if self.discriminator_name is not None:
            return (self.discriminator_mapping[data[self.discriminator_name]], )

        r = []
        for signature, s in self.signature_mapping.items():
            if all(k in data for k in signature):
                r.append(s)
        return r

    def find_schemas(self, data):
        if self.discriminator_name is not None:
            return (self.discriminator_mapping[data[self.discriminator_name]], )
        return self.schemas


class ComposedMarshaller(marshalling.Marshaller):
    def __init__(self, marshaller, finder, final_check, *, include_self=False):
        super().__init__()
        self._marshaller = marshaller
        self.finder = finder
        self.final_check = final_check
        self.include_self = include_self

    def marshall(self, obj, fields_dict, *, many, accessor, dict_class, index_errors, index=None):
        self.reset_errors()
        self_errors = None
        try:
            result = self._marshaller(
                obj,
                fields_dict,
                many=many,
                accessor=accessor,
                dict_class=dict_class,
                index_errors=index_errors,
                index=index,
            )
        except ValidationError as err:
            self_errors = self._marshaller.errors
            result = err.data

        if many:
            d, errors = run_many(obj, self._marshall_one)
            for i in range(len(result)):
                d[i].update(result[i])
        else:
            d, errors = self._marshall_one(obj)
            d.update(result)

        if not self.include_self:
            if self_errors is not None:
                errors.update(self_errors)
            self.errors = errors
        else:
            self.errors = _merge_errors_for_include_self(
                errors, self_errors, many=many, size=len(result) if many else 1
            )
        if self.errors:
            raise ValidationError(self.errors, data=d)
        return d

    __call__ = marshall

    def _marshall_one(self, data):
        results = []
        errors = []
        compacted = []
        schemas = self.finder.find_matched_schemas(data)
        for s in schemas:
            r, err = s.dump(data, update_fields=False)
            if not err:
                compacted.append(r)
            results.append(r)
            errors.append(err)
        return self.final_check(
            data=data, results=results, errors=errors, schemas=schemas, compacted=compacted
        )


class ComposedUnmarshaller(marshalling.Unmarshaller):
    def __init__(self, unmarshaller, finder, final_check, *, include_self=False):
        super().__init__()
        self._unmarshaller = unmarshaller
        self.finder = finder
        self.final_check = final_check
        self.include_self = include_self

    def unmarshall(self, data, fields, *, many, partial, dict_class, index_errors):
        self.reset_errors()
        self_errors = None
        try:
            result = self._unmarshaller(
                data,
                fields,
                many=many,
                partial=partial,
                dict_class=dict_class,
                index_errors=index_errors,
            )
        except ValidationError as err:
            self_errors = self._unmarshaller.errors
            result = err.data

        if many:
            d, errors = run_many(data, self._unmarshall_one, partial=partial)
            for i in range(len(result)):
                d[i].update(result[i])
        else:
            d, errors = self._unmarshall_one(data, partial=partial)
            d.update(result)

        if not self.include_self:
            if self_errors is not None:
                errors.update(self_errors)
            self.errors = errors
        else:
            self.errors = _merge_errors_for_include_self(
                errors, self_errors, many=many, size=len(result) if many else 1
            )
        return d

    __call__ = unmarshall

    def _unmarshall_one(self, data, *, partial):
        results = []
        errors = []
        compacted = []
        schemas = self.finder.find_schemas(data)
        for s in schemas:
            r, err = s.load(data, partial=partial)
            if not err:
                compacted.append(r)
            results.append(r)
            errors.append(err)
        return self.final_check(
            data=data, results=results, errors=errors, schemas=schemas, compacted=compacted
        )


def _merge_errors_for_include_self(errors, self_errors, *, many, size):
    if many:
        for i in range(size):
            if i in errors:
                errors[i] = _merge_errors_one_for_include_self(
                    errors[i],
                    self_errors if self_errors is None else self_errors.get(i),
                )
        return errors
    else:
        return _merge_errors_one_for_include_self(errors, self_errors)


def _merge_errors_one_for_include_self(errors, self_errors):
    if self_errors is not None:
        if not all(msg.startswith("not matched, any of") for msg in errors.get("_schema") or []):
            errors.update(self_errors)
    else:
        if not errors:
            errors.update(_schema=["satisfied both of self and others, not only one"])
        elif all(msg.startswith("not matched, any of") for msg in errors.get("_schema") or []):
            errors.pop("_schema")
    return errors
