import logging

from petl.util.base import Table

from django.db.models.query import QuerySet
from django.db.models import Model
from django.conf import settings


DEFAULTS = {
    'BULK_CREATE_CHUNK_SIZE': 50,
}


logger = logging.getLogger(__name__)
debug = logger.debug
warning = logger.warning


def fromdjango(model, queryset, fields=None, *args, **kwargs):
    assert type(queryset) == QuerySet, 'Must be supplied a Django QuerySet'
    assert issubclass(model, Model), \
        'Must be supplied a valid Django model class'

    return DjangoView(model, queryset, fields, *args, **kwargs)


class DjangoView(Table):
    def __init__(self, model, queryset, fields, *args, **kwargs):
        self.model = model
        self.queryset = queryset
        self.fields = fields
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        return _iter_django_model(self.model, self.queryset, self.fields)


def _iter_django_model(model, queryset, fields, *args, **kwargs):
    if fields is None:
        column_names = _get_model_field_names(model)
    else:
        column_names = fields
    yield column_names
    for r in queryset.all().values_list(*column_names):
        yield r


def _get_model_column_names(model):
    return [f.column for f in model._meta.fields]


def _get_model_field_names(model):
    return [f.name for f in model._meta.fields]


def todjango(table, model, update=True, create=True, use_bulk_create=True,
             *args, **kwargs):
    '''
    Given a table with appropriate headings create Django models.
    '''
    assert issubclass(model, Model), \
        'Must be supplied a valid Django model class'

    table_iterator = iter(table)
    table_headers = table_iterator.next()

    model_pk_field_name = model._meta.pk.name
    model_field_names = _get_model_field_names(model)
    model_name = model._meta.label

    if update:
        # if we are going to update existing models we need to have
        # a table field that corresponds to the model's 'pk' field.
        assert model_pk_field_name in set(model_field_names), \
            'To update existing models the data must include the ' \
            f'Django primary key field {model_pk_field_name}'

        # Load all existing instances.
        existing_models = _get_django_objects(model)
        existing_model_map = dict([(m.pk, m) for m in existing_models])
    else:
        existing_model_map = dict()

    updated_model_count = 0
    unsaved_models = []

    for row in table_iterator:
        value_map = dict(zip(table_headers, row))
        pk = value_map.get(model_pk_field_name, None)

        if (update and pk in existing_model_map):
            django_object = existing_model_map[pk]
            if _will_model_change(value_map, django_object):
                _apply_value_map(value_map, django_object)
                try:
                    django_object.save()
                except Exception as e:
                    # Add the data that caused the exception
                    # to the exception and reraise
                    e.petl_data = value_map
                    raise e
                updated_model_count += 1
        else:
            django_object = model(**value_map)
            if use_bulk_create:
                unsaved_models.append(django_object)
            else:
                try:
                    django_object.save()
                except Exception as e:
                    e.petl_data = value_map
                    raise e

    logger.debug(f'Bulk creating unsaved {model_name}')
    if use_bulk_create:
        _chunked_bulk_create(model, unsaved_models)

    logger.info(f'Updated {updated_model_count} existing {model_name}')
    logger.info(f'Created {len(unsaved_models)} new {model_name}')


def _get_django_objects(model):
    '''
    Given a Django model class get all of the current records that match.
    This is better than django's bulk methods and has no upper limit.
    '''
    model_name = model._meta.label
    model_objects = [i for i in model.objects.all()]
    logger.debug(f'Found {len(model_objects)} {model_name} objects')
    return model_objects


def _get_setting(name):
    '''
    Return a value from settings.

    If it is not in Django's settings, look in the local defaults.
    '''
    return getattr(settings, name, DEFAULTS[name])


def _chunked_bulk_create(django_model_object, unsaved_models, chunk_size=None):
    '''Create new models using bulk_create in batches of `chunk_size`.
    This is designed to overcome a query size limitation in some databases'''
    if chunk_size is None:
        chunk_size = _get_setting('BULK_CREATE_CHUNK_SIZE')
    for i in range(0, len(unsaved_models), chunk_size):
        try:
            django_model_object.objects.bulk_create(
                unsaved_models[i:i + chunk_size])
        except Exception as e:
            chunk_data = unsaved_models[i:i + chunk_size]
            e.petl_chunk_data = chunk_data
            raise e


def _will_model_change(value_map, django_model_instance):
    # I think all the attrs are utf-8 strings, possibly need to coerce
    # local user values to strings?
    for model_attr, value in value_map.items():
        if not getattr(django_model_instance, model_attr) == value:
            return True
    return False


def _apply_value_map(value_map, django_model_instance):
    for k, v in value_map.items():
        try:
            setattr(django_model_instance, k, v)
        except AttributeError:
            model_name = django_model_instance.__class__._meta.model_name
            raise UnableToApplyValueMapError(
                f'Field {k} not present in model {model_name}')
    return django_model_instance


class UnableToApplyValueMapError(Exception):
    pass
