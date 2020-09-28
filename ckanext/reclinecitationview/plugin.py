# encoding: utf-8

from logging import getLogger

import ckan.plugins as p
import ckan.plugins.toolkit as toolkit
from ckan.common import json, config

log = getLogger(__name__)
ignore_empty = p.toolkit.get_validator('ignore_empty')
natural_number_validator = p.toolkit.get_validator('natural_number_validator')
Invalid = p.toolkit.Invalid


def get_mapview_config():
    '''
    Extracts and returns map view configuration of the reclineview extension.
    '''
    namespace = 'ckanext.spatial.common_map.'
    return dict([(k.replace(namespace, ''), v) for k, v in config.items()
                 if k.startswith(namespace)])


def get_dataproxy_url():
    '''
    Returns the value of the ckan.recline.dataproxy_url config option
    '''
    return config.get(
        'ckan.recline.dataproxy_url', '//jsonpdataproxy.appspot.com')


def in_list(list_possible_values):
    '''
    Validator that checks that the input value is one of the given
    possible values.

    :param list_possible_values: function that returns list of possible values
        for validated field
    :type possible_values: function
    '''

    def validate(key, data, errors, context):
        if not data[key] in list_possible_values():
            raise Invalid('"{0}" is not a valid parameter'.format(data[key]))

    return validate


def datastore_fields(resource, valid_field_types):
    '''
    Return a list of all datastore fields for a given resource, as long as
    the datastore field type is in valid_field_types.

    :param resource: resource dict
    :type resource: dict
    :param valid_field_types: field types to include in returned list
    :type valid_field_types: list of strings
    '''
    data = {'resource_id': resource['id'], 'limit': 0}
    fields = toolkit.get_action('datastore_search')({}, data)['fields']
    return [{'value': f['id'], 'text': f['id']} for f in fields
            if f['type'] in valid_field_types]


class ReclineCitationViewBase(p.SingletonPlugin):
    '''
    This base class for the Recline view extensions.
    '''
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IResourceView, inherit=True)
    p.implements(p.ITemplateHelpers, inherit=True)

    def update_config(self, config):
        '''
        Set up the resource library, public directory and
        template directory for the view
        '''
        toolkit.add_public_directory(config, 'theme/public')
        toolkit.add_template_directory(config, 'theme/templates')
        toolkit.add_resource('theme/public', 'ckanext-reclineview')

    def can_view(self, data_dict):
        resource = data_dict['resource']
        return (resource.get('datastore_active') or
                '_datastore_only_resource' in resource.get('url', ''))

    def setup_template_variables(self, context, data_dict):
        return {'resource_json': json.dumps(data_dict['resource']),
                'resource_view_json': json.dumps(data_dict['resource_view'])}

    def view_template(self, context, data_dict):
        return 'recline_view.html'

    def get_helpers(self):
        return {
            'get_map_config': get_mapview_config,
            'get_dataproxy_url': get_dataproxy_url,
        }


class ReclineCitationView(ReclineCitationViewBase):
    '''
    This extension views resources using a Recline MultiView.
    '''

    def info(self):
        return {'name': 'reclinecitation_view',
                'title': 'Data Explorer',
                'filterable': True,
                'icon': 'table',
                'requires_datastore': False,
                'default_title': p.toolkit._('Data Explorer'),
                }

    def can_view(self, data_dict):
        resource = data_dict['resource']

        if (resource.get('datastore_active') or
                '_datastore_only_resource' in resource.get('url', '')):
            return True
        resource_format = resource.get('format', None)
        if resource_format:
            return resource_format.lower() in ['csv', 'xls', 'xlsx', 'tsv']
        else:
            return False
