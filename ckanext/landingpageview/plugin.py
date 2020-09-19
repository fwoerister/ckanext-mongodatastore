# encoding: utf-8

from logging import getLogger

import ckan.plugins as p
import ckan.plugins.toolkit as toolkit
from ckan.common import json, config
from ckan.logic import get_action

log = getLogger(__name__)
ignore_empty = p.toolkit.get_validator('ignore_empty')
natural_number_validator = p.toolkit.get_validator('natural_number_validator')
Invalid = p.toolkit.Invalid


class LandingPageView(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IResourceView, inherit=True)

    def update_config(self, config):
        '''
        Set up the resource library, public directory and
        template directory for the view
        '''
        toolkit.add_public_directory(config, 'theme/public')
        toolkit.add_template_directory(config, 'theme/templates')
        toolkit.add_resource('theme/public', 'ckanext-landingpageview')

    def can_view(self, data_dict):
        resource = data_dict['resource']
        return resource.get('format') == 'pid'

    def setup_template_variables(self, context, data_dict):
        resource = data_dict['resource']
        url_parts = resource['url'].split('/')
        pid = '/'.join(url_parts[-2:])

        result = get_action('querystore_resolve')(None, {'id': pid,
                                                         'skip': 0,
                                                         'limit': 100})

        return {'query': result['query'],
                'meta': result['meta'],
                'result_set': result['records'],
                'count': 10,
                'projection': result['fields']}

    def view_template(self, context, data_dict):
        return 'landingpageview/query_view.html'

    def info(self):
        return {
            'name': 'landingpage_view',
            'title': 'Landing Page View',
            'filterable': False,
            'icon': 'file-alt',
            'requires_datastore': False,
            'default_title': p.toolkit._('Landing Page View'),
        }
