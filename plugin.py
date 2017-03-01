"""
Main plugin module for CPU Report plugin
"""
from DivvyPlugins.plugin_metadata import PluginMetadata
from plugins.example_harvester.ExampleHarvester import SkeletonHarvest  # pylint: disable=F0401


class metadata(PluginMetadata):
    """
    Information about this plugin
    """
    version = '1.0'
    last_updated_date = '2015-08-05'
    author = 'DivvyCloud Inc.'
    nickname = 'DivvyCloud Github Repos'
    default_language_description = 'Fetches DivvyCloud Github repos.'
    support_email = 'support@divvycloud.com'
    support_url = 'http://support.divvycloud.com'
    main_url = 'http://www.divvycloud.com'
    category = 'Reports'
    managed = True

