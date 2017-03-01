import logging
import requests
from DivvyDb import DivvyDbObjects, Elasticsearch
from DivvyPlugins.plugin_jobs import PluginHarvester
from DivvyDb.DivvyCloudGatewayORM import DivvyCloudGatewayORM
from DivvyDb.DivvyDb import SharedSessionScope
from DivvyJobs.schedules import LazyScheduleGoal
from DivvyHistory.providers import ResourceHistoryEntry

logger = logging.getLogger('ExampleHarvest')
document_store = 'default' # Name of documentstore configuration to use

repos_url =  "https://api.github.com/orgs/DivvyCloud/repos"
api_version = {"Accept": "application/vnd.github.v3+json"}

class SkeletonHarvester(PluginHarvester):
    """ Example skeleton for a DivvyCloud harvester """

    def __init__(self, resource_location, resource_class):
        super(CloudWatchHarvester, self).__init__()
        self.current_location = resource_location
        self.sample_period_seconds = 43200
        self.resource_class = resource_class
        self._type = resource_class.get_db_class()
        self._frontend = None
        self._backend = None

    def _setup(self):
        """
        Override to perform setup operations in the worker process
        """
        super(SkeletonHarvester, self)._setup()
        # Setup cloud gw
        # self._frontend = get_cloud_type_by_organization_service_id(
        #     self.current_location.organization_service_id
        # )
        # self._backend = self._frontend.get_cloud_gw(
        #     self.current_location.region_name
        # )

    @classmethod
    def get_harvest_schedule(cls, **job_creation_kwargs):
        return LazyScheduleGoal(
            queue_name='DivvyCloudLongHarvesters',
            schedulable=schedule.Periodic(hours=1)
        )

    @classmethod
    def get_template_id(cls, **job_creation_kwargs):
        assert False  # Inheriting classes must override this method

    def repo_getter(self):
        response = requests.get(repos_url, headers=api_version)

    def make_metric(self, raw_metric):
        return GitHubRepoEntry(
            name=raw_metric.get('name', ''),
            description=raw_metric.get('description', ''),
            html_url=raw_metric.get('html_url', ''),
            timestamp=pytz.utc.localize(datetime.utcnow())
        )

    @SharedSessionScope(DivvyCloudGatewayORM)
    @EscalatePermissions()
    def do_harvest(self):
        """
        Ask libcloud to provide an updated resource history for all the
        resources in our region based on the last time we stored records
        for each instance.
        """

        es_connection = Elasticsearch.get_connection(document_store)

        logger.info('Collecting Github repo data')

        with Elasticsearch.ElasticsearchActionBatch(es_connection) as batch:
            for metric in self.metrics_getter():
                batch.update(
                    self.make_metric(metric).es_action()
                )

    def _cleanup(self):
        super(SkeletonHarvester, self)._cleanup()


class GitHubRepoEntry(ResourceHistoryEntry):
    es_index_prefix = 'divvy_metrics_'

    def __init__(self, object_id, timestamp, period, name, description, html_url):
        super(GitHubRepoEntry, self).__init__(
            resource_id=object_id, timestamp=timestamp
        )
        self.name = name
        self.description = description
        self.html_url = html_url

    @classmethod
    def get_entry_type(cls):
        return 'cloudwatch'

    def get_es_index(self):
        return Elasticsearch.get_daily_index(self.es_index_prefix, self.timestamp)

    def get_es_doc_type(self):
        assert False  # Inheriting classes must override this method

    def get_es_doc_id(self):
        return self.object_id + self.timestamp.strftime('%Y%m%d%H%M%S')

    def es_dict(self):
        # Inheriting classes should override this method
        return {
            'name': self.timestamp,
            'description': self.period,
            'html_url': self.object_id
        }

    def es_action(self):
        return {
            '_index': self.get_es_index(),
            '_type': self.get_es_doc_type(),
            '_id': self.get_es_doc_id(),
            '_source': self.es_dict()
        }
