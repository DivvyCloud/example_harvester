import logging
import requests
from datetime import datetime
from DivvyDb import DivvyDbObjects, Elasticsearch
from DivvyPlugins.plugin_jobs import PluginHarvester
from DivvyDb.DivvyCloudGatewayORM import DivvyCloudGatewayORM
from DivvyDb.DivvyDb import SharedSessionScope
from DivvyJobs.schedules import LazyScheduleGoal
from DivvyHistory.providers import ResourceHistoryEntry
from DivvySession.DivvySession import EscalatePermissions
from DivvyPlugins.plugin_helpers import register_job_module, unregister_job_module
from DivvyUtils import schedule

logger = logging.getLogger('ExampleHarvest')
document_store = 'default' # Name of documentstore configuration to use

repos_url =  "https://api.github.com/orgs/DivvyCloud/repos"
api_version = {"Accept": "application/vnd.github.v3+json"}

class SkeletonHarvester(PluginHarvester):
    """ Example skeleton for a DivvyCloud harvester

    This class inherits from PluginHarvester which implements
    a base method interface for executing a harvest job. The
    lifecycle of a job consists of:

    0) JobScheduler loading schedule interval via `get_harvest_schedule/2` and `get_template_id/2`
    1) job._setup/1
    2) job.do_harvest/1
    3) job._cleanup/1

    Python allows for additional methods to be added to the class which
    can be a convenient way to call business logic. See `repo_getter/1` and
    `make_metric/1` which abstract the read and write logic. If you wanted
    to define your own base class extension, adding methods like this which
    return None/False could be overwritten by a child class to allow for a
    single implementation of `do_harvest/1` while extending the read/write
    behavior over multiple use cases.
    """

    def __init__(self):
        super(SkeletonHarvester, self).__init__()

    def _setup(self):
        """
        Override to perform setup operations in the worker process
        """
        super(SkeletonHarvester, self)._setup()
        """
        Internally DivvyCloud uses this to initialize the generic
        interface for multiple cloud backends.

        # For example: Setup cloud gw

        self._frontend = get_cloud_type_by_organization_service_id(
            self.current_location.organization_service_id
        )
        self._backend = self._frontend.get_cloud_gw(
            self.current_location.region_name
        )

        """

    @classmethod
    def get_harvest_schedule(cls, **job_creation_kwargs):
        """ Sets frequency and worker queue """
        return LazyScheduleGoal(
            queue_name='DivvyCloudHarvest',
            schedulable=schedule.Periodic(hours=1)
        )

    @classmethod
    def get_template_id(cls, **job_creation_kwargs):
        """ This provides a unique name to the job """
        return 'github-repo-harvest'

    def repo_getter(self):
        """ My custom method for talking to Github """
        response = requests.get(repos_url, headers=api_version)
        return response.json()

    def make_metric(self, raw_metric):
        """ My custom method for formatting an ElasticSearch entry

        This implementation creates a single index for all repos.

        If you wanted to make a daily index to track repos over time. Simply change the `_index`
        to something like `github_ + datetime.utcnow().strftime('%Y%m%d%H%M%S')`
        """
        return {
            '_index': 'github', # ES will create if doesn't exist
            '_type': 'github_repo', # ES will auto analyze and infer document type. Document schema can be manually added to ES.
            '_id': raw_metric.get('full_name'), # Unique Id for document, here I'm using the fully qualified repo name.
            '_source': { # Document payload
                'name': raw_metric.get('name'),
                'description': raw_metric.get('description', ''),
                'html_url': raw_metric.get('html_url')
            }
        }

    @SharedSessionScope(DivvyCloudGatewayORM)
    @EscalatePermissions()
    def do_harvest(self):
        """ Do the harvest

        While not used, the `@SharedSessionScope(DivvyCloudGatewayORM)` decorator
        is require anytime you need to access the MySQL database within a job context.

        Acquire a connection like so; `db = DivvyCloudGatewayORM()` which can be used
        to make SQLAlchemy queries like: `db.session.query(...query)`.

        Here we will use ElasticSearch for persistence. The `document_store` argument is the ES
        index name. In your `divvy.json` config file you will note this entry:

        ```json

        "documentstores": {
            "default" : {
                "type": "elasticsearch",
                "name": "localhost",
                "hosts": ["http://localhost:9200"],
                "index_name" : "default"
            },
            // THIS IS NEW!!!
            "github" : {
                "type": "elasticsearch",
                "name": "localhost",
                "hosts": ["http://localhost:9200"],
                "index_name" : "github"
            }
        }
        ```

        DivvyCloud supports acquiring connections for multiple indexes and while this example
        uses the "default" index, any number of new indexes can be configured.

        Two remaining things to note. Since each harvest will be inserting multiple documents,
        DivvyCloud's ElasticSearch interface supports batch inserts with the simple syntax of
        a context manager `with` statement. Using `batch.update/1` adds a ES ready formatted
        dictionary. To simplify the formatting of the data, we out line a helper method above
        called `make_metric/1` which takes the response dictionary from the Github API.
        """

        es_connection = Elasticsearch.get_connection(document_store)

        logger.info('Collecting Github repo data')

        with Elasticsearch.ElasticsearchActionBatch(es_connection) as batch:
            for metric in self.repo_getter():
                batch.update(
                    self.make_metric(metric)
                )

    def _cleanup(self):
        super(SkeletonHarvester, self)._cleanup()


@SharedSessionScope(DivvyCloudGatewayORM)
def list_job_templates():
    # Only 1 job template for this job

    job_templates = [
        SkeletonHarvester.create_job_template(),
    ]

    return job_templates


_JOB_LOADED = False


def load():
    global _JOB_LOADED
    try:
        _JOB_LOADED = register_job_module(__name__)
    except AttributeError:
        pass

def unload():
    global _JOB_LOADED
    if _JOB_LOADED:
        unregister_job_module(__name__)
        _JOB_LOADED = False

