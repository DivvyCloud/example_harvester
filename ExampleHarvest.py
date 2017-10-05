import logging
from datetime import datetime

import requests
from DivvyDb.DbObjects.metric import Metric
from DivvyDb.DivvyCloudGatewayORM import DivvyCloudGatewayORM
from DivvyDb.DivvyDb import NewSession, SharedSessionScope
from DivvyJobs.schedules import LazyScheduleGoal
from DivvyPlugins.plugin_helpers import (register_job_module,
                                         unregister_job_module)
from DivvyPlugins.plugin_jobs import PluginHarvester
from DivvySession.DivvySession import EscalatePermissions
from DivvyUtils import schedule

logger = logging.getLogger('ExampleHarvest')

repos_url = "https://api.github.com/orgs/DivvyCloud/repos"
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

    @SharedSessionScope(DivvyCloudGatewayORM)
    @EscalatePermissions()
    def do_harvest(self):
        """ Do the harvest

        While not used, the `@SharedSessionScope(DivvyCloudGatewayORM)` decorator
        is require anytime you need to access the MySQL database within a job context.

        Acquire a connection like so; `db = DivvyCloudGatewayORM()` which can be used
        to make SQLAlchemy queries like: `db.session.query(...query)`.

        In the case below where I'm only writing data, I'm using NewSessionScope which
        creates a new database session.

        Here we will use MySQL for persistence. The Metrics table allows for somewhat
        arbirary key/value storage of metric information about resources. In this case
        we're not referring to actual divvy resources so `target_resource_id` is the
        fully qualified name of the git repo.

        Two remaining things to note. Since each harvest will be inserting multiple records,
        we use the SQLAlchemy `bulk_save_objects/1` function to commit them all in one query.
        In certain cases where the insert size is large, it is sometimes necessary to chunk
        the inserts into batch sizes to avoid max packet size limits of mysql queries.

        Here we get each github repo and then insert three metrics for each; name, description,
        and url. Each is associated with the `full_name` attribute which is unique.
        """

        with NewSession(DivvyCloudGatewayORM):
            db = DivvyCloudGatewayORM()
            metric_resources = []
            for metric in self.repo_getter():
                for metric_type in ['name', 'description', 'html_url']:
                    metric_resources.append(
                        Metric(
                            metric_id='mymetric.github.%s' % metric_type,
                            value=metric.get(metric_type, ''),
                            # Cloud account resource my belong to
                            organization_service_id=None,
                            # Organization resource belongs to
                            organization_id=None,
                            # Unique Id for document, here I'm using the fully qualified repo name.
                            target_resource_id=metric.get('full_name'),
                            creation_timestamp=datetime.strftime(
                                datetime.utcnow(), '%Y-%m-%d %H:%M:%S'
                            )
                        )
                    )

            if metric_resources:
                db.session.bulk_save_objects(metric_resources)
                db.session.commit()

        logger.info('Collecting Github repo data')

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
