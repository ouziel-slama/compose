from __future__ import unicode_literals
from __future__ import absolute_import
from requests.exceptions import ConnectionError, SSLError
import logging
import os
import re
import six

from .. import config
from ..container import get_container_name, Container
from ..project import Project
from ..service import ConfigError
from .docopt_command import DocoptCommand
from .utils import call_silently, is_mac, is_ubuntu, find_candidates_in_parent_dirs
from .docker_client import docker_client
from . import verbose_proxy
from . import errors
from .. import __version__

log = logging.getLogger(__name__)

SUPPORTED_FILENAMES = [
    'docker-compose.yml',
    'docker-compose.yaml',
    'fig.yml',
    'fig.yaml',
]


class Command(DocoptCommand):
    base_dir = '.'

    def dispatch(self, *args, **kwargs):
        try:
            super(Command, self).dispatch(*args, **kwargs)
        except SSLError as e:
            raise errors.UserError('SSL error: %s' % e)
        except ConnectionError:
            if call_silently(['which', 'docker']) != 0:
                if is_mac():
                    raise errors.DockerNotFoundMac()
                elif is_ubuntu():
                    raise errors.DockerNotFoundUbuntu()
                else:
                    raise errors.DockerNotFoundGeneric()
            elif call_silently(['which', 'boot2docker']) == 0:
                raise errors.ConnectionErrorBoot2Docker()
            else:
                raise errors.ConnectionErrorGeneric(self.get_client().base_url)

    def perform_command(self, options, handler, command_options):
        if options['COMMAND'] == 'help':
            # Skip looking up the compose file.
            handler(None, command_options)
            return

        if 'FIG_FILE' in os.environ:
            log.warn('The FIG_FILE environment variable is deprecated.')
            log.warn('Please use COMPOSE_FILE instead.')

        explicit_config_path = options.get('--file') or os.environ.get('COMPOSE_FILE') or os.environ.get('FIG_FILE')
        project = self.get_project(
            self.get_config_path(explicit_config_path),
            project_name=options.get('--project-name'),
            verbose=options.get('--verbose'))

        if options['--migrate-to-labels']:
            migrate_project_to_labels(project)
            return

        handler(project, command_options)

    def get_client(self, verbose=False):
        client = docker_client()
        if verbose:
            version_info = six.iteritems(client.version())
            log.info("Compose version %s", __version__)
            log.info("Docker base_url: %s", client.base_url)
            log.info("Docker version: %s",
                     ", ".join("%s=%s" % item for item in version_info))
            return verbose_proxy.VerboseProxy('docker', client)
        return client

    def get_project(self, config_path, project_name=None, verbose=False):
        try:
            return Project.from_dicts(
                self.get_project_name(config_path, project_name),
                config.load(config_path),
                self.get_client(verbose=verbose))
        except ConfigError as e:
            raise errors.UserError(six.text_type(e))

    def get_project_name(self, config_path, project_name=None):
        def normalize_name(name):
            return re.sub(r'[^a-z0-9]', '', name.lower())

        if 'FIG_PROJECT_NAME' in os.environ:
            log.warn('The FIG_PROJECT_NAME environment variable is deprecated.')
            log.warn('Please use COMPOSE_PROJECT_NAME instead.')

        project_name = project_name or os.environ.get('COMPOSE_PROJECT_NAME') or os.environ.get('FIG_PROJECT_NAME')
        if project_name is not None:
            return normalize_name(project_name)

        project = os.path.basename(os.path.dirname(os.path.abspath(config_path)))
        if project:
            return normalize_name(project)

        return 'default'

    def get_config_path(self, file_path=None):
        if file_path:
            return os.path.join(self.base_dir, file_path)

        (candidates, path) = find_candidates_in_parent_dirs(SUPPORTED_FILENAMES, self.base_dir)

        if len(candidates) == 0:
            raise errors.ComposeFileNotFound(SUPPORTED_FILENAMES)

        winner = candidates[0]

        if len(candidates) > 1:
            log.warning("Found multiple config files with supported names: %s", ", ".join(candidates))
            log.warning("Using %s\n", winner)

        if winner == 'docker-compose.yaml':
            log.warning("Please be aware that .yml is the expected extension "
                        "in most cases, and using .yaml can cause compatibility "
                        "issues in future.\n")

        if winner.startswith("fig."):
            log.warning("%s is deprecated and will not be supported in future. "
                        "Please rename your config file to docker-compose.yml\n" % winner)

        return os.path.join(path, winner)


# TODO: remove this section when migrate_project_to_labels is removed
NAME_RE = re.compile(r'^([^_]+)_([^_]+)_(run_)?(\d+)$')


def is_valid_name(name):
    match = NAME_RE.match(name)
    return match is not None


def add_labels(project, container, name):
    project_name, service_name, one_off, number = NAME_RE.match(name).groups()
    service = project.get_service(service_name)
    service.recreate_container(container)


def migrate_project_to_labels(project):
    log.info("Running migration to labels for project %s", project.name)

    client = project.client
    for container in client.containers(all=True):
        name = get_container_name(container)
        if not is_valid_name(name):
            continue
        add_labels(project, Container.from_ps(client, container), name)
