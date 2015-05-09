from __future__ import unicode_literals
from __future__ import absolute_import
from collections import namedtuple
import logging
import re
from operator import attrgetter
import sys
import six

from docker.errors import APIError
from docker.utils import create_host_config, LogConfig

from . import __version__
from .config import DOCKER_CONFIG_KEYS
from .const import (
    CONTAINER_NUMBER_LABEL,
    ONE_OFF_LABEL,
    PROJECT_LABEL,
    SERVICE_LABEL,
    VERSION_LABEL,
)
from .container import Container, get_container_name
from .progress_stream import stream_output, StreamOutputError

log = logging.getLogger(__name__)


DOCKER_START_KEYS = [
    'cap_add',
    'cap_drop',
    'dns',
    'dns_search',
    'env_file',
    'extra_hosts',
    'read_only',
    'net',
    'log_driver',
    'pid',
    'privileged',
    'restart',
    'volumes_from',
]

VALID_NAME_CHARS = '[a-zA-Z0-9]'


class BuildError(Exception):
    def __init__(self, service, reason):
        self.service = service
        self.reason = reason


class CannotBeScaledError(Exception):
    pass


class ConfigError(ValueError):
    pass


VolumeSpec = namedtuple('VolumeSpec', 'external internal mode')


ServiceName = namedtuple('ServiceName', 'project service number')


class Service(object):
    def __init__(self, name, client=None, project='default', links=None, external_links=None, volumes_from=None, net=None, **options):
        if not re.match('^%s+$' % VALID_NAME_CHARS, name):
            raise ConfigError('Invalid service name "%s" - only %s are allowed' % (name, VALID_NAME_CHARS))
        if not re.match('^%s+$' % VALID_NAME_CHARS, project):
            raise ConfigError('Invalid project name "%s" - only %s are allowed' % (project, VALID_NAME_CHARS))
        if 'image' in options and 'build' in options:
            raise ConfigError('Service %s has both an image and build path specified. A service can either be built to image or use an existing image, not both.' % name)
        if 'image' not in options and 'build' not in options:
            raise ConfigError('Service %s has neither an image nor a build path specified. Exactly one must be provided.' % name)

        self.name = name
        self.client = client
        self.project = project
        self.links = links or []
        self.external_links = external_links or []
        self.volumes_from = volumes_from or []
        self.net = net or None
        self.options = options

    def containers(self, stopped=False, one_off=False):
        containers = [
            Container.from_ps(self.client, container)
            for container in self.client.containers(
                all=stopped,
                filters={'label': self.labels(one_off=one_off)})]

        if not containers:
            check_for_legacy_containers(
                self.client,
                self.project,
                [self.name],
                stopped=stopped,
                one_off=one_off)

        return containers

    def get_container(self, number=1):
        """Return a :class:`compose.container.Container` for this service. The
        container must be active, and match `number`.
        """
        labels = self.labels() + ['{0}={1}'.format(CONTAINER_NUMBER_LABEL, number)]
        for container in self.client.containers(filters={'label': labels}):
            return Container.from_ps(self.client, container)

        raise ValueError("No container found for %s_%s" % (self.name, number))

    def start(self, **options):
        for c in self.containers(stopped=True):
            self.start_container_if_stopped(c, **options)

    def stop(self, **options):
        for c in self.containers():
            log.info("Stopping %s..." % c.name)
            c.stop(**options)

    def kill(self, **options):
        for c in self.containers():
            log.info("Killing %s..." % c.name)
            c.kill(**options)

    def restart(self, **options):
        for c in self.containers():
            log.info("Restarting %s..." % c.name)
            c.restart(**options)

    def scale(self, desired_num):
        """
        Adjusts the number of containers to the specified number and ensures
        they are running.

        - creates containers until there are at least `desired_num`
        - stops containers until there are at most `desired_num` running
        - starts containers until there are at least `desired_num` running
        - removes all stopped containers
        """
        if not self.can_be_scaled():
            raise CannotBeScaledError()

        # Create enough containers
        containers = self.containers(stopped=True)
        while len(containers) < desired_num:
            containers.append(self.create_container(detach=True))

        running_containers = []
        stopped_containers = []
        for c in containers:
            if c.is_running:
                running_containers.append(c)
            else:
                stopped_containers.append(c)
        running_containers.sort(key=lambda c: c.number)
        stopped_containers.sort(key=lambda c: c.number)

        # Stop containers
        while len(running_containers) > desired_num:
            c = running_containers.pop()
            log.info("Stopping %s..." % c.name)
            c.stop(timeout=1)
            stopped_containers.append(c)

        # Start containers
        while len(running_containers) < desired_num:
            c = stopped_containers.pop(0)
            log.info("Starting %s..." % c.name)
            self.start_container(c)
            running_containers.append(c)

        self.remove_stopped()

    def remove_stopped(self, **options):
        for c in self.containers(stopped=True):
            if not c.is_running:
                log.info("Removing %s..." % c.name)
                c.remove(**options)

    def create_container(self,
                         one_off=False,
                         insecure_registry=False,
                         do_build=True,
                         previous_container=None,
                         number=None,
                         **override_options):
        """
        Create a container for this service. If the image doesn't exist, attempt to pull
        it.
        """
        override_options['volumes_from'] = self._get_volumes_from(previous_container)
        container_options = self._get_container_create_options(
            override_options,
            number or self._next_container_number(one_off=one_off),
            one_off=one_off,
        )

        if (do_build and
                self.can_be_built() and
                not self.client.images(name=self.full_name)):
            self.build()

        try:
            return Container.create(self.client, **container_options)
        except APIError as e:
            if e.response.status_code == 404 and e.explanation and 'No such image' in str(e.explanation):
                self.pull(insecure_registry=insecure_registry)
                return Container.create(self.client, **container_options)
            raise

    def recreate_containers(self, insecure_registry=False, do_build=True, **override_options):
        """
        If a container for this service doesn't exist, create and start one. If there are
        any, stop them, create+start new ones, and remove the old containers.
        """
        containers = self.containers(stopped=True)
        if not containers:
            container = self.create_container(
                insecure_registry=insecure_registry,
                do_build=do_build,
                **override_options)
            self.start_container(container)
            return [container]

        return [
            self.recreate_container(
                c,
                insecure_registry=insecure_registry,
                **override_options)
            for c in containers
        ]

    def recreate_container(self, container, **override_options):
        """Recreate a container.

        The original container is renamed to a temporary name so that data
        volumes can be copied to the new container, before the original
        container is removed.
        """
        log.info("Recreating %s..." % container.name)
        try:
            container.stop()
        except APIError as e:
            if (e.response.status_code == 500
                    and e.explanation
                    and 'no such process' in str(e.explanation)):
                pass
            else:
                raise

        # Use a hopefully unique container name by prepending the short id
        self.client.rename(
            container.id,
            '%s_%s' % (container.short_id, container.name))
        new_container = self.create_container(
            do_build=False,
            previous_container=container,
            number=container.labels.get(CONTAINER_NUMBER_LABEL),
            **override_options)
        self.start_container(new_container)
        container.remove()
        return new_container

    def start_container_if_stopped(self, container):
        if container.is_running:
            return container
        else:
            log.info("Starting %s..." % container.name)
            return self.start_container(container)

    def start_container(self, container):
        container.start()
        return container

    def start_or_create_containers(
            self,
            insecure_registry=False,
            detach=False,
            do_build=True):
        containers = self.containers(stopped=True)

        if not containers:
            new_container = self.create_container(
                insecure_registry=insecure_registry,
                detach=detach,
                do_build=do_build,
            )
            return [self.start_container(new_container)]
        else:
            return [self.start_container_if_stopped(c) for c in containers]

    def get_linked_names(self):
        return [s.name for (s, _) in self.links]

    def get_volumes_from_names(self):
        return [s.name for s in self.volumes_from if isinstance(s, Service)]

    def get_net_name(self):
        if isinstance(self.net, Service):
            return self.net.name
        else:
            return

    def get_container_name(self, number, one_off=False):
        # TODO: Implement issue #652 here
        return build_container_name(self.project, self.name, number, one_off)

    # TODO: this would benefit from github.com/docker/docker/pull/11943
    # to remove the need to inspect every container
    def _next_container_number(self, one_off=False):
        numbers = [
            Container.from_ps(self.client, container).number
            for container in self.client.containers(
                all=True,
                filters={'label': self.labels(one_off=one_off)})
        ]
        return 1 if not numbers else max(numbers) + 1

    def _get_links(self, link_to_self):
        links = []
        for service, link_name in self.links:
            for container in service.containers():
                links.append((container.name, link_name or service.name))
                links.append((container.name, container.name))
                links.append((container.name, container.name_without_project))
        if link_to_self:
            for container in self.containers():
                links.append((container.name, self.name))
                links.append((container.name, container.name))
                links.append((container.name, container.name_without_project))
        for external_link in self.external_links:
            if ':' not in external_link:
                link_name = external_link
            else:
                external_link, link_name = external_link.split(':')
            links.append((external_link, link_name))
        return links

    def _get_volumes_from(self, previous_container=None):
        volumes_from = []
        for volume_source in self.volumes_from:
            if isinstance(volume_source, Service):
                containers = volume_source.containers(stopped=True)
                if not containers:
                    volumes_from.append(volume_source.create_container().id)
                else:
                    volumes_from.extend(map(attrgetter('id'), containers))

            elif isinstance(volume_source, Container):
                volumes_from.append(volume_source.id)

        if previous_container:
            volumes_from.append(previous_container.id)

        return volumes_from

    def _get_net(self):
        if not self.net:
            return "bridge"

        if isinstance(self.net, Service):
            containers = self.net.containers()
            if len(containers) > 0:
                net = 'container:' + containers[0].id
            else:
                log.warning("Warning: Service %s is trying to use reuse the network stack "
                            "of another service that is not running." % (self.net.name))
                net = None
        elif isinstance(self.net, Container):
            net = 'container:' + self.net.id
        else:
            net = self.net

        return net

    def _get_container_create_options(
            self,
            override_options,
            number,
            one_off=False):
        container_options = dict(
            (k, self.options[k])
            for k in DOCKER_CONFIG_KEYS if k in self.options)
        container_options.update(override_options)

        container_options['name'] = self.get_container_name(number, one_off)

        # If a qualified hostname was given, split it into an
        # unqualified hostname and a domainname unless domainname
        # was also given explicitly. This matches the behavior of
        # the official Docker CLI in that scenario.
        if ('hostname' in container_options
                and 'domainname' not in container_options
                and '.' in container_options['hostname']):
            parts = container_options['hostname'].partition('.')
            container_options['hostname'] = parts[0]
            container_options['domainname'] = parts[2]

        if 'ports' in container_options or 'expose' in self.options:
            ports = []
            all_ports = container_options.get('ports', []) + self.options.get('expose', [])
            for port in all_ports:
                port = str(port)
                if ':' in port:
                    port = port.split(':')[-1]
                if '/' in port:
                    port = tuple(port.split('/'))
                ports.append(port)
            container_options['ports'] = ports

        if 'volumes' in container_options:
            container_options['volumes'] = dict(
                (parse_volume_spec(v).internal, {})
                for v in container_options['volumes'])

        if self.can_be_built():
            container_options['image'] = self.full_name

        container_options['labels'] = build_container_labels(
            container_options.get('labels', {}),
            self.labels(one_off=one_off),
            number)

        # Delete options which are only used when starting
        for key in DOCKER_START_KEYS:
            container_options.pop(key, None)

        container_options['host_config'] = self._get_container_host_config(
            override_options,
            one_off=one_off)

        return container_options

    def _get_container_host_config(self, override_options, one_off=False):
        options = dict(self.options, **override_options)
        port_bindings = build_port_bindings(options.get('ports') or [])

        volume_bindings = dict(
            build_volume_binding(parse_volume_spec(volume))
            for volume in options.get('volumes') or []
            if ':' in volume)

        privileged = options.get('privileged', False)
        cap_add = options.get('cap_add', None)
        cap_drop = options.get('cap_drop', None)
        log_config = LogConfig(type=options.get('log_driver', 'json-file'))
        pid = options.get('pid', None)

        dns = options.get('dns', None)
        if isinstance(dns, six.string_types):
            dns = [dns]

        dns_search = options.get('dns_search', None)
        if isinstance(dns_search, six.string_types):
            dns_search = [dns_search]

        restart = parse_restart_spec(options.get('restart', None))

        extra_hosts = build_extra_hosts(options.get('extra_hosts', None))
        read_only = options.get('read_only', None)

        return create_host_config(
            links=self._get_links(link_to_self=one_off),
            port_bindings=port_bindings,
            binds=volume_bindings,
            volumes_from=options.get('volumes_from'),
            privileged=privileged,
            network_mode=self._get_net(),
            dns=dns,
            dns_search=dns_search,
            restart_policy=restart,
            cap_add=cap_add,
            cap_drop=cap_drop,
            log_config=log_config,
            extra_hosts=extra_hosts,
            read_only=read_only,
            pid_mode=pid
        )

    def build(self, no_cache=False):
        log.info('Building %s...' % self.name)

        path = six.binary_type(self.options['build'])

        build_output = self.client.build(
            path=path,
            tag=self.full_name,
            stream=True,
            rm=True,
            nocache=no_cache,
            dockerfile=self.options.get('dockerfile', None),
        )

        try:
            all_events = stream_output(build_output, sys.stdout)
        except StreamOutputError as e:
            raise BuildError(self, unicode(e))

        # Ensure the HTTP connection is not reused for another
        # streaming command, as the Docker daemon can sometimes
        # complain about it
        self.client.close()

        image_id = None

        for event in all_events:
            if 'stream' in event:
                match = re.search(r'Successfully built ([0-9a-f]+)', event.get('stream', ''))
                if match:
                    image_id = match.group(1)

        if image_id is None:
            raise BuildError(self, event if all_events else 'Unknown')

        return image_id

    def can_be_built(self):
        return 'build' in self.options

    @property
    def full_name(self):
        """
        The tag to give to images built for this service.
        """
        return '%s_%s' % (self.project, self.name)

    def labels(self, one_off=False):
        return [
            '{0}={1}'.format(PROJECT_LABEL, self.project),
            '{0}={1}'.format(SERVICE_LABEL, self.name),
            '{0}={1}'.format(ONE_OFF_LABEL, "True" if one_off else "False")
        ]

    def can_be_scaled(self):
        for port in self.options.get('ports', []):
            if ':' in str(port):
                return False
        return True

    def pull(self, insecure_registry=False):
        if 'image' not in self.options:
            return

        repo, tag = parse_repository_tag(self.options['image'])
        tag = tag or 'latest'
        log.info('Pulling %s (%s:%s)...' % (self.name, repo, tag))
        output = self.client.pull(
            repo,
            tag=tag,
            stream=True,
            insecure_registry=insecure_registry)
        stream_output(output, sys.stdout)


def build_container_name(project, service, number, one_off=False):
    bits = [project, service]
    if one_off:
        bits.append('run')
    return '_'.join(bits + [str(number)])


def build_container_labels(label_options, service_labels, number, one_off=False):
    labels = label_options or {}
    labels.update(label.split('=', 1) for label in service_labels)
    labels[CONTAINER_NUMBER_LABEL] = str(number)
    labels[VERSION_LABEL] = __version__
    return labels


def check_for_legacy_containers(
        client,
        project,
        services,
        stopped=False,
        one_off=False):
    """Check if there are containers named using the old naming convention
    and warn the user that those containers may need to be migrated to
    using labels, so that compose can find them.
    """
    for container in client.containers(all=stopped):
        name = get_container_name(container)
        for service in services:
            prefix = '%s_%s_%s' % (project, service, 'run_' if one_off else '')
            if not name.startswith(prefix):
                continue

            log.warn(
                "Compose found a found a container named %s without any "
                "labels. As of compose 1.3.0 containers are identified with "
                "labels instead of naming convention. If you'd like compose "
                "to use this container, please run "
                "`docker-compose --migrate-to-labels`" % (name,))


def parse_restart_spec(restart_config):
    if not restart_config:
        return None
    parts = restart_config.split(':')
    if len(parts) > 2:
        raise ConfigError("Restart %s has incorrect format, should be "
                          "mode[:max_retry]" % restart_config)
    if len(parts) == 2:
        name, max_retry_count = parts
    else:
        name, = parts
        max_retry_count = 0

    return {'Name': name, 'MaximumRetryCount': int(max_retry_count)}


def parse_volume_spec(volume_config):
    parts = volume_config.split(':')
    if len(parts) > 3:
        raise ConfigError("Volume %s has incorrect format, should be "
                          "external:internal[:mode]" % volume_config)

    if len(parts) == 1:
        return VolumeSpec(None, parts[0], 'rw')

    if len(parts) == 2:
        parts.append('rw')

    external, internal, mode = parts
    if mode not in ('rw', 'ro'):
        raise ConfigError("Volume %s has invalid mode (%s), should be "
                          "one of: rw, ro." % (volume_config, mode))

    return VolumeSpec(external, internal, mode)


def parse_repository_tag(s):
    if ":" not in s:
        return s, ""
    repo, tag = s.rsplit(":", 1)
    if "/" in tag:
        return s, ""
    return repo, tag


def build_volume_binding(volume_spec):
    internal = {'bind': volume_spec.internal, 'ro': volume_spec.mode == 'ro'}
    return volume_spec.external, internal


def build_port_bindings(ports):
    port_bindings = {}
    for port in ports:
        internal_port, external = split_port(port)
        if internal_port in port_bindings:
            port_bindings[internal_port].append(external)
        else:
            port_bindings[internal_port] = [external]
    return port_bindings


def split_port(port):
    parts = str(port).split(':')
    if not 1 <= len(parts) <= 3:
        raise ConfigError('Invalid port "%s", should be '
                          '[[remote_ip:]remote_port:]port[/protocol]' % port)

    if len(parts) == 1:
        internal_port, = parts
        return internal_port, None
    if len(parts) == 2:
        external_port, internal_port = parts
        return internal_port, external_port

    external_ip, external_port, internal_port = parts
    return internal_port, (external_ip, external_port or None)


def build_extra_hosts(extra_hosts_config):
    if not extra_hosts_config:
        return {}

    if isinstance(extra_hosts_config, list):
        extra_hosts_dict = {}
        for extra_hosts_line in extra_hosts_config:
            if not isinstance(extra_hosts_line, six.string_types):
                raise ConfigError(
                    "extra_hosts_config \"%s\" must be either a list of strings or a string->string mapping," %
                    extra_hosts_config
                )
            host, ip = extra_hosts_line.split(':')
            extra_hosts_dict.update({host.strip(): ip.strip()})
        extra_hosts_config = extra_hosts_dict

    if isinstance(extra_hosts_config, dict):
        return extra_hosts_config

    raise ConfigError(
        "extra_hosts_config \"%s\" must be either a list of strings or a string->string mapping," %
        extra_hosts_config
    )
