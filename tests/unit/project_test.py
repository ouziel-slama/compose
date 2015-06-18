from __future__ import unicode_literals

import docker
import mock
from .. import unittest

from compose.service import (
    Service,
    ServiceLink,
)
from compose.project import Project
from compose.container import Container
from compose import config

import mock
import docker


class ProjectTest(unittest.TestCase):
    def test_from_dict(self):
        project = Project.from_dicts('composetest', [
            {
                'name': 'web',
                'image': 'busybox:latest'
            },
            {
                'name': 'db',
                'image': 'busybox:latest'
            },
        ], None, None, None)
        self.assertEqual(len(project.services), 2)
        self.assertEqual(project.get_service('web').name, 'web')
        self.assertEqual(project.get_service('web').options['image'], 'busybox:latest')
        self.assertEqual(project.get_service('db').name, 'db')
        self.assertEqual(project.get_service('db').options['image'], 'busybox:latest')

    def test_from_dict_sorts_in_dependency_order(self):
        project = Project.from_dicts('composetest', [
            {
                'name': 'web',
                'image': 'busybox:latest',
                'links': ['db'],
            },
            {
                'name': 'db',
                'image': 'busybox:latest',
                'volumes_from': ['volume']
            },
            {
                'name': 'volume',
                'image': 'busybox:latest',
                'volumes': ['/tmp'],
            }
        ], None, None, None)

        self.assertEqual(project.services[0].name, 'volume')
        self.assertEqual(project.services[1].name, 'db')
        self.assertEqual(project.services[2].name, 'web')

    def test_from_config(self):
        dicts = config.from_dictionary({
            'web': {
                'image': 'busybox:latest',
            },
            'db': {
                'image': 'busybox:latest',
            },
        })
        project = Project.from_dicts('composetest', dicts, None)
        self.assertEqual(len(project.services), 2)
        self.assertEqual(project.get_service('web').name, 'web')
        self.assertEqual(project.get_service('web').options['image'], 'busybox:latest')
        self.assertEqual(project.get_service('db').name, 'db')
        self.assertEqual(project.get_service('db').options['image'], 'busybox:latest')

    def test_up_with_fresh_start(self):
        mock_client = mock.create_autospec(docker.Client)
        services = [
            {'name': 'web', 'image': 'busybox:latest', 'links': ['db']},
            {'name': 'db',  'image': 'busybox:latest'},
        ]
        project = Project.from_dicts('test', services, mock_client, None, None)
        containers = project.up(do_build=False, fresh_start=True)
        self.assertEqual(len(containers), 2)

        def build_start_call(links):
            return mock.call.start(
                mock_client.create_container.return_value.__getitem__.return_value,
                links=links,
                cap_add=None,
                restart_policy=None,
                dns_search=None,
                network_mode='bridge',
                binds={},
                dns=None,
                volumes_from=[],
                port_bindings={},
                cap_drop=None,
                privileged=False,
            )

        expected = [
            mock.call.create_container(
                environment={},
                image='busybox:latest',
                detach=False,
                name='test_db_1',
            ),
            build_start_call([]),
            mock.call.create_container(
                environment={},
                image='busybox:latest',
                detach=False,
                name='test_web_1',
            ),
            build_start_call([
                ('test_db_1', 'db'),
                ('test_db_1', 'test_db_1'),
                ('test_db_1', 'db_1'),
            ]),
        ]
        self.assertEqual(mock_client.method_calls, expected)

    def test_get_service(self):
        web = Service(
            project='composetest',
            name='web',
            client=None,
            image="busybox:latest",
        )
        project = Project('test', [web], None, None)
        self.assertEqual(project.get_service('web'), web)

    def test_get_service_with_project_name(self):
        web = Service(project='figtest', name='web')
        project = Project('test', [web], None, None)
        self.assertEqual(project.get_service('test_web'), web)

    def test_get_service_not_found(self):
        project = Project('test', [], None, None)
        with self.assertRaises(NoSuchService):
            project.get_service('not_found')

    def test_get_service_from_external(self):
        project_name = 'theproject'
        web = Service(project='test', name='web')
        external_web = Service(project='other', name='web')
        external_project = Project(
            project_name + 'other',
            [external_web],
            None,
            project_name)
        project = Project(project_name, [web], None, None, [external_project])

        self.assertEqual(project.get_service('other_web'), external_web)

    def test_get_services_returns_all_services_without_args(self):
        web = Service(
            project='composetest',
            name='web',
            image='foo',
        )
        console = Service(
            project='composetest',
            name='console',
            image='foo',
        )
        project = Project('test', [web, console], None)
        self.assertEqual(project.get_services(), [web, console])

    def test_get_services_returns_listed_services_with_args(self):
        web = Service(
            project='composetest',
            name='web',
            image='foo',
        )
        console = Service(
            project='composetest',
            name='console',
            image='foo',
        )
        project = Project('test', [web, console], None)
        self.assertEqual(project.get_services(['console']), [console])

    def test_get_services_with_include_links(self):
        db = Service(
            project='composetest',
            name='db',
            image='foo',
        )
        web = Service(
            project='composetest',
            name='web',
            image='foo',
            links=[ServiceLink(db, 'database')]
        )
        cache = Service(
            project='composetest',
            name='cache',
            image='foo'
        )
        console = Service(
            project='composetest',
            name='console',
            image='foo',
            links=[ServiceLink(web, 'web')]
        )
        project = Project('test', [web, db, cache, console], None)
        self.assertEqual(
            project.get_services(['console'], include_deps=True),
            [db, web, console]
        )

    def test_get_services_removes_duplicates_following_links(self):
        db = Service(
            project='composetest',
            name='db',
            image='foo',
        )
        web = Service(
            project='composetest',
            name='web',
            image='foo',
            links=[ServiceLink(db, 'database')]
        )
        project = Project('test', [web, db], None)
        self.assertEqual(
            project.get_services(['web', 'db'], include_deps=True),
            [db, web]
        )

    def test_use_volumes_from_container(self):
        container_id = 'aabbccddee'
        container_dict = dict(Name='aaa', Id=container_id)
        mock_client = mock.create_autospec(docker.Client)
        mock_client.inspect_container.return_value = container_dict
        project = Project.from_dicts('test', [
            {
                'name': 'test',
                'image': 'busybox:latest',
                'volumes_from': ['aaa']
            }
        ], mock_client)
        self.assertEqual(project.get_service('test')._get_volumes_from(), [container_id])

    def test_use_volumes_from_service_no_container(self):
        container_name = 'test_vol_1'
        mock_client = mock.create_autospec(docker.Client)
        mock_client.containers.return_value = [
            {
                "Name": container_name,
                "Names": [container_name],
                "Id": container_name,
                "Image": 'busybox:latest'
            }
        ]
        project = Project.from_dicts('test', [
            {
                'name': 'vol',
                'image': 'busybox:latest'
            },
            {
                'name': 'test',
                'image': 'busybox:latest',
                'volumes_from': ['vol']
            }
        ], mock_client)
        self.assertEqual(project.get_service('test')._get_volumes_from(), [container_name])

    @mock.patch.object(Service, 'containers')
    def test_use_volumes_from_service_container(self, mock_return):
        container_ids = ['aabbccddee', '12345']
        mock_return.return_value = [
            mock.Mock(id=container_id, spec=Container)
            for container_id in container_ids]

        project = Project.from_dicts('test', [
            {
                'name': 'vol',
                'image': 'busybox:latest'
            },
            {
                'name': 'test',
                'image': 'busybox:latest',
                'volumes_from': ['vol']
            }
        ], None)
        self.assertEqual(project.get_service('test')._get_volumes_from(), container_ids)

    def test_use_net_from_container(self):
        container_id = 'aabbccddee'
        container_dict = dict(Name='aaa', Id=container_id)
        mock_client = mock.create_autospec(docker.Client)
        mock_client.inspect_container.return_value = container_dict
        project = Project.from_dicts('test', [
            {
                'name': 'test',
                'image': 'busybox:latest',
                'net': 'container:aaa'
            }
        ], mock_client)
        service = project.get_service('test')
        self.assertEqual(service._get_net(), 'container:' + container_id)

    def test_use_net_from_service(self):
        container_name = 'test_aaa_1'
        mock_client = mock.create_autospec(docker.Client)
        mock_client.containers.return_value = [
            {
                "Name": container_name,
                "Names": [container_name],
                "Id": container_name,
                "Image": 'busybox:latest'
            }
        ]
        project = Project.from_dicts('test', [
            {
                'name': 'aaa',
                'image': 'busybox:latest'
            },
            {
                'name': 'test',
                'image': 'busybox:latest',
                'net': 'container:aaa'
            }
        ], mock_client)

        service = project.get_service('test')
        self.assertEqual(service._get_net(), 'container:' + container_name)
