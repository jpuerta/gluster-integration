import tendrl.gluster_integration.sds_sync as sds_sync

import json

from pytest import fixture
from mock import patch, Mock, call

@fixture(autouse=True)
def _NS():
    with patch('__builtin__.NS', Mock(), create=True) as mock:
        yield mock

@fixture
def _sds_sync():
    return sds_sync.GlusterIntegrationSdsSyncStateThread()


def test_setup_ok(_sds_sync):
    _sds_sync._setup()

    assert NS.gluster.objects.GlusterBrickDir.return_value.save.called


@patch.object(sds_sync.GlusterIntegrationSdsSyncStateThread,
        'extract_network_inteface', Mock())
@patch('tendrl.commons.utils.etcd_utils.read',
        side_effect=[sds_sync.etcd.EtcdKeyNotFound, Mock])
def test_setup_etcd_key_not_found(etcd_utils_read, _sds_sync):
    _sds_sync._setup()

    assert NS.gluster.objects.GlusterBrickDir.return_value.save.called
    assert etcd_utils_read.call_count == 2
    assert NS.tendrl.objects.Cluster.return_value.load.return_value.save.called


@patch.object(sds_sync, 'event_utils', Mock())
@patch.object(sds_sync, 'brick_status_alert', Mock())
def test_update_peers(_sds_sync):
    peers = {
            'peer1.uuid': 'ed24881b-2ca6-4d46-8df4-e2f8c7dc04c7',
            'peer1.primary_hostname': 'example.com',
            'peer1.state': 'disconnected',
            'peer1.connected': 'disconnected',
            }
    sync_ttl = _sds_sync._update_peers(peers, 10)

    assert NS.gluster.objects.Peer.return_value.save.called
    assert sync_ttl == 15


@patch.object(sds_sync, 'event_utils', Mock())
@patch.object(sds_sync, 'sync_volumes', Mock(side_effect=[None, KeyError]))
def test_update_volumes(_sds_sync):
    volumes = {
            'volume1.options': 'dummy_option',
            'volume1.id': 'id',
            }
    options = {
            'Volume Options': 'options',
            }

    sync_ttl = _sds_sync._update_volumes(volumes, options, 15)

    assert NS.gluster.objects.VolumeOptions.return_value.save.called
    assert sync_ttl == 15 + 1


@patch('subprocess.call')
@patch.multiple('tendrl.gluster_integration.sds_sync',
        cluster_status=Mock(),
        utilization=Mock(),
        client_connections=Mock(),
        georep_details=Mock(),
        rebalance_status=Mock(),
        snapshots=Mock(),
        )
@patch.object(sds_sync.GlusterIntegrationSdsSyncStateThread, '_update_peers',
        Mock(return_value=10))
@patch.object(sds_sync.GlusterIntegrationSdsSyncStateThread, '_update_volumes',
        Mock(return_value=10))
@patch.object(sds_sync, 'evt', Mock())
@patch.object(sds_sync, 'Event', Mock())
@patch.object(sds_sync, 'ExceptionMessage', Mock())
@patch.object(sds_sync.ini2json, 'ini_to_dict',
        Mock(side_effect=[{'Peers': None, 'Volumes': None}, {}]))
def test_run_once_ok(subprocess_call_mock, _sds_sync):
    NS.config.data.get.return_value = "10"
    NS.tendrl_context.load.return_value.integration_id = '13'
    NS.node_context.load.return_value.tags = ['provisioner/13']
    Volume = Mock()
    Volume.deleted = "false"
    NS.gluster.objects.Volume.return_value.load_all.return_value = [Volume]
    NS.tendrl.objects.Cluster.return_value.exists.return_value = True

    _sds_sync._run_once()

    assert NS.tendrl.objects.Cluster.return_value.load_called
    assert NS._int.wclient.write.called
    assert subprocess_call_mock.call_args_list == [
            call(['gluster', 'get-state', 'glusterd', 'odir', '/var/run', 'file',
            'glusterd-state', 'detail']),
            call(['rm', '-rf', '/var/run/glusterd-state']),
            call(['gluster', 'get-state', 'glusterd', 'odir', '/var/run', 'file',
            'glusterd-state-vol-opts', 'volumeoptions']),
            call(['rm', '-rf', '/var/run/glusterd-state-vol-opts']),
            ]
    assert NS.gluster.objects.SyncObject.return_value.save.called
    assert NS.tendrl.objects.Cluster.return_value.load.return_value.save.called

