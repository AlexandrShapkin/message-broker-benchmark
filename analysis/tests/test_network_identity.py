import copy
import json

from broker_analysis.pipeline import host_controls, run_summary
from test_pipeline import fixture_run


def links(suffix='a' * 12, index=10):
    return [
        {'ifname': 'eth0', 'ifindex': 2, 'mtu': 1500, 'address': '02:00:00:00:00:01'},
        {'ifname': 'br-' + suffix, 'ifindex': index, 'mtu': 1500, 'address': suffix},
        {'ifname': 'veth' + suffix[:7], 'ifindex': index + 1, 'link_index': 2,
         'link_netnsid': index, 'master': 'br-' + suffix, 'mtu': 1500, 'address': suffix},
    ]


def test_generated_links_do_not_split_repetitions(tmp_path):
    runs = [fixture_run(tmp_path / name, name) for name in ['a', 'b']]
    for run, nic in zip(runs, [links(), list(reversed(links('b' * 12, 30)))]):
        path = run / 'manifest.json'
        m = json.loads(path.read_text())
        m['host']['nic'] = json.dumps(nic)
        path.write_text(json.dumps(m))
    assert run_summary(runs[0])[0]['group_id'] == run_summary(runs[1])[0]['group_id']


def test_network_configuration_remains_a_control():
    original = links()
    baseline = host_controls({'nic': json.dumps(original)})
    for index, field, value in [(0, 'address', 'different'), (0, 'mtu', 9000),
                                (1, 'mtu', 9000), (2, 'mtu', 9000)]:
        changed = copy.deepcopy(original)
        changed[index][field] = value
        assert host_controls({'nic': changed}) != baseline
    assert host_controls({'nic': original[:-1]}) != baseline
    assert original == links()  # Normalization must never mutate raw metadata.


def test_custom_bridges_and_unrelated_veth_are_not_normalized():
    a = links()
    a[1]['ifname'] = a[2]['master'] = 'research-bridge'
    b = copy.deepcopy(a)
    b[2]['address'] = 'changed'
    assert host_controls({'nic': a}) != host_controls({'nic': b})


def test_bridge_membership_is_preserved():
    a = links() + links('b' * 12, 30)[1:]
    b = copy.deepcopy(a)
    b[-1]['master'] = a[1]['ifname']
    assert host_controls({'nic': a}) != host_controls({'nic': b})
