from broker_analysis.pipeline import proc_row


def test_cgroup_io_includes_all_devices_without_process_io():
    row = proc_row({'time': '2026-01-01T00:00:00Z', 'files': {
        '/sys/fs/cgroup/io.stat': '8:0 rbytes=100 wbytes=500 rios=1 wios=2\n'
                                  '8:1 rbytes=200 wbytes=300 rios=3 wios=4\n',
    }}, 'mosquitto')
    assert row['disk_read'] == 300
    assert row['disk_write'] == 800
    assert row['disk_read_operations'] == 4
    assert row['disk_write_operations'] == 6


def test_empty_readable_io_is_zero_but_missing_io_is_unknown():
    row = {'time': '2026-01-01T00:00:00Z', 'files': {'/sys/fs/cgroup/io.stat': ''}}
    assert proc_row(row, 'nats')['disk_write'] == 0
    row['files'] = {}
    assert proc_row(row, 'nats')['disk_write'] is None


def test_broker_rss_uses_selected_process_instead_of_launcher():
    row = proc_row({'time': '2026-01-01T00:00:00Z', 'files': {
        'broker_pid': '42\n', '/proc/1/status': 'Name:\tsh\nVmRSS:\t2000 kB',
        '/proc/42/status': 'Name:\tbeam.smp\nVmRSS:\t100000 kB\nThreads:\t20',
    }}, 'rabbitmq')
    assert row['rss_bytes'] == 102400000
    assert row['threads'] == 20
    assert row['process_name'] == 'beam.smp'


def test_new_protocol_port_requires_no_analysis_branch():
    row = proc_row({'time': '2026-01-01T00:00:00Z', 'files': {
        '/proc/net/tcp': '0: 0100007F:1F40 0100007F:C001 01\n',
    }}, 'experimental-broker', protocol_port=8000)
    assert row['connections'] == 1
