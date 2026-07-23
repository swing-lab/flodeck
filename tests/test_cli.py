import pytest

from flodeck.cli import main


def test_poisson_run_prints_report(capsys):
    exit_code = main(['--nodes', '10',
                      '--arrival-rate', '2.',
                      '--execution-rate', '1.',
                      '--time-limit', '100.'])

    assert exit_code == 0
    printed = capsys.readouterr().out
    assert 'Completed jobs' in printed
    assert 'Utilization' in printed


def test_input_file_run(tmp_path, capsys):
    data = tmp_path / 'input.txt'
    data.write_text('1.0,2.0,1\n2.0,2.0,1\n')
    out = tmp_path / 'out.txt'

    exit_code = main(['--nodes', '2',
                      '--input', str(data),
                      '--output', str(out)])

    assert exit_code == 0
    assert len(out.read_text().splitlines()) == 2
    assert 'Completed jobs       : 2' in capsys.readouterr().out


def test_policy_run(capsys):
    exit_code = main(['--policy', 'frontier',
                      '--backfill', '--hold-overflow',
                      '--arrival-rate', '0.01',
                      '--time-limit', '2000.'])

    assert exit_code == 0
    assert 'Utilization' in capsys.readouterr().out


def test_nodes_required_without_policy(capsys):
    with pytest.raises(SystemExit):
        main(['--arrival-rate', '1.', '--execution-rate', '1.',
              '--num-jobs', '5'])
    assert '--nodes is required' in capsys.readouterr().err


def test_generated_flow_needs_a_limit(capsys):
    with pytest.raises(SystemExit):
        main(['--nodes', '2', '--arrival-rate', '1.',
              '--execution-rate', '1.'])
    assert 'needs --num-jobs or --time-limit' \
        in capsys.readouterr().err


def test_arrival_rate_needs_execution_rate_or_policy(capsys):
    with pytest.raises(SystemExit):
        main(['--nodes', '2', '--arrival-rate', '1.',
              '--num-jobs', '5'])
    assert '--execution-rate' in capsys.readouterr().err


def test_missing_workload(capsys):
    with pytest.raises(SystemExit):
        main(['--nodes', '2'])
    assert 'no workload is defined' in capsys.readouterr().err


def test_unknown_policy_is_rejected():
    with pytest.raises(SystemExit):
        main(['--policy', 'summit', '--arrival-rate', '1.',
              '--num-jobs', '5'])
