import { fireEvent, screen } from '@testing-library/react';
import { describe, expect, test } from 'vitest';

import WorkersTable from 'components/WorkersTable/WorkersTable';
import { SWARM_STATE } from 'constants/swarm';
import { renderWithProvider } from 'test/testUtils';
import { ISwarmWorker } from 'types/ui.types';

const healthyWorker: ISwarmWorker = {
  cpuUsage: 1.2,
  id: 'healthy_worker',
  memoryUsage: 1024,
  state: SWARM_STATE.RUNNING,
  userCount: 3,
  quarantineReasons: [],
};

const quarantinedWorker: ISwarmWorker = {
  cpuUsage: 0,
  id: 'bad_worker',
  memoryUsage: 512,
  state: SWARM_STATE.QUARANTINED,
  userCount: 0,
  quarantineReasons: [
    'locustfile content differs: locustfile.py',
    'user class weight differs: MyUser',
  ],
};

describe('WorkersTable', () => {
  test('renders quarantined workers with an error chip and their reasons', async () => {
    const { getByTestId, getByText } = renderWithProvider(<WorkersTable />, {
      ui: { workers: [healthyWorker, quarantinedWorker] },
    });

    expect(getByTestId('quarantined-bad_worker')).toBeTruthy();
    expect(getByText('quarantined')).toBeTruthy();
    expect(getByText('healthy_worker')).toBeTruthy();

    fireEvent.mouseEnter(getByTestId('quarantined-bad_worker'));
    expect(await screen.findByText('locustfile content differs: locustfile.py')).toBeTruthy();
    expect(screen.getByText('user class weight differs: MyUser')).toBeTruthy();
  });

  test('renders healthy worker state as plain text without a quarantine chip', () => {
    const { queryByTestId, getByText } = renderWithProvider(<WorkersTable />, {
      ui: { workers: [healthyWorker] },
    });

    expect(queryByTestId('quarantined-healthy_worker')).toBeNull();
    expect(getByText('running')).toBeTruthy();
  });
});
