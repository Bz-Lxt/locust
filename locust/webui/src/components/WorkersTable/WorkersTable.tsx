import { Chip, Tooltip } from '@mui/material';
import { connect } from 'react-redux';

import Table from 'components/Table/Table';
import { SWARM_STATE } from 'constants/swarm';
import { IRootState } from 'redux/store';
import { ITableStructure } from 'types/table.types';
import { ISwarmWorker } from 'types/ui.types';
import { formatBytes } from 'utils/string';

const renderState = (state: ISwarmWorker['state'], row?: ISwarmWorker) => {
  if (state === SWARM_STATE.QUARANTINED) {
    const label = 'quarantined';

    if (row?.quarantineReasons?.length) {
      return (
        <Tooltip
          title={row.quarantineReasons.map(reason => (
            <div key={reason}>{reason}</div>
          ))}
        >
          <Chip color='error' data-testid={`quarantined-${row.id}`} label={label} size='small' />
        </Tooltip>
      );
    }

    return <Chip color='error' data-testid={`quarantined-${row?.id}`} label={label} size='small' />;
  }

  return state;
};

const tableStructure: ITableStructure<ISwarmWorker>[] = [
  { key: 'id', title: 'Worker' },
  { key: 'state', title: 'State', formatter: renderState },
  { key: 'userCount', title: '# users' },
  { key: 'cpuUsage', title: 'CPU usage' },
  { key: 'memoryUsage', title: 'Memory usage', formatter: content => formatBytes(content) },
];

function WorkersTable({ workers = [] }: { workers?: ISwarmWorker[] }) {
  return <Table<ISwarmWorker> defaultSortKey='id' rows={workers} structure={tableStructure} />;
}

const storeConnector = ({ ui: { workers } }: IRootState) => ({ workers });

export default connect(storeConnector)(WorkersTable);
