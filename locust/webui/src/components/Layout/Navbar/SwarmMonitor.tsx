import { Box, Chip, Divider, Tooltip, Typography } from '@mui/material';
import { connect } from 'react-redux';

import { SWARM_STATE } from 'constants/swarm';
import { IUiState } from 'redux/slice/ui.slice';
import { IRootState } from 'redux/store';
import { ISwarmState } from 'types/swarm.types';

interface ISwarmMonitor
  extends
    Pick<
      ISwarmState,
      'isDistributed' | 'host' | 'state' | 'workerCount' | 'quarantinedWorkerCount'
    >,
    Pick<IUiState, 'currentRps' | 'failRatio' | 'userCount'> {}

function SwarmMonitor({
  isDistributed,
  state,
  host,
  currentRps,
  failRatio,
  userCount,
  workerCount,
  quarantinedWorkerCount = 0,
}: ISwarmMonitor) {
  return (
    <Box
      sx={{ display: 'flex', columnGap: 2, rowGap: 1, flexDirection: { xs: 'column', md: 'row' } }}
    >
      <Box sx={{ display: 'flex', flexDirection: 'column' }}>
        <Typography sx={{ fontWeight: 'bold' }}>Host</Typography>
        <Tooltip title={host}>
          <Typography
            noWrap
            sx={{
              overflow: 'hidden',
              textOverflow: 'ellipsis',

              maxWidth: { xs: '180px', lg: '400px' },
            }}
          >
            {host}
          </Typography>
        </Tooltip>
      </Box>
      <Divider flexItem orientation='vertical' />
      <Box sx={{ display: 'flex', flexDirection: 'column' }}>
        <Typography sx={{ fontWeight: 'bold' }}>Status</Typography>
        <Typography variant='button'>{state}</Typography>
      </Box>
      {(state === SWARM_STATE.RUNNING || state === SWARM_STATE.SPAWNING) && (
        <>
          <Divider flexItem orientation='vertical' />
          <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: { md: 'center' } }}>
            <Typography sx={{ fontWeight: 'bold' }}>Users</Typography>
            <Typography noWrap variant='button'>
              {userCount}
            </Typography>
          </Box>
        </>
      )}
      {isDistributed && (
        <>
          <Divider flexItem orientation='vertical' />
          <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: { md: 'center' } }}>
            <Typography sx={{ fontWeight: 'bold' }}>Workers</Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', columnGap: 1 }}>
              <Typography noWrap variant='button'>
                {workerCount}
              </Typography>
              {quarantinedWorkerCount > 0 && (
                <Tooltip title='Workers quarantined because their execution manifest (locustfile, user classes or version) does not match the master. They are not generating traffic.'>
                  <Chip
                    color='error'
                    data-testid='quarantined-workers-chip'
                    label={`${quarantinedWorkerCount} quarantined`}
                    size='small'
                    variant='outlined'
                  />
                </Tooltip>
              )}
            </Box>
          </Box>
        </>
      )}
      <Divider flexItem orientation='vertical' />
      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: { md: 'center' } }}>
        <Typography sx={{ fontWeight: 'bold' }}>RPS</Typography>
        <Typography noWrap variant='button'>
          {currentRps}
        </Typography>
      </Box>
      <Divider flexItem orientation='vertical' />
      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: { md: 'center' } }}>
        <Typography sx={{ fontWeight: 'bold' }}>Failures</Typography>
        <Typography noWrap variant='button'>{`${failRatio}%`}</Typography>
      </Box>
    </Box>
  );
}

const storeConnector = ({
  swarm: { isDistributed, state, host, workerCount, quarantinedWorkerCount },
  ui: { currentRps, failRatio, userCount },
}: IRootState) => ({
  isDistributed,
  state,
  host,
  currentRps,
  failRatio,
  userCount,
  workerCount,
  quarantinedWorkerCount,
});

export default connect(storeConnector)(SwarmMonitor);
