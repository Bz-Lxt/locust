import { act, fireEvent, waitFor } from '@testing-library/react';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { beforeAll, afterAll, describe, test, expect, vi, afterEach } from 'vitest';

import ResetButton from 'components/StateButtons/ResetButton';
import { TEST_BASE_API } from 'test/constants';
import { renderWithProvider } from 'test/testUtils';

const resetStats = vi.fn(() => HttpResponse.json({ success: true }));

const server = setupServer(
  http.get(`${TEST_BASE_API}/stats/reset`, resetStats,
  ),
);


describe('ResetButton', () => {
  beforeAll(() => server.listen());
  afterEach(() => {
    server.resetHandlers();
    resetStats.mockClear();
  });
  afterAll(() => server.close());

  test('should call resetStats on ResetButton click', async () => {
    const { getByText } = renderWithProvider(<ResetButton />);

    act(() => {
      fireEvent.click(getByText('Reset'));
    });

    await waitFor(async () => {
      expect(resetStats).toHaveBeenCalled();
    })
  });

  test('should clear chart series and add a boundary marker after successful reset', async () => {
    const oldTime = '2024-01-01T00:00:00.000Z';
    const initialState = {
      ui: {
        charts: {
          currentRps: [[oldTime, 10]],
          currentFailPerSec: [[oldTime, 1]],
          totalAvgResponseTime: [[oldTime, 100]],
          userCount: [[oldTime, 5]],
          time: [oldTime],
        },
      },
    };

    const { getByText, store } = renderWithProvider(<ResetButton />, initialState);

    act(() => {
      fireEvent.click(getByText('Reset'));
    });

    await waitFor(() => {
      expect(resetStats).toHaveBeenCalled();
    });

    await waitFor(() => {
      const { charts } = store.getState().ui;
      expect(charts.currentRps).toEqual([]);
      expect(charts.currentFailPerSec).toEqual([]);
      expect(charts.totalAvgResponseTime).toEqual([]);
      expect(charts.userCount).toEqual([]);
      expect(charts.time).toEqual([]);
      expect(charts.markers).toHaveLength(1);
    });

    // marker is an ISO timestamp at the reset boundary
    const marker = store.getState().ui.charts.markers![0];
    expect(new Date(marker).toString()).not.toBe('Invalid Date');
    expect(new Date(marker).getTime()).toBeGreaterThanOrEqual(new Date(oldTime).getTime());
  });
});
