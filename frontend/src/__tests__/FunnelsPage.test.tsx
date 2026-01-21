import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from 'react-query';
import { MemoryRouter } from 'react-router-dom';
import Funnels from '../pages/Funnels';

jest.mock('../utils/api', () => ({
  analyticsAPI: {
    getFunnelSummary: jest.fn(),
    getFunnelConfig: jest.fn(),
    saveFunnelConfig: jest.fn(),
    getFunnelTiming: jest.fn(),
    getFunnelDropoffs: jest.fn(),
  }
}));

const { analyticsAPI: mockAnalytics } = jest.requireMock('../utils/api');

const renderPage = async () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <Funnels />
      </MemoryRouter>
    </QueryClientProvider>
  );
};

describe('Funnels page', () => {
  beforeEach(() => {
    mockAnalytics.getFunnelSummary.mockResolvedValue({
      funnels: [
        {
          key: 'demo',
          label: 'Demo Funnel',
          stages: [
            { label: 'Visited /demo', count: 10 },
            { label: 'Submitted form', count: 2 },
          ],
          rates: [
            { label: 'Visited /demo → Submitted form', rate: 20, dropoff_count: 8 },
          ],
        },
      ],
    });
    mockAnalytics.getFunnelConfig.mockResolvedValue({
      funnels: [
        {
          key: 'demo',
          label: 'Demo Funnel',
          steps: [
            { label: 'Visited /demo', type: 'page', path: '/demo' },
            { label: 'Submitted form', type: 'event', path: '/demo', event_type: 'form_submit' },
          ],
        },
      ],
    });
    mockAnalytics.getFunnelTiming.mockResolvedValue({
      transitions: [
        { from: 'Visited /demo', to: 'Submitted form', sample_size: 2, avg_seconds: 30, median_seconds: 25, p90_seconds: 40 },
      ],
    });
    mockAnalytics.getFunnelDropoffs.mockResolvedValue({ users: [] });
    mockAnalytics.saveFunnelConfig.mockResolvedValue({});
  });

  it('renders funnel analytics and saves config changes', async () => {
    await renderPage();

    const labels = await screen.findAllByText('Demo Funnel');
    expect(labels.length).toBeGreaterThan(0);
    expect(screen.getByText('Drop-off: 8')).toBeInTheDocument();

    const user = userEvent.setup();
    const configureButton = screen.getByRole('button', { name: /configure funnels/i });
    await user.click(configureButton);

    const labelInputs = await screen.findAllByDisplayValue('Demo Funnel');
    const labelInput = labelInputs.find((el) => el.tagName === 'INPUT') as HTMLInputElement;
    await user.clear(labelInput);
    await user.type(labelInput, 'Demo Funnel Updated');

    const saveButton = screen.getByRole('button', { name: /save funnels/i });
    await user.click(saveButton);

    await waitFor(() => {
      expect(mockAnalytics.saveFunnelConfig).toHaveBeenCalled();
    });
  });
});
