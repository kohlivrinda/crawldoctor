import React, { useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import { analyticsAPI } from '../utils/api';

const getDefaultEndDate = () => new Date().toISOString().split('T')[0];
const getDefaultStartDate = () => {
  const date = new Date();
  date.setDate(date.getDate() - 30);
  return date.toISOString().split('T')[0];
};

const Funnels: React.FC = () => {
  const queryClient = useQueryClient();
  const [startDate, setStartDate] = useState(getDefaultStartDate());
  const [endDate, setEndDate] = useState(getDefaultEndDate());
  const [showConfig, setShowConfig] = useState(false);
  const [funnelConfigDraft, setFunnelConfigDraft] = useState<any[]>([]);
  const [selectedFunnelKey, setSelectedFunnelKey] = useState<string>('');
  const [selectedStepIndex, setSelectedStepIndex] = useState<number>(0);
  const [selectedStageIndex, setSelectedStageIndex] = useState<number>(0);
  const [userView, setUserView] = useState<'reached' | 'dropoff'>('reached');
  const [showCapturedOnly, setShowCapturedOnly] = useState(false);

  const { data: funnelSummary, isLoading: funnelLoading } = useQuery(
    ['funnel-summary', startDate, endDate],
    () => analyticsAPI.getFunnelSummary(startDate, endDate),
    { refetchInterval: 60000 }
  );

  const { data: funnelConfig } = useQuery(
    ['funnel-config'],
    () => analyticsAPI.getFunnelConfig(),
    { refetchInterval: 60000 }
  );

  useEffect(() => {
    if (funnelConfig?.funnels) {
      setFunnelConfigDraft(funnelConfig.funnels);
    }
  }, [funnelConfig]);

  useEffect(() => {
    if (!selectedFunnelKey && funnelSummary?.funnels?.length) {
      setSelectedFunnelKey(funnelSummary.funnels[0].key || funnelSummary.funnels[0].label);
    }
  }, [funnelSummary, selectedFunnelKey]);

  const timingQuery = useQuery(
    ['funnel-timing', selectedFunnelKey, startDate, endDate],
    () => analyticsAPI.getFunnelTiming(selectedFunnelKey, startDate, endDate),
    { enabled: Boolean(selectedFunnelKey) }
  );

  const dropoffQuery = useQuery(
    ['funnel-dropoffs', selectedFunnelKey, selectedStepIndex, startDate, endDate],
    () => analyticsAPI.getFunnelDropoffs(selectedFunnelKey, selectedStepIndex, 50, 0, startDate, endDate),
    { enabled: Boolean(selectedFunnelKey) && userView === 'dropoff' }
  );

  const stageUsersQuery = useQuery(
    ['funnel-stage-users', selectedFunnelKey, selectedStageIndex, startDate, endDate],
    () => analyticsAPI.getFunnelStageUsers(selectedFunnelKey, selectedStageIndex, 50, 0, startDate, endDate),
    { enabled: Boolean(selectedFunnelKey) && userView === 'reached' }
  );

  const saveFunnelConfigMutation = useMutation(
    (config: any) => analyticsAPI.saveFunnelConfig(config),
    {
      onSuccess: () => {
        queryClient.invalidateQueries('funnel-summary');
        queryClient.invalidateQueries('funnel-config');
        setShowConfig(false);
      },
      onError: () => {
        alert('Failed to save funnel configuration.');
      }
    }
  );

  const funnelOptions = useMemo(() => {
    return (funnelSummary?.funnels || []).map((f: any) => ({
      key: f.key || f.label,
      label: f.label,
      stages: f.stages,
    }));
  }, [funnelSummary]);

  const selectedFunnel = useMemo(() => {
    return funnelOptions.find((f: any) => f.key === selectedFunnelKey);
  }, [funnelOptions, selectedFunnelKey]);

  const renderCapturedSummary = (user: any) => {
    const data = user.captured_data;
    if (!data) return 'No captured data';
    if (typeof data === 'string') {
      try {
        const parsed = JSON.parse(data);
        const entries = Object.entries(parsed || {}).slice(0, 3);
        return entries.length ? entries.map(([key, value]) => `${key}: ${value}`).join(', ') : 'Captured data available';
      } catch {
        return data;
      }
    }
    if (typeof data === 'object') {
      const entries = Object.entries(data).slice(0, 3);
      return entries.length ? entries.map(([key, value]) => `${key}: ${value}`).join(', ') : 'Captured data available';
    }
    return 'Captured data available';
  };

  const updateFunnel = (index: number, updates: any) => {
    setFunnelConfigDraft((prev: any[]) => prev.map((f, i) => (i === index ? { ...f, ...updates } : f)));
  };

  const updateStep = (funnelIndex: number, stepIndex: number, updates: any) => {
    setFunnelConfigDraft((prev: any[]) => prev.map((f, i) => {
      if (i !== funnelIndex) return f;
      const steps = (f.steps || []).map((s: any, j: number) => (j === stepIndex ? { ...s, ...updates } : s));
      return { ...f, steps };
    }));
  };

  const addFunnel = () => {
    const id = Date.now();
    setFunnelConfigDraft((prev: any[]) => ([
      ...prev,
      {
        key: `custom_${id}`,
        label: 'New Funnel',
        steps: [{ label: 'Visited /path', type: 'page', path: '/' }]
      }
    ]));
  };

  const removeFunnel = (index: number) => {
    setFunnelConfigDraft((prev: any[]) => prev.filter((_, i) => i !== index));
  };

  const addStep = (funnelIndex: number) => {
    setFunnelConfigDraft((prev: any[]) => prev.map((f, i) => {
      if (i !== funnelIndex) return f;
      const steps = [...(f.steps || []), { label: 'Submitted form', type: 'event', path: '/', event_type: 'form_submit' }];
      return { ...f, steps };
    }));
  };

  const removeStep = (funnelIndex: number, stepIndex: number) => {
    setFunnelConfigDraft((prev: any[]) => prev.map((f, i) => {
      if (i !== funnelIndex) return f;
      const steps = (f.steps || []).filter((_: any, j: number) => j !== stepIndex);
      return { ...f, steps };
    }));
  };

  return (
    <div className="space-y-8">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Funnels</h2>
          <p className="text-sm text-gray-500">Track conversion paths and configure drop-off insights.</p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">From</label>
            <input
              type="date"
              value={startDate}
              max={endDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-1 text-sm"
            />
          </div>
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">To</label>
            <input
              type="date"
              value={endDate}
              min={startDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-1 text-sm"
            />
          </div>
          <button
            onClick={() => setShowConfig(true)}
            className="px-3 py-2 text-xs font-semibold rounded-md bg-gray-100 text-gray-700 hover:bg-gray-200"
          >
            Configure Funnels
          </button>
        </div>
      </div>

      <div className="bg-white p-6 rounded-lg shadow">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-xl font-semibold text-gray-800">Conversion Funnels</h3>
          {funnelLoading && <span className="text-sm text-gray-400">Loading...</span>}
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {(funnelSummary?.funnels || []).map((funnel: any) => (
            <div key={funnel.key || funnel.label} className="border border-gray-200 rounded-lg p-4">
              <div className="text-sm font-semibold text-gray-700 mb-3">{funnel.label}</div>
              <div className="space-y-3">
                {funnel.stages.map((stage: any, idx: number) => (
                  <div key={`${funnel.key || funnel.label}-${idx}`}>
                    <div className="flex justify-between text-xs text-gray-500">
                      <span>{stage.label}</span>
                      <span className="font-semibold text-gray-700">{stage.count}</span>
                    </div>
                    <div className="w-full bg-gray-100 rounded-full h-2 mt-1">
                      <div
                        className="h-2 rounded-full bg-blue-500"
                        style={{
                          width: funnel.stages[0]?.count ? `${Math.round((stage.count / funnel.stages[0].count) * 100)}%` : '0%'
                        }}
                      ></div>
                    </div>
                  </div>
                ))}
                <div className="pt-2 text-xs text-gray-600 space-y-1">
                  {(funnel.rates || []).map((rate: any) => (
                    <div key={rate.label} className="flex items-center justify-between">
                      <span>{rate.label}</span>
                      <span className="font-semibold">{rate.rate}%</span>
                      <span className="text-gray-400">Drop-off: {rate.dropoff_count ?? 0}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ))}
          {(!funnelSummary?.funnels || funnelSummary.funnels.length === 0) && (
            <div className="text-sm text-gray-400">No funnel data available.</div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white p-6 rounded-lg shadow">
          <h3 className="text-lg font-semibold text-gray-800 mb-4">Time-to-Convert</h3>
          <div className="flex items-center gap-3 mb-4">
            <label className="text-sm text-gray-600">Funnel</label>
            <select
              value={selectedFunnelKey}
              onChange={(e) => {
                setSelectedFunnelKey(e.target.value);
                setSelectedStepIndex(0);
                setSelectedStageIndex(0);
              }}
              className="border border-gray-300 rounded-md px-3 py-1 text-sm"
            >
              {funnelOptions.map((f: any) => (
                <option key={f.key} value={f.key}>{f.label}</option>
              ))}
            </select>
          </div>
          <div className="space-y-3 text-sm">
            {(timingQuery.data?.transitions || []).map((transition: any) => (
              <div key={`${transition.from}-${transition.to}`} className="border rounded-md p-3">
                <div className="font-semibold text-gray-700">{transition.from} → {transition.to}</div>
                <div className="text-xs text-gray-500">Samples: {transition.sample_size}</div>
                <div className="mt-2 grid grid-cols-3 gap-2 text-xs">
                  <div>
                    <div className="text-gray-400">Avg</div>
                    <div className="font-semibold text-gray-700">{transition.avg_seconds}s</div>
                  </div>
                  <div>
                    <div className="text-gray-400">Median</div>
                    <div className="font-semibold text-gray-700">{transition.median_seconds}s</div>
                  </div>
                  <div>
                    <div className="text-gray-400">P90</div>
                    <div className="font-semibold text-gray-700">{transition.p90_seconds}s</div>
                  </div>
                </div>
              </div>
            ))}
            {timingQuery.data?.transitions?.length === 0 && (
              <div className="text-sm text-gray-400">No timing data available.</div>
            )}
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg shadow">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-800">Funnel Users</h3>
            <div className="flex items-center gap-2 text-xs">
              <button
                onClick={() => setUserView('reached')}
                className={`px-3 py-1 rounded-full border ${userView === 'reached' ? 'bg-blue-600 text-white border-blue-600' : 'text-gray-600 border-gray-300'}`}
              >
                Reached Stage
              </button>
              <button
                onClick={() => setUserView('dropoff')}
                className={`px-3 py-1 rounded-full border ${userView === 'dropoff' ? 'bg-blue-600 text-white border-blue-600' : 'text-gray-600 border-gray-300'}`}
              >
                Drop-offs
              </button>
            </div>
          </div>
          <div className="flex items-center gap-3 mb-4">
            <label className="text-sm text-gray-600">Step</label>
            {userView === 'reached' ? (
              <select
                value={selectedStageIndex}
                onChange={(e) => setSelectedStageIndex(Number(e.target.value))}
                className="border border-gray-300 rounded-md px-3 py-1 text-sm"
              >
                {(selectedFunnel?.stages || []).map((stage: any, idx: number) => (
                  <option key={`${stage.label}-${idx}`} value={idx}>{stage.label}</option>
                ))}
              </select>
            ) : (
              <select
                value={selectedStepIndex}
                onChange={(e) => setSelectedStepIndex(Number(e.target.value))}
                className="border border-gray-300 rounded-md px-3 py-1 text-sm"
              >
                {(selectedFunnel?.stages || []).slice(0, -1).map((stage: any, idx: number) => (
                  <option key={`${stage.label}-${idx}`} value={idx}>After {stage.label}</option>
                ))}
              </select>
            )}
            {userView === 'reached' && (
              <label className="flex items-center gap-2 text-xs text-gray-600">
                <input
                  type="checkbox"
                  checked={showCapturedOnly}
                  onChange={(e) => setShowCapturedOnly(e.target.checked)}
                />
                With captured data
              </label>
            )}
          </div>
          <div className="space-y-3 text-sm">
            {userView === 'reached' && (stageUsersQuery.data?.users || [])
              .filter((user: any) => (!showCapturedOnly ? true : user.has_captured_data))
              .map((user: any) => (
              <div key={user.client_id} className="border rounded-md p-3">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <div className="font-mono text-xs text-gray-600">{user.client_id}</div>
                    <div className="text-xs text-gray-500 mt-1">{user.source} / {user.medium} / {user.campaign}</div>
                    <div className="text-xs text-gray-500">Entry: {user.entry_page}</div>
                    {user.entry_referrer && (
                      <div className="text-xs text-gray-400">Referrer: {user.entry_referrer}</div>
                    )}
                    {user.stage_reached_at && (
                      <div className="text-xs text-gray-400">Reached: {user.stage_reached_at}</div>
                    )}
                  </div>
                  <div className="text-xs text-gray-500 text-right">
                    {user.email && <div>{user.email}</div>}
                    {user.name && <div>{user.name}</div>}
                    <div>{user.first_seen} → {user.last_seen}</div>
                  </div>
                </div>
                {user.path_sequence && (
                  <div className="text-xs text-gray-500 mt-2">Journey: {user.path_sequence}</div>
                )}
                <div className="text-xs text-gray-500 mt-1">Captured: {renderCapturedSummary(user)}</div>
              </div>
            ))}
            {userView === 'dropoff' && (dropoffQuery.data?.users || []).map((user: any) => (
              <div key={user.client_id} className="border rounded-md p-3">
                <div className="font-mono text-xs text-gray-600">{user.client_id}</div>
                <div className="text-xs text-gray-500 mt-1">{user.source} / {user.medium} / {user.campaign}</div>
                <div className="text-xs text-gray-500">Entry: {user.entry_page}</div>
                <div className="text-xs text-gray-400">{user.first_seen} → {user.last_seen}</div>
              </div>
            ))}
            {userView === 'reached' && stageUsersQuery.data?.users?.filter((user: any) => (!showCapturedOnly ? true : user.has_captured_data)).length === 0 && (
              <div className="text-sm text-gray-400">No users reached this stage.</div>
            )}
            {userView === 'dropoff' && dropoffQuery.data?.users?.length === 0 && (
              <div className="text-sm text-gray-400">No drop-offs for this step.</div>
            )}
          </div>
        </div>
      </div>

      {showConfig && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-10 mx-auto p-6 border w-11/12 md:w-3/4 shadow-lg rounded-md bg-white">
            <div className="flex justify-between items-center mb-4">
              <div>
                <h3 className="text-lg font-semibold text-gray-900">Configure Funnels</h3>
                <p className="text-sm text-gray-500">Define the conversion steps you want tracked.</p>
              </div>
              <button onClick={() => setShowConfig(false)} className="text-gray-600 hover:text-gray-800">Close</button>
            </div>

            <div className="space-y-6 max-h-[65vh] overflow-y-auto pr-2">
              {funnelConfigDraft.map((funnel, idx) => (
                <div key={funnel.key || idx} className="border rounded-lg p-4">
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1 space-y-2">
                      <div>
                        <label className="text-xs font-semibold text-gray-500 uppercase">Funnel Label</label>
                        <input
                          value={funnel.label || ''}
                          onChange={(e) => updateFunnel(idx, { label: e.target.value })}
                          className="mt-1 w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
                        />
                      </div>
                      <div>
                        <label className="text-xs font-semibold text-gray-500 uppercase">Key</label>
                        <input
                          value={funnel.key || ''}
                          onChange={(e) => updateFunnel(idx, { key: e.target.value })}
                          className="mt-1 w-full border border-gray-300 rounded-md px-3 py-2 text-sm font-mono"
                        />
                      </div>
                    </div>
                    <button
                      onClick={() => removeFunnel(idx)}
                      className="px-3 py-1 text-xs font-semibold rounded-md bg-red-100 text-red-700"
                    >
                      Remove Funnel
                    </button>
                  </div>

                  <div className="mt-4 space-y-3">
                    {(funnel.steps || []).map((step: any, stepIdx: number) => (
                      <div key={`${funnel.key}-${stepIdx}`} className="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
                        <div className="md:col-span-2">
                          <label className="text-xs font-semibold text-gray-500 uppercase">Step Label</label>
                          <input
                            value={step.label || ''}
                            onChange={(e) => updateStep(idx, stepIdx, { label: e.target.value })}
                            className="mt-1 w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
                          />
                        </div>
                        <div>
                          <label className="text-xs font-semibold text-gray-500 uppercase">Type</label>
                          <select
                            value={step.type || 'page'}
                            onChange={(e) => updateStep(idx, stepIdx, { type: e.target.value })}
                            className="mt-1 w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
                          >
                            <option value="page">Page Visit</option>
                            <option value="event">Event</option>
                          </select>
                        </div>
                        <div>
                          <label className="text-xs font-semibold text-gray-500 uppercase">Path</label>
                          <input
                            value={step.path || ''}
                            onChange={(e) => updateStep(idx, stepIdx, { path: e.target.value })}
                            className="mt-1 w-full border border-gray-300 rounded-md px-3 py-2 text-sm font-mono"
                          />
                        </div>
                        <div>
                          <label className="text-xs font-semibold text-gray-500 uppercase">Event Type</label>
                          <input
                            value={step.event_type || ''}
                            onChange={(e) => updateStep(idx, stepIdx, { event_type: e.target.value })}
                            disabled={step.type !== 'event'}
                            className="mt-1 w-full border border-gray-300 rounded-md px-3 py-2 text-sm disabled:bg-gray-100"
                          />
                        </div>
                        <div className="md:col-span-5 flex justify-end">
                          <button
                            onClick={() => removeStep(idx, stepIdx)}
                            className="text-xs text-red-600 hover:text-red-800"
                          >
                            Remove Step
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>

                  <div className="mt-4">
                    <button
                      onClick={() => addStep(idx)}
                      className="px-3 py-1 text-xs font-semibold rounded-md bg-blue-100 text-blue-700"
                    >
                      Add Step
                    </button>
                  </div>
                </div>
              ))}

              <button
                onClick={addFunnel}
                className="px-4 py-2 text-sm font-semibold rounded-md bg-gray-200 text-gray-700"
              >
                Add Funnel
              </button>
            </div>

            <div className="mt-6 flex justify-end gap-3">
              <button
                onClick={() => setShowConfig(false)}
                className="px-4 py-2 text-sm font-medium rounded-md bg-gray-100 text-gray-700"
              >
                Cancel
              </button>
              <button
                onClick={() => saveFunnelConfigMutation.mutate({ funnels: funnelConfigDraft })}
                disabled={saveFunnelConfigMutation.isLoading}
                className="px-4 py-2 text-sm font-semibold rounded-md bg-blue-600 text-white disabled:opacity-60"
              >
                {saveFunnelConfigMutation.isLoading ? 'Saving...' : 'Save Funnels'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Funnels;
