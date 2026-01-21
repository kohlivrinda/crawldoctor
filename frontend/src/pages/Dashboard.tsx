import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from 'react-query';
import { analyticsAPI } from '../utils/api';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#F97316'];

const Dashboard: React.FC = () => {
  // Calculate default dates: last 7 days for faster initial load
  const getDefaultEndDate = () => {
    const today = new Date();
    return today.toISOString().split('T')[0];
  };
  
  const getDefaultStartDate = () => {
    const date = new Date();
    date.setDate(date.getDate() - 7);
    return date.toISOString().split('T')[0];
  };
  
  const [startDate, setStartDate] = useState(getDefaultStartDate());
  const [endDate, setEndDate] = useState(getDefaultEndDate());
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [jumpTo, setJumpTo] = useState<string>('');
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const queryClient = useQueryClient();
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  
  const SessionTimeline: React.FC<{ sessionId: string }> = ({ sessionId }) => {
    const { data, isLoading } = useQuery(
      ['dashboard-session-detail', sessionId],
      () => analyticsAPI.getSessionDetail(sessionId),
      { enabled: !!sessionId }
    );
    if (isLoading) return <div className="p-4">Loading...</div>;
    if (!data) return <div className="p-4">No data</div>;
    return (
      <div className="space-y-4 max-h-[70vh] overflow-y-auto">
        {data.timeline.map((item: any) => (
          <div key={`${item.type}-${item.id}`} className="border rounded p-3">
            <div className="text-xs text-gray-500">{item.timestamp ? new Date(item.timestamp).toLocaleString() : '—'}</div>
            <div className="flex items-center space-x-2">
              <div className="text-sm font-semibold capitalize">{item.type === 'visit' ? 'Page View' : item.event_type}</div>
              {item.crawler_type && (
                <span className={`inline-flex px-2 py-0.5 text-xs rounded-full ${item.is_bot ? 'bg-red-100 text-red-800' : 'bg-green-100 text-green-800'}`}>
                  {item.crawler_type}
                </span>
              )}
              {(item.source || item.medium || item.campaign) && (
                <span className="inline-flex px-2 py-0.5 text-xs rounded-full bg-blue-100 text-blue-800">
                  {(item.source || '—')}/{(item.medium || '—')}/{(item.campaign || '—')}
                </span>
              )}
              {(item.country || item.city) && (
                <span className="inline-flex px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-800">
                  {(item.city || '')}{item.city && item.country ? ', ' : ''}{item.country || ''}
                </span>
              )}
              {(item.tracking_id || item.tracking_method) && (
                <span className="inline-flex px-2 py-0.5 text-xs rounded-full bg-purple-100 text-purple-800">
                  {(item.tracking_method || '—')}{item.tracking_id ? `:${item.tracking_id}` : ''}
                </span>
              )}
            </div>
            {item.page_url && (
              <div className="text-sm text-blue-700 break-words">
                <a href={item.page_url} target="_blank" rel="noopener noreferrer" className="underline">{item.page_url}</a>
              </div>
            )}
            {item.type !== 'visit' && (
              <pre className="text-xs bg-gray-50 p-2 rounded overflow-auto">{JSON.stringify(item.data, null, 2)}</pre>
            )}
          </div>
        ))}
      </div>
    );
  };

  // Fetch analytics data with error handling
  const { data: visitorSummary, isLoading: summaryLoading, error: summaryError } = useQuery(
    ['visitor-summary', startDate, endDate],
    () => analyticsAPI.getVisitorSummary(startDate, endDate),
    { 
      refetchInterval: 60000, // Refresh every minute
      retry: 3,
      retryDelay: 1000,
      onError: (error) => console.error('Visitor summary error:', error)
    }
  );

  const { data: funnelSummary, isLoading: funnelLoading } = useQuery(
    ['funnel-summary', startDate, endDate],
    () => analyticsAPI.getFunnelSummary(startDate, endDate),
    {
      refetchInterval: 60000,
      retry: 2,
      retryDelay: 1000,
    }
  );


  const { data: pageAnalytics, error: pageError } = useQuery(
    ['page-analytics', startDate, endDate],
    () => analyticsAPI.getPageAnalytics(startDate, endDate),
    { 
      refetchInterval: 60000,
      retry: 3,
      retryDelay: 1000,
      onError: (error) => console.error('Page analytics error:', error)
    }
  );

  const { data: recentActivity, error: activityError } = useQuery(
    ['recent-activity', currentPage, pageSize, startDate, endDate],
    () => analyticsAPI.getRecentActivity(pageSize, (currentPage - 1) * pageSize, startDate, endDate),
    { 
      refetchInterval: 30000, // Refresh every 30 seconds
      retry: 3,
      retryDelay: 1000,
      onError: (error) => console.error('Recent activity error:', error)
    }
  );

  // Session data moved to Sessions page

  const { data: categories, error: categoriesError } = useQuery(
    ['visitor-categories', startDate, endDate],
    () => analyticsAPI.getVisitorCategories(startDate, endDate),
    { 
      refetchInterval: 60000,
      retry: 3,
      retryDelay: 1000,
      onError: (error) => console.error('Visitor categories error:', error)
    }
  );

  // CSV Export mutation
  const exportMutation = useMutation(
    () => analyticsAPI.exportVisitsCSV(startDate, endDate),
    {
      onSuccess: (response) => {
        const blob = new Blob([response.data], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `crawldoctor_visits_${startDate}_to_${endDate}.csv`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
      },
      onError: (error) => {
        console.error('Export failed:', error);
        alert('Export failed. For very large datasets, try a shorter date range.');
      }
    }
  );

  const exportEventsMutation = useMutation(
    () => analyticsAPI.exportEventsCSV(startDate, endDate),
    {
      onSuccess: (response) => {
        const blob = new Blob([response.data], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `crawldoctor_events_${startDate}_to_${endDate}.csv`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
      },
      onError: (error) => {
        console.error('Export events failed:', error);
        alert('Export failed. For very large datasets, try a shorter date range.');
      }
    }
  );

  // Delete all visits mutation
  const deleteMutation = useMutation(analyticsAPI.deleteAllVisits, {
    onSuccess: () => {
      queryClient.invalidateQueries(); // Refresh all data
      setShowDeleteConfirm(false);
      alert('All data deleted successfully');
    },
    onError: (error) => {
      console.error('Delete failed:', error);
      alert('Failed to delete data');
    }
  });


  const handleExport = () => {
    exportMutation.mutate();
  };

  const handleDeleteAll = () => {
    deleteMutation.mutate();
  };


  // Show errors if any critical queries fail
  if (summaryError || pageError || activityError || categoriesError) {
    return (
      <div className="p-6 max-w-4xl mx-auto">
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <h2 className="text-red-800 font-semibold">Dashboard Error</h2>
          <p className="text-red-600 mt-2">
            Unable to load dashboard data. Please try refreshing the page.
          </p>
          <button 
            onClick={() => window.location.reload()} 
            className="mt-3 bg-red-600 text-white px-4 py-2 rounded hover:bg-red-700"
          >
            Reload Dashboard
          </button>
        </div>
      </div>
    );
  }

  // Show partial loading state - don't block entire dashboard

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <h1 className="text-3xl font-bold text-gray-900">CrawlDoctor Dashboard</h1>
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2">
            <label className="text-sm font-medium text-gray-700">Start:</label>
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              max={endDate}
              className="border border-gray-300 rounded-md px-3 py-2"
            />
          </div>
          <div className="flex items-center space-x-2">
            <label className="text-sm font-medium text-gray-700">End:</label>
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              min={startDate}
              className="border border-gray-300 rounded-md px-3 py-2"
            />
          </div>
          <select
            value={pageSize}
            onChange={(e) => { setPageSize(Number(e.target.value)); setCurrentPage(1); }}
            className="border border-gray-300 rounded-md px-3 py-2"
          >
            <option value={10}>10 / page</option>
            <option value={20}>20 / page</option>
            <option value={50}>50 / page</option>
            <option value={100}>100 / page</option>
          </select>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-6 gap-6">
        <div className="bg-white p-6 rounded-lg shadow border-l-4 border-blue-500">
          <h3 className="text-sm font-semibold text-gray-500 uppercase">Total Visits</h3>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {summaryLoading ? '...' : (visitorSummary?.total_visits || 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-white p-6 rounded-lg shadow border-l-4 border-green-500">
          <h3 className="text-sm font-semibold text-gray-500 uppercase">Unique Visitors</h3>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {summaryLoading ? '...' : (visitorSummary?.unique_visitors || 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-white p-6 rounded-lg shadow border-l-4 border-indigo-500">
          <h3 className="text-sm font-semibold text-gray-500 uppercase text-indigo-600">Conversions</h3>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {summaryLoading ? '...' : (visitorSummary?.conversions || 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-white p-6 rounded-lg shadow border-l-4 border-yellow-500">
          <h3 className="text-sm font-semibold text-gray-500 uppercase text-yellow-600">Conv. Rate</h3>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {summaryLoading ? '...' : `${visitorSummary?.conversion_rate || 0}%`}
          </p>
        </div>
        <div className="bg-white p-6 rounded-lg shadow border-l-4 border-orange-500">
          <h3 className="text-sm font-semibold text-gray-500 uppercase">AI Crawlers</h3>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {summaryLoading ? '...' : (visitorSummary?.visits_by_category?.find((c: any) => c.category === 'AI Crawlers')?.count || 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-white p-6 rounded-lg shadow border-l-4 border-purple-500">
          <h3 className="text-sm font-semibold text-gray-500 uppercase">Human Visitors</h3>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {summaryLoading ? '...' : (
              (visitorSummary?.visits_by_category?.find((c: any) => c.category === 'Desktop Humans')?.count || 0) +
              (visitorSummary?.visits_by_category?.find((c: any) => c.category === 'Mobile Humans')?.count || 0)
            ).toLocaleString()}
          </p>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Visitor Categories Pie Chart */}
        <div className="bg-white p-6 rounded-lg shadow">
          <h3 className="text-xl font-semibold text-gray-800 mb-4">Visitor Categories</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={categories?.categories || []}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="count"
              >
                {categories?.categories?.map((entry: any, index: number) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Page Visits Bar Chart */}
        <div className="bg-white p-6 rounded-lg shadow">
          <h3 className="text-xl font-semibold text-gray-800 mb-4">Top Pages</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={pageAnalytics?.page_visits?.slice(0, 10) || []}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="domain" angle={-45} textAnchor="end" height={80} />
              <YAxis />
              <Tooltip />
              <Bar dataKey="total_visits" fill="#3B82F6" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Acquisition Insights */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white p-6 rounded-lg shadow">
          <h3 className="text-xl font-semibold text-gray-800 mb-4">Top Sources</h3>
          <div className="space-y-2">
            {(visitorSummary?.top_sources || []).map((s: any) => (
              <div key={s.source} className="flex items-center justify-between">
                <span className="text-sm text-gray-700 font-medium">{s.source}</span>
                <span className="text-sm text-gray-500">{s.count}</span>
              </div>
            ))}
            {(!visitorSummary?.top_sources || visitorSummary.top_sources.length === 0) && (
              <div className="text-sm text-gray-400">No source data available.</div>
            )}
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg shadow">
          <h3 className="text-xl font-semibold text-gray-800 mb-4">Top Campaigns</h3>
          <div className="space-y-2">
            {(visitorSummary?.top_campaigns || []).map((c: any) => (
              <div key={c.campaign} className="flex items-center justify-between">
                <span className="text-sm text-gray-700 font-medium truncate max-w-[200px]" title={c.campaign}>{c.campaign}</span>
                <span className="text-sm text-gray-500">{c.count}</span>
              </div>
            ))}
            {(!visitorSummary?.top_campaigns || visitorSummary.top_campaigns.length === 0) && (
              <div className="text-sm text-gray-400">No campaign data available.</div>
            )}
          </div>
        </div>
      </div>

      {/* Funnel Insights */}
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

      {/* Recent Activity */}
      <div className="bg-white p-6 rounded-lg shadow">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-xl font-semibold text-gray-800">Recent Activity</h3>
          <div className="flex items-center space-x-4">
            <div className="text-sm text-gray-600">
              Export range: <span className="font-semibold text-blue-600">{startDate} to {endDate}</span>
            </div>
            <div className="flex space-x-2">
              <button
                onClick={handleExport}
                disabled={exportMutation.isLoading}
                className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
              >
                {exportMutation.isLoading ? 'Exporting...' : 'Export Visits'}
              </button>
              <button
                onClick={() => exportEventsMutation.mutate()}
                disabled={exportEventsMutation.isLoading}
                className="px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 disabled:opacity-50"
              >
                {exportEventsMutation.isLoading ? 'Exporting...' : 'Export Events'}
              </button>
              <button
                onClick={() => setShowDeleteConfirm(true)}
                disabled={deleteMutation.isLoading}
                className="px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 disabled:opacity-50"
              >
                Delete All Data
              </button>
            </div>
          </div>
        </div>

        {/* Pagination Info */}
        {recentActivity && (
          <div className="mb-4 text-sm text-gray-600">
            Showing {recentActivity.visits?.length || 0} of {recentActivity.total_count || 0} visits
            {recentActivity.total_pages > 1 && (
              <span> (Page {recentActivity.current_page} of {recentActivity.total_pages})</span>
            )}
          </div>
        )}

        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Time
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  User Agent
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Page URL
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Session
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Type
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Location
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Browser/Device
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Tracking ID
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Source
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {recentActivity?.visits?.map((visit: any) => (
                <tr key={visit.id}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {new Date(visit.timestamp).toLocaleString()}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-900 max-w-md">
                    <div className="break-words">{visit.user_agent}</div>
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-900 max-w-md">
                    <div className="break-words">
                      {visit.page_url ? (
                        <a 
                          href={visit.page_url} 
                          target="_blank" 
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:text-blue-800 underline"
                        >
                          {visit.page_url}
                        </a>
                      ) : 'N/A'}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-blue-700">
                    <button className="underline" onClick={() => setSelectedSession(visit.session_id)}>
                      {visit.session_id?.slice(0,8)}...
                    </button>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                      visit.is_bot 
                        ? 'bg-red-100 text-red-800' 
                        : 'bg-green-100 text-green-800'
                    }`}>
                      {visit.crawler_type || (visit.is_bot ? 'AI Crawler' : 'Human')}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {visit.country || visit.city ? `${visit.city || ''}${visit.city && visit.country ? ', ' : ''}${visit.country || ''}` : '—'}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-900">
                    <div className="text-xs space-y-1">
                      {visit.client_side_language && <div>🌐 {visit.client_side_language}</div>}
                      {visit.client_side_screen_resolution && <div>📱 {visit.client_side_screen_resolution}</div>}
                      {visit.client_side_connection_type && <div>📡 {visit.client_side_connection_type}</div>}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {visit.tracking_id || 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {visit.source || visit.medium || visit.campaign || 'N/A'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination Controls */}
        {recentActivity && recentActivity.total_pages > 1 && (
          <div className="mt-4 flex items-center justify-between">
            <button
              onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
              disabled={!recentActivity.has_prev}
              className="px-4 py-2 text-sm font-medium text-gray-500 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Previous
            </button>
            
            <div className="flex items-center space-x-2">
              <span className="text-sm text-gray-600">Page {recentActivity.current_page} of {recentActivity.total_pages}</span>
              <input
                type="number"
                min={1}
                max={recentActivity.total_pages}
                value={jumpTo}
                onChange={(e) => setJumpTo(e.target.value)}
                placeholder="#"
                className="w-20 border border-gray-300 rounded-md px-2 py-1 text-sm"
              />
              <button
                onClick={() => {
                  const num = parseInt(jumpTo || '');
                  if (!isNaN(num)) {
                    const page = Math.max(1, Math.min(recentActivity.total_pages, num));
                    setCurrentPage(page);
                  }
                }}
                className="px-3 py-2 text-sm font-medium rounded-md text-gray-700 bg-white border border-gray-300 hover:bg-gray-50"
              >
                Go
              </button>
            </div>

            <button
              onClick={() => setCurrentPage(Math.min(recentActivity.total_pages, currentPage + 1))}
              disabled={!recentActivity.has_next}
              className="px-4 py-2 text-sm font-medium text-gray-500 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Next
            </button>
          </div>
        )}
      </div>

      {/* Session Timeline Modal */}
      {selectedSession && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-10 mx-auto p-5 border w-11/12 md:w-3/4 shadow-lg rounded-md bg-white">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-medium text-gray-900">Session Timeline: {selectedSession.slice(0,8)}...</h3>
              <button onClick={() => setSelectedSession(null)} className="text-gray-600 hover:text-gray-800">Close</button>
            </div>
            {/* Reuse Sessions page detail API */}
            {/* Lightweight fetch within modal */}
            <SessionTimeline sessionId={selectedSession} />
          </div>
        </div>
      )}


      {/* Delete Confirmation Modal */}
      {showDeleteConfirm && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-20 mx-auto p-5 border w-96 shadow-lg rounded-md bg-white">
            <div className="mt-3 text-center">
              <h3 className="text-lg font-medium text-gray-900">Delete All Data</h3>
              <div className="mt-2 px-7 py-3">
                <p className="text-sm text-gray-500">
                  Are you sure you want to delete all visit data? This action cannot be undone.
                </p>
              </div>
              <div className="items-center px-4 py-3">
                <button
                  onClick={handleDeleteAll}
                  disabled={deleteMutation.isLoading}
                  className="px-4 py-2 bg-red-600 text-white text-base font-medium rounded-md w-full shadow-sm hover:bg-red-700 focus:outline-none disabled:opacity-50"
                >
                  {deleteMutation.isLoading ? 'Deleting...' : 'Delete All Data'}
                </button>
                <button
                  onClick={() => setShowDeleteConfirm(false)}
                  className="mt-3 px-4 py-2 bg-gray-300 text-gray-800 text-base font-medium rounded-md w-full shadow-sm hover:bg-gray-400 focus:outline-none"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Dashboard;
