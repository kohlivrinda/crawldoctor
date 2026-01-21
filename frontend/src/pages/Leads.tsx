import React, { useEffect, useState } from 'react';
import { useQuery } from 'react-query';
import { analyticsAPI } from '../utils/api';

const getDefaultStartDate = () => {
  const date = new Date();
  date.setDate(date.getDate() - 30);
  return date.toISOString().split('T')[0];
};

const getDefaultEndDate = () => new Date().toISOString().split('T')[0];

const LEAD_PATHS = [
  { label: 'All', value: '' },
  { label: '/demo', value: '/demo' },
  { label: '/sign-up', value: '/sign-up' },
  { label: '/bifrost/enterprise', value: '/bifrost/enterprise' },
];

const DataValue: React.FC<{ value: any }> = ({ value }) => {
  if (value === null || value === undefined) return <span>—</span>;
  if (typeof value === 'object') {
    return <pre className="text-xs bg-gray-50 p-2 rounded overflow-auto">{JSON.stringify(value, null, 2)}</pre>;
  }
  
  const str = String(value);
  
  // Try to detect and parse JSON string
  if (str.trim().startsWith('{') || str.trim().startsWith('[')) {
    try {
      const parsed = JSON.parse(str);
      return <pre className="text-xs bg-gray-50 p-2 rounded overflow-auto">{JSON.stringify(parsed, null, 2)}</pre>;
    } catch (e) {}
  }
  
  // Try to detect pipe-separated key-value pairs
  if (str.includes('|') && str.includes(':')) {
    const parts = str.split('|').map(p => p.trim());
    return (
      <div className="space-y-1">
        {parts.map((p, i) => (
          <div key={i} className="text-xs border-b border-gray-100 last:border-0 pb-1">
            {p}
          </div>
        ))}
      </div>
    );
  }

  return <span className="break-words">{str}</span>;
};

const Leads: React.FC = () => {
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [capturedPath, setCapturedPath] = useState('');
  const [sourceFilter, setSourceFilter] = useState('');
  const [mediumFilter, setMediumFilter] = useState('');
  const [campaignFilter, setCampaignFilter] = useState('');
  const [startDate, setStartDate] = useState(getDefaultStartDate());
  const [endDate, setEndDate] = useState(getDefaultEndDate());
  const [selectedLead, setSelectedLead] = useState<string | null>(null);
  const [isExporting, setIsExporting] = useState(false);
  const [detailPage, setDetailPage] = useState(1);
  const detailPageSize = 200;

  const { data: leads, isLoading } = useQuery(
    ['leads', capturedPath, sourceFilter, mediumFilter, campaignFilter, startDate, endDate, currentPage, pageSize],
    () => analyticsAPI.listLeads(
      capturedPath || undefined,
      startDate,
      endDate,
      pageSize,
      (currentPage - 1) * pageSize,
      sourceFilter || undefined,
      mediumFilter || undefined,
      campaignFilter || undefined,
    ),
    { refetchInterval: 60000 }
  );

  const { data: leadDetail, isLoading: leadDetailLoading } = useQuery(
    ['lead-detail', selectedLead, detailPage],
    () => selectedLead ? analyticsAPI.getLeadDetail(selectedLead, detailPageSize, (detailPage - 1) * detailPageSize) : Promise.resolve(null),
    { enabled: !!selectedLead }
  );

  useEffect(() => {
    setDetailPage(1);
  }, [selectedLead]);

  const applyPath = (value: string) => {
    setCapturedPath(value);
    setCurrentPage(1);
  };

  const handleExport = async () => {
    try {
      setIsExporting(true);
      const response = await analyticsAPI.exportLeadsCSV(
        capturedPath || undefined,
        sourceFilter || undefined,
        mediumFilter || undefined,
        campaignFilter || undefined,
        startDate,
        endDate
      );
      const contentDisposition = response.headers?.['content-disposition'] || '';
      const filenameMatch = contentDisposition.match(/filename=([^;]+)/i);
      const filename = filenameMatch ? filenameMatch[1].replace(/"/g, '') : `crawldoctor_leads_${startDate}_to_${endDate}.csv`;
      const blob = response.data instanceof Blob ? response.data : new Blob([response.data], { type: 'text/csv' });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Leads export failed', err);
      alert('Export failed. Please try again or reduce the date range.');
    } finally {
      setIsExporting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Leads</h1>
          <p className="text-sm text-gray-500">Captured form submissions with full journey context.</p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={handleExport}
            disabled={isExporting}
            className="px-4 py-2 text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60"
          >
            {isExporting ? 'Exporting...' : 'Export CSV'}
          </button>
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

      <div className="bg-white p-4 rounded-lg shadow space-y-3">
        <div className="flex flex-wrap items-center gap-4">
          <div className="flex items-center space-x-2">
            <label className="text-sm font-medium text-gray-700">From:</label>
            <input
              type="date"
              value={startDate}
              onChange={(e) => { setStartDate(e.target.value); setCurrentPage(1); }}
              className="border border-gray-300 rounded-md px-3 py-2 text-sm"
            />
          </div>
          <div className="flex items-center space-x-2">
            <label className="text-sm font-medium text-gray-700">To:</label>
            <input
              type="date"
              value={endDate}
              onChange={(e) => { setEndDate(e.target.value); setCurrentPage(1); }}
              className="border border-gray-300 rounded-md px-3 py-2 text-sm"
            />
          </div>
          <div className="flex items-center space-x-2">
            <label className="text-sm font-medium text-gray-700">Captured On:</label>
            <select
              value={capturedPath}
              onChange={(e) => applyPath(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-2 text-sm"
            >
              {LEAD_PATHS.map((path) => (
                <option key={path.label} value={path.value}>{path.label}</option>
              ))}
            </select>
          </div>
          <div className="flex items-center space-x-2">
            <label className="text-sm font-medium text-gray-700">Source:</label>
            <input
              type="text"
              value={sourceFilter}
              onChange={(e) => { setSourceFilter(e.target.value); setCurrentPage(1); }}
              placeholder="utm_source"
              className="border border-gray-300 rounded-md px-3 py-2 text-sm"
            />
          </div>
          <div className="flex items-center space-x-2">
            <label className="text-sm font-medium text-gray-700">Medium:</label>
            <input
              type="text"
              value={mediumFilter}
              onChange={(e) => { setMediumFilter(e.target.value); setCurrentPage(1); }}
              placeholder="utm_medium"
              className="border border-gray-300 rounded-md px-3 py-2 text-sm"
            />
          </div>
          <div className="flex items-center space-x-2">
            <label className="text-sm font-medium text-gray-700">Campaign:</label>
            <input
              type="text"
              value={campaignFilter}
              onChange={(e) => { setCampaignFilter(e.target.value); setCurrentPage(1); }}
              placeholder="utm_campaign"
              className="border border-gray-300 rounded-md px-3 py-2 text-sm"
            />
          </div>
        </div>
      </div>

      <div className="bg-white p-6 rounded-lg shadow">
        <div className="flex items-center justify-between mb-4">
          <div className="text-sm text-gray-600">{leads?.total_count || 0} leads</div>
          {isLoading && <div className="text-sm text-gray-500">Loading...</div>}
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Lead</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Captured Page</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Source</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Captured At</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Journey</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {leads?.leads?.map((lead: any) => (
                <tr key={lead.client_id}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-mono">
                    {lead.client_id?.slice(0, 8)}...
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-700">
                    {lead.captured_path || '—'}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-700">
                    {lead.email || '—'}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-700">
                    {lead.name || '—'}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-700">
                    {lead.source || 'direct'} / {lead.medium || 'none'}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-700">
                    {lead.captured_at ? new Date(lead.captured_at).toLocaleString() : '—'}
                  </td>
                  <td className="px-6 py-4 text-sm text-blue-700">
                    <button className="underline" onClick={() => setSelectedLead(lead.client_id)}>
                      View Journey
                    </button>
                  </td>
                </tr>
              ))}
              {leads?.leads?.length === 0 && (
                <tr>
                  <td className="px-6 py-4 text-sm text-gray-500" colSpan={7}>
                    No leads found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {leads && leads.total_pages > 1 && (
          <div className="mt-4 flex items-center justify-between">
            <button
              onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
              disabled={!leads.has_prev}
              className="px-4 py-2 text-sm font-medium text-gray-500 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Previous
            </button>
            <span className="text-sm text-gray-600">Page {leads.current_page} of {leads.total_pages}</span>
            <button
              onClick={() => setCurrentPage(Math.min(leads.total_pages, currentPage + 1))}
              disabled={!leads.has_next}
              className="px-4 py-2 text-sm font-medium text-gray-500 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Next
            </button>
          </div>
        )}
      </div>

      {selectedLead && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-10 mx-auto p-5 border w-11/12 md:w-3/4 shadow-lg rounded-md bg-white">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-medium text-gray-900">Lead Detail: {selectedLead.slice(0, 8)}...</h3>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setDetailPage(Math.max(1, detailPage - 1))}
                  disabled={!leadDetail?.journey?.has_prev}
                  className="px-3 py-1 text-sm rounded-md bg-white border border-gray-300 text-gray-700 disabled:opacity-50"
                >
                  Prev
                </button>
                <button
                  onClick={() => setDetailPage(detailPage + 1)}
                  disabled={!leadDetail?.journey?.has_next}
                  className="px-3 py-1 text-sm rounded-md bg-white border border-gray-300 text-gray-700 disabled:opacity-50"
                >
                  Next
                </button>
                <button onClick={() => { setSelectedLead(null); setDetailPage(1); }} className="text-gray-600 hover:text-gray-800">Close</button>
              </div>
            </div>
            {leadDetailLoading ? (
              <div className="p-4">Loading...</div>
            ) : leadDetail ? (
              <div className="space-y-6 max-h-[70vh] overflow-y-auto">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="border rounded p-3">
                      <div className="text-xs text-gray-500 font-bold mb-1">Captured Values</div>
                      <DataValue value={leadDetail.latest_capture?.form_values} />
                    </div>
                    <div className="border rounded p-3">
                      <div className="text-xs text-gray-500 font-bold mb-1">URL Params</div>
                      <DataValue value={leadDetail.url_params} />
                    </div>
                </div>
                <div>
                  <h4 className="text-sm font-semibold text-gray-700 mb-2">Full Journey</h4>
                  <div className="space-y-3">
                    {(leadDetail.journey?.timeline || []).map((item: any) => (
                      <div key={`${item.type}-${item.id}`} className="border rounded p-3">
                        <div className="text-xs text-gray-500">{item.timestamp ? new Date(item.timestamp).toLocaleString() : '—'}</div>
                        <div className="text-sm font-semibold capitalize">{item.type === 'visit' ? 'Page View' : item.event_type}</div>
                        {item.page_url && (
                          <div className="text-sm text-blue-700 break-words">
                            <a href={item.page_url} target="_blank" rel="noopener noreferrer" className="underline">{item.page_url}</a>
                          </div>
                        )}
                        {item.type !== 'visit' && item.data && (
                          <pre className="text-xs bg-gray-50 p-2 rounded overflow-auto">{JSON.stringify(item.data, null, 2)}</pre>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div className="p-4">No data</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default Leads;
