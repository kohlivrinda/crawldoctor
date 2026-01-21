import axios from 'axios';

// Create API client
export const apiClient = axios.create({
  baseURL: process.env.REACT_APP_API_URL || window.location.origin,
  timeout: 120000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token to requests
apiClient.interceptors.request.use((config) => {
  const token = localStorage.getItem('auth_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Handle auth errors
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('auth_token');
      window.location.href = '/login';
    } else if (error.response?.status >= 500) {
      console.error('Server error:', error.response?.data);
    }
    return Promise.reject(error);
  }
);

// Auth API
export const authAPI = {
  login: async (username: string, password: string) => {
    const response = await apiClient.post('/api/v1/auth/login', {
      username,
      password,
    });
    return response.data;
  },

  me: async () => {
    const response = await apiClient.get('/api/v1/auth/me');
    return response.data;
  },

  logout: async () => {
    const response = await apiClient.post('/api/v1/auth/logout');
    return response.data;
  },
};

// Analytics API
export const analyticsAPI = {
  getVisitorSummary: async (startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/summary?${params.toString()}`);
    return response.data;
  },

  getFunnelSummary: async (startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/funnels?${params.toString()}`);
    return response.data;
  },

  getFunnelConfig: async () => {
    const response = await apiClient.get('/api/v1/analytics/funnels/config');
    return response.data;
  },

  saveFunnelConfig: async (config: any) => {
    const response = await apiClient.put('/api/v1/analytics/funnels/config', config);
    return response.data;
  },

  getFunnelTiming: async (funnelKey: string, startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/funnels/${funnelKey}/timing?${params.toString()}`);
    return response.data;
  },

  getFunnelDropoffs: async (funnelKey: string, step: number, limit: number = 50, offset: number = 0, startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    params.append('step', String(step));
    params.append('limit', String(limit));
    params.append('offset', String(offset));
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/funnels/${funnelKey}/dropoffs?${params.toString()}`);
    return response.data;
  },

  getFunnelStageUsers: async (funnelKey: string, step: number, limit: number = 50, offset: number = 0, startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    params.append('step', String(step));
    params.append('limit', String(limit));
    params.append('offset', String(offset));
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/funnels/${funnelKey}/stage-users?${params.toString()}`);
    return response.data;
  },

  getPageAnalytics: async (startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/pages?${params.toString()}`);
    return response.data;
  },

  getRecentActivity: async (limit: number = 50, offset: number = 0, startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    params.append('limit', String(limit));
    params.append('offset', String(offset));
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/recent?${params.toString()}`);
    return response.data;
  },

  listSessions: async (limit: number = 50, offset: number = 0) => {
    const response = await apiClient.get(`/api/v1/analytics/sessions?limit=${limit}&offset=${offset}`);
    return response.data;
  },

  getSessionDetail: async (sessionId: string) => {
    const response = await apiClient.get(`/api/v1/analytics/sessions/${sessionId}`);
    return response.data;
  },

  exportVisitsCSV: async (startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/export/csv?${params.toString()}`, {
      responseType: 'blob'
    });
    return response;
  },

  exportEventsCSV: async (startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/export/events.csv?${params.toString()}`, {
      responseType: 'blob'
    });
    return response;
  },

  deleteAllVisits: async () => {
    const response = await apiClient.delete('/api/v1/analytics/visits/all');
    return response.data;
  },

  getVisitorCategories: async (startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/categories?${params.toString()}`);
    return response.data;
  },

  listUnifiedUsers: async (limit: number = 50, offset: number = 0) => {
    const response = await apiClient.get(`/api/v1/analytics/users?limit=${limit}&offset=${offset}`);
    return response.data;
  },

  getUnifiedUserActivity: async (clientId: string) => {
    const response = await apiClient.get(`/api/v1/analytics/users/${clientId}`);
    return response.data;
  },

  getJourneyTimeline: async (clientId: string, limit: number = 200, offset: number = 0) => {
    const response = await apiClient.get(`/api/v1/analytics/journeys/${clientId}?limit=${limit}&offset=${offset}`);
    return response.data;
  },

  getPageFlows: async (days: number = 7, limit: number = 100) => {
    const response = await apiClient.get(`/api/v1/analytics/flows?days=${days}&limit=${limit}`);
    return response.data;
  },

  listJourneys: async (targetPath?: string, withCapturedOnly: boolean = false, startDate?: string, endDate?: string, limit: number = 50, offset: number = 0) => {
    const params = new URLSearchParams();
    if (targetPath) params.append('target_path', targetPath);
    if (withCapturedOnly) params.append('with_captured_only', 'true');
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    params.append('limit', String(limit));
    params.append('offset', String(offset));
    const response = await apiClient.get(`/api/v1/analytics/journeys?${params.toString()}`);
    return response.data;
  },

  exportJourneysCSV: async (targetPath?: string, withCapturedOnly: boolean = false, startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (targetPath) params.append('target_path', targetPath);
    if (withCapturedOnly) params.append('with_captured_only', 'true');
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/journeys/export?${params.toString()}`, {
      responseType: 'blob'
    });
    return response;
  },

  exportLeadsCSV: async (capturedPath?: string, source?: string, medium?: string, campaign?: string, startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (capturedPath) params.append('captured_path', capturedPath);
    if (source) params.append('source', source);
    if (medium) params.append('medium', medium);
    if (campaign) params.append('campaign', campaign);
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await apiClient.get(`/api/v1/analytics/leads/export?${params.toString()}`, {
      responseType: 'blob'
    });
    return response;
  },

  listLeads: async (capturedPath?: string, startDate?: string, endDate?: string, limit: number = 50, offset: number = 0, source?: string, medium?: string, campaign?: string) => {
    const params = new URLSearchParams();
    if (capturedPath) params.append('captured_path', capturedPath);
    if (source) params.append('source', source);
    if (medium) params.append('medium', medium);
    if (campaign) params.append('campaign', campaign);
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    params.append('limit', String(limit));
    params.append('offset', String(offset));
    const response = await apiClient.get(`/api/v1/analytics/leads?${params.toString()}`);
    return response.data;
  },

  getLeadDetail: async (clientId: string, limit: number = 200, offset: number = 0) => {
    const response = await apiClient.get(`/api/v1/analytics/leads/${clientId}?limit=${limit}&offset=${offset}`);
    return response.data;
  },
};

// Admin API
export const adminAPI = {
  getUsers: async () => {
    const response = await apiClient.get('/api/v1/admin/users');
    return response.data;
  },

  createUser: async (userData: any) => {
    const response = await apiClient.post('/api/v1/admin/users', userData);
    return response.data;
  },

  updateUser: async (userId: string, userData: any) => {
    const response = await apiClient.put(`/api/v1/admin/users/${userId}`, userData);
    return response.data;
  },

  deleteUser: async (userId: string) => {
    const response = await apiClient.delete(`/api/v1/admin/users/${userId}`);
    return response.data;
  },
};

// Utility functions
export const formatDateForAPI = (date: Date): string => {
  return date.toISOString();
};

export const getDateRangeParams = (days: number) => {
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);

  return {
    start_date: formatDateForAPI(startDate),
    end_date: formatDateForAPI(endDate),
  };
};

export const handleApiError = (error: any): string => {
  if (error.response?.data?.detail) {
    return error.response.data.detail;
  }
  if (error.response?.data?.message) {
    return error.response.data.message;
  }
  if (error.message) {
    return error.message;
  }
  return 'An unexpected error occurred';
};

export const downloadFile = (url: string, filename: string) => {
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};
