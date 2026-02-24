/**
 * Axios HTTP client configured for the backend API.
 *
 * In development, Vite proxies /api to the FastAPI server.
 * In production, set VITE_API_URL to the full backend URL.
 *
 * The client automatically attaches a JWT token from localStorage
 * and redirects to login on 401 responses.
 */
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || '';

const client = axios.create({
  baseURL: `${API_URL}/api`,
  headers: { 'Content-Type': 'application/json' },
});

// Attach JWT token to every request
client.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// On 401 — clear token and reload (forces login screen)
client.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      window.location.reload();
    }
    return Promise.reject(error);
  }
);

// ───── Halfs ─────

export const halfs = {
  getMatches: (tournament, limit = 10000) =>
    client.get('/halfs/matches', { params: { tournament, limit } }),
  previewImport: (rawText) =>
    client.post('/halfs/matches/preview', { raw_text: rawText }),
  importMatches: (rawText) =>
    client.post('/halfs/matches/import', { raw_text: rawText }),
  updateMatch: (id, field, value) =>
    client.patch(`/halfs/matches/${id}`, { field, value }),
  normalizeDates: () =>
    client.post('/halfs/matches/normalize-dates'),
  deleteMatches: (ids) => client.delete('/halfs/matches', { data: { ids } }),
  clearAll: () => client.delete('/halfs/matches/all'),
  getTournaments: () => client.get('/halfs/tournaments'),
  getStatistics: () => client.get('/halfs/statistics'),
  getTeamStats: (tournament) => client.get(`/halfs/team-stats/${tournament}`),
  getSummary: () => client.get('/halfs/summary'),
  getDeviations: (tournament) => client.get(`/halfs/deviations/${tournament}`),
  getWinsLosses: (tournament) => client.get(`/halfs/wins-losses/${tournament}`),
  getQuarterDistribution: (tournament, team1, team2, total) =>
    client.get(`/halfs/quarter-distribution/${tournament}`, {
      params: { team1, team2, total },
    }),
  getCoefficients: (tournament, team1, team2, q, h, m) =>
    client.get(`/halfs/coefficients/${tournament}`, {
      params: {
        team1,
        team2,
        ...(q != null ? { q_threshold: q } : {}),
        ...(h != null ? { h_threshold: h } : {}),
        ...(m != null ? { m_threshold: m } : {}),
      },
    }),
};

// ───── Royka ─────

export const royka = {
  getMatches: (tournament, limit = 10000) =>
    client.get('/royka/matches', { params: { tournament, limit } }),
  addMatches: (matches) => client.post('/royka/matches', matches),
  deleteMatches: (ids) => client.delete('/royka/matches', { data: { ids } }),
  clearAll: () => client.delete('/royka/matches/all'),
  getTournaments: () => client.get('/royka/tournaments'),
  getStatistics: () => client.get('/royka/statistics'),
  analyzeTournament: (tournament) => client.get(`/royka/analysis/${tournament}`),
  analyzeAll: () => client.get('/royka/analysis'),
  analyzeDifferences: (tournament) => client.get(`/royka/analysis/differences/${tournament}`),
  analyzeRanges: (tournament) => client.get(`/royka/analysis/ranges/${tournament}`),
  analyzeHalf: (tournament) => client.get(`/royka/analysis/half/${tournament}`),
  analyzeHalfChange: (tournament) => client.get(`/royka/analysis/half-change/${tournament}`),
  analyzeHalfAll: () => client.get('/royka/analysis/half-all'),
  analyzeHalfChangeAll: () => client.get('/royka/analysis/half-change-all'),
};

// ───── Cyber ─────

export const cyber = {
  getMatches: (tournament, limit = 10000) =>
    client.get('/cyber/matches', { params: { tournament, limit } }),
  importMatches: (rawText) => client.post('/cyber/matches/import', { raw_text: rawText }),
  updateMatch: (id, field, value) =>
    client.patch(`/cyber/matches/${id}`, { field, value }),
  normalizeDates: () =>
    client.post('/cyber/matches/normalize-dates'),
  deleteMatches: (ids) => client.delete('/cyber/matches', { data: { ids } }),
  clearAll: () => client.delete('/cyber/matches/all'),
  getTournaments: () => client.get('/cyber/tournaments'),
  getStatistics: () => client.get('/cyber/statistics'),
  getSummary: (tournament) => client.get('/cyber/summary', { params: { tournament } }),
  getPredict: (tournament, team1, team2) =>
    client.get('/cyber/predict', { params: { tournament, team1, team2 } }),
  getLive: () => client.get('/cyber/live'),
  saveLive: (rows) => client.put('/cyber/live', rows),
  archiveLive: (row) => client.post('/cyber/live/archive', row),
  getLiveArchive: (limit = 5000) => client.get('/cyber/live/archive', { params: { limit } }),
  deleteLiveArchiveSelected: (ids) => client.delete('/cyber/live/archive/selected', { data: { ids } }),
  clearLiveArchive: () => client.delete('/cyber/live/archive'),
  clearLive: () => client.delete('/cyber/live'),
};

// ───── Sort Halves ─────

export const sortHalves = {
  process: (sourceFile, destinationFile) => {
    const formData = new FormData();
    formData.append('source_file', sourceFile);
    formData.append('destination_file', destinationFile);
    return client.post('/sort-halves/process', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      responseType: 'blob',
    });
  },
  getSheets: (destinationFile) => {
    const formData = new FormData();
    formData.append('destination_file', destinationFile);
    return client.post('/sort-halves/sheets', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },
};

export default client;
