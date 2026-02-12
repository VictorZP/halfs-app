/**
 * Axios HTTP client configured for the backend API.
 *
 * In development, Vite proxies /api to the FastAPI server.
 * In production, set VITE_API_URL to the full backend URL.
 */
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || '';

const client = axios.create({
  baseURL: `${API_URL}/api`,
  headers: { 'Content-Type': 'application/json' },
});

// ───── Halfs ─────

export const halfs = {
  getMatches: (tournament, limit = 10000) =>
    client.get('/halfs/matches', { params: { tournament, limit } }),
  importMatches: (rawText) =>
    client.post('/halfs/matches/import', { raw_text: rawText }),
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
      params: { team1, team2, q_threshold: q, h_threshold: h, m_threshold: m },
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
};

export default client;
