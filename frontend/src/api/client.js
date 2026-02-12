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

// ───── Cybers ─────

export const cybers = {
  getMatches: (tournament) =>
    client.get('/cybers/matches', { params: tournament ? { tournament } : {} }),
  importMatches: (rawText) =>
    client.post('/cybers/matches/import', { raw_text: rawText }),
  deleteMatches: (ids) => client.delete('/cybers/matches', { data: { ids } }),
  clearAll: () => client.delete('/cybers/matches/all'),
  getTournaments: () => client.get('/cybers/tournaments'),
  getSummary: () => client.get('/cybers/summary'),
  findDuplicates: () => client.post('/cybers/duplicates'),
  replaceInBase: (find, replace, ids) =>
    client.post('/cybers/replace', { find, replace, ids }),

  // Predict
  predict: (tournament, team1, team2) =>
    client.post('/cybers/predict', { tournament, team1, team2 }),

  // Live
  getLive: () => client.get('/cybers/live'),
  addLive: (match) => client.post('/cybers/live', match),
  updateLive: (id, update) => client.put(`/cybers/live/${id}`, update),
  deleteLive: (id) => client.delete(`/cybers/live/${id}`),
  clearLive: () => client.delete('/cybers/live'),
};

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
