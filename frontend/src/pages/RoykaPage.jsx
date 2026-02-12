import React, { useEffect, useState } from 'react';
import {
  Table, Tabs, Select, Space, message, Button, Statistic, Row, Col, Card, Empty,
} from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { royka } from '../api/client';

export default function RoykaPage() {
  const [tournaments, setTournaments] = useState([]);
  const [stats, setStats] = useState({});
  const [matches, setMatches] = useState([]);
  const [allStats, setAllStats] = useState([]);
  const [selectedTournament, setSelectedTournament] = useState(null);
  const [tournamentStats, setTournamentStats] = useState(null);
  const [loading, setLoading] = useState(false);

  const loadBase = async () => {
    try {
      const [t, s] = await Promise.all([
        royka.getTournaments(),
        royka.getStatistics(),
      ]);
      setTournaments(t.data);
      setStats(s.data);
    } catch (e) {
      message.error('Ошибка загрузки');
    }
  };

  useEffect(() => { loadBase(); }, []);

  const loadTournamentData = async (tournament) => {
    if (!tournament) return;
    setLoading(true);
    try {
      const [m, a] = await Promise.all([
        royka.getMatches(tournament),
        royka.analyzeTournament(tournament),
      ]);
      setMatches(m.data);
      setTournamentStats(a.data);
    } catch (e) {
      message.error('Ошибка загрузки');
    }
    setLoading(false);
  };

  const loadAllStats = async () => {
    setLoading(true);
    try {
      const res = await royka.analyzeAll();
      setAllStats(res.data);
    } catch (e) {
      message.error('Ошибка загрузки');
    }
    setLoading(false);
  };

  const matchColumns = [
    { title: 'Дата', dataIndex: 'date', key: 'date', width: 100 },
    { title: 'Турнир', dataIndex: 'tournament', key: 'tournament', width: 120 },
    { title: 'Дома', dataIndex: 'team_home', key: 'team_home', width: 130 },
    { title: 'Гости', dataIndex: 'team_away', key: 'team_away', width: 130 },
    { title: 'T1H', dataIndex: 't1h', key: 't1h', width: 60, render: v => v != null ? Number(v).toFixed(1) : '' },
    { title: 'T2H', dataIndex: 't2h', key: 't2h', width: 60, render: v => v != null ? Number(v).toFixed(1) : '' },
    { title: 'TIM', dataIndex: 'tim', key: 'tim', width: 60, render: v => Number(v).toFixed(1) },
    { title: 'Dev', dataIndex: 'deviation', key: 'deviation', width: 60, render: v => v != null ? Number(v).toFixed(1) : '' },
    { title: 'Predict', dataIndex: 'predict', key: 'predict', width: 70 },
    { title: 'Result', dataIndex: 'result', key: 'result', width: 70, render: v => v != null ? Number(v).toFixed(1) : '' },
  ];

  const statsColumns = [
    { title: 'Турнир', dataIndex: 'tournament', key: 'tournament', width: 160 },
    { title: 'Матчей', dataIndex: 'total', key: 'total', width: 80 },
    { title: 'Over', dataIndex: 'over', key: 'over', width: 60 },
    { title: 'Under', dataIndex: 'under', key: 'under', width: 60 },
    { title: 'No bet', dataIndex: 'no_bet', key: 'no_bet', width: 70 },
    { title: 'Win', dataIndex: 'win', key: 'win', width: 60 },
    { title: 'Lose', dataIndex: 'lose', key: 'lose', width: 60 },
    {
      title: 'Win %', dataIndex: 'win_rate', key: 'win_rate', width: 70,
      render: v => `${v}%`,
      sorter: (a, b) => a.win_rate - b.win_rate,
    },
  ];

  const tabItems = [
    {
      key: 'data',
      label: 'Управление данными',
      children: (
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Row gutter={16}>
            <Col span={6}><Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
              <Statistic title="Записей" value={stats.total_records || 0} valueStyle={{ color: '#e0e0e0' }} />
            </Card></Col>
            <Col span={6}><Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
              <Statistic title="Турниров" value={stats.tournaments_count || 0} valueStyle={{ color: '#e0e0e0' }} />
            </Card></Col>
            <Col span={6}><Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
              <Statistic title="Команд" value={stats.teams_count || 0} valueStyle={{ color: '#e0e0e0' }} />
            </Card></Col>
          </Row>
          <Space>
            <Select
              showSearch
              placeholder="Турнир"
              style={{ width: 250 }}
              value={selectedTournament}
              onChange={(v) => { setSelectedTournament(v); loadTournamentData(v); }}
              options={tournaments.map(t => ({ label: t, value: t }))}
              filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
            />
          </Space>
          <Table
            dataSource={matches}
            columns={matchColumns}
            rowKey="id"
            loading={loading}
            size="small"
            scroll={{ x: 1000 }}
            pagination={{ pageSize: 50 }}
          />
        </Space>
      ),
    },
    {
      key: 'stats',
      label: 'Статистика',
      children: (
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Button icon={<ReloadOutlined />} onClick={loadAllStats} loading={loading}>
            Загрузить статистику по всем турнирам
          </Button>
          {allStats.length > 0 ? (
            <Table
              dataSource={allStats}
              columns={statsColumns}
              rowKey="tournament"
              loading={loading}
              size="small"
              pagination={false}
            />
          ) : (
            !loading && <Empty description="Нажмите кнопку для загрузки" />
          )}
        </Space>
      ),
    },
  ];

  return <Tabs items={tabItems} />;
}
