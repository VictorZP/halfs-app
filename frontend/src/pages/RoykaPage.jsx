import React, { useEffect, useMemo, useState } from 'react';
import {
  Button,
  Card,
  Col,
  Empty,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tabs,
  message,
} from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { royka } from '../api/client';

const toNum = (v) => (v == null || v === '' ? '' : Number(v).toFixed(1));
const toRoi = (v) => `${Number(v || 0).toFixed(1)}%`;

function HalfSummaryTable({ stats }) {
  const rows = ['OVER', 'UNDER', 'TOTAL'].map((k) => {
    const s = stats?.[k] || {};
    return {
      key: k,
      category: k,
      count: s['кол-во'] || 0,
      win: s.WIN || 0,
      roi: ((s['%'] || 0) * 100),
    };
  });
  return (
    <Table
      size="small"
      pagination={false}
      rowKey="key"
      dataSource={rows}
      columns={[
        { title: 'Категория', dataIndex: 'category', width: 120 },
        { title: 'Кол-во', dataIndex: 'count', width: 90 },
        {
          title: 'WIN',
          dataIndex: 'win',
          width: 90,
          render: (v) => <span style={{ color: v > 0 ? '#52c41a' : v < 0 ? '#ff4d4f' : '#e0e0e0' }}>{v > 0 ? `+${v}` : v}</span>,
        },
        { title: 'ROI', dataIndex: 'roi', width: 90, render: toRoi },
      ]}
    />
  );
}

export default function RoykaPage() {
  const [tournaments, setTournaments] = useState([]);
  const [baseStats, setBaseStats] = useState({});
  const [loading, setLoading] = useState(false);

  const [selectedTournament, setSelectedTournament] = useState(null);
  const [matches, setMatches] = useState([]);

  const [selectedDiffTournament, setSelectedDiffTournament] = useState(null);
  const [diffRows, setDiffRows] = useState([]);

  const [selectedRangeTournament, setSelectedRangeTournament] = useState(null);
  const [rangeRows, setRangeRows] = useState([]);

  const [selectedHalfTournament, setSelectedHalfTournament] = useState(null);
  const [halfStats, setHalfStats] = useState(null);
  const [halfAll, setHalfAll] = useState(null);

  const [selectedHalfChangeTournament, setSelectedHalfChangeTournament] = useState(null);
  const [halfChangeStats, setHalfChangeStats] = useState(null);
  const [halfChangeAll, setHalfChangeAll] = useState(null);

  const loadBase = async () => {
    try {
      const [t, s] = await Promise.all([royka.getTournaments(), royka.getStatistics()]);
      setTournaments(t.data);
      setBaseStats(s.data);
    } catch {
      message.error('Ошибка загрузки Ройки');
    }
  };

  useEffect(() => {
    loadBase();
  }, []);

  const tournamentOptions = useMemo(
    () => tournaments.map((t) => ({ label: t, value: t })),
    [tournaments]
  );

  const loadMatches = async (tournament) => {
    if (!tournament) return;
    setLoading(true);
    try {
      const res = await royka.getMatches(tournament);
      setMatches(res.data);
    } catch {
      message.error('Ошибка загрузки матчей');
    } finally {
      setLoading(false);
    }
  };

  const loadDiffStats = async (tournament) => {
    if (!tournament) return;
    setLoading(true);
    try {
      const res = await royka.analyzeDifferences(tournament);
      setDiffRows(res.data || []);
    } catch {
      message.error('Ошибка расчёта статистики');
    } finally {
      setLoading(false);
    }
  };

  const loadRangeStats = async (tournament) => {
    if (!tournament) return;
    setLoading(true);
    try {
      const res = await royka.analyzeRanges(tournament);
      setRangeRows(res.data || []);
    } catch {
      message.error('Ошибка расчёта диапазонов');
    } finally {
      setLoading(false);
    }
  };

  const loadHalfStats = async (tournament) => {
    if (!tournament) return;
    setLoading(true);
    try {
      const res = await royka.analyzeHalf(tournament);
      setHalfStats(res.data || null);
    } catch {
      message.error('Ошибка расчёта 4.5+');
    } finally {
      setLoading(false);
    }
  };

  const loadHalfAll = async () => {
    setLoading(true);
    try {
      const res = await royka.analyzeHalfAll();
      setHalfAll(res.data || null);
    } catch {
      message.error('Ошибка расчёта всех турниров 4.5+');
    } finally {
      setLoading(false);
    }
  };

  const loadHalfChangeStats = async (tournament) => {
    if (!tournament) return;
    setLoading(true);
    try {
      const res = await royka.analyzeHalfChange(tournament);
      setHalfChangeStats(res.data || null);
    } catch {
      message.error('Ошибка расчёта 4.5+ CHANGE');
    } finally {
      setLoading(false);
    }
  };

  const loadHalfChangeAll = async () => {
    setLoading(true);
    try {
      const res = await royka.analyzeHalfChangeAll();
      setHalfChangeAll(res.data || null);
    } catch {
      message.error('Ошибка расчёта всех турниров 4.5+ CHANGE');
    } finally {
      setLoading(false);
    }
  };

  const matchColumns = [
    { title: 'Дата', dataIndex: 'date', width: 100 },
    { title: 'Турнир', dataIndex: 'tournament', width: 150 },
    { title: 'Дома', dataIndex: 'team_home', width: 140 },
    { title: 'Гости', dataIndex: 'team_away', width: 140 },
    { title: 'T1H', dataIndex: 't1h', width: 70, render: toNum },
    { title: 'T2H', dataIndex: 't2h', width: 70, render: toNum },
    { title: 'TIM', dataIndex: 'tim', width: 70, render: toNum },
    { title: 'Dev', dataIndex: 'deviation', width: 70, render: toNum },
    { title: 'Kickoff', dataIndex: 'kickoff', width: 80, render: toNum },
    { title: 'Predict', dataIndex: 'predict', width: 80 },
    { title: 'Result', dataIndex: 'result', width: 80, render: toNum },
  ];

  const diffColumns = [
    { title: 'Разница', dataIndex: 'difference', width: 80 },
    { title: 'Общее кол-во', dataIndex: 'overall_count', width: 110 },
    { title: 'Общее WIN', dataIndex: 'overall_win', width: 100 },
    { title: 'Общее ROI', dataIndex: 'overall_roi', width: 90, render: toRoi },
    { title: 'OVER кол-во', dataIndex: 'over_count', width: 110 },
    { title: 'OVER WIN', dataIndex: 'over_win', width: 90 },
    { title: 'OVER ROI', dataIndex: 'over_roi', width: 90, render: toRoi },
    { title: 'UNDER кол-во', dataIndex: 'under_count', width: 110 },
    { title: 'UNDER WIN', dataIndex: 'under_win', width: 100 },
    { title: 'UNDER ROI', dataIndex: 'under_roi', width: 95, render: toRoi },
  ];

  const rangeColumns = [
    { title: 'Диапазон', dataIndex: 'range', width: 100 },
    { title: 'Общее кол-во', dataIndex: 'overall_count', width: 110 },
    { title: 'Общее WIN', dataIndex: 'overall_win', width: 100 },
    { title: 'Общее ROI', dataIndex: 'overall_roi', width: 90, render: toRoi },
    { title: 'OVER кол-во', dataIndex: 'over_count', width: 110 },
    { title: 'OVER WIN', dataIndex: 'over_win', width: 90 },
    { title: 'OVER ROI', dataIndex: 'over_roi', width: 90, render: toRoi },
    { title: 'UNDER кол-во', dataIndex: 'under_count', width: 110 },
    { title: 'UNDER WIN', dataIndex: 'under_win', width: 100 },
    { title: 'UNDER ROI', dataIndex: 'under_roi', width: 95, render: toRoi },
  ];

  const tournamentsHalfColumns = [
    { title: 'Турнир', dataIndex: 'tournament', width: 220 },
    { title: 'OVER WIN', dataIndex: 'over_win', width: 100 },
    { title: 'UNDER WIN', dataIndex: 'under_win', width: 100 },
    { title: 'TOTAL WIN', dataIndex: 'total_win', width: 100 },
    { title: 'ROI', dataIndex: 'roi', width: 90, render: toRoi },
  ];

  const toTournamentRows = (payload) => {
    if (!payload?.tournaments) return [];
    return Object.entries(payload.tournaments).map(([name, s]) => {
      const totalCount = s?.TOTAL?.['кол-во'] || 0;
      const totalWin = s?.TOTAL?.WIN || 0;
      const roi = totalCount > 0 ? (totalWin / (totalCount * 100)) * 100 : 0;
      return {
        key: name,
        tournament: name,
        over_win: s?.OVER?.WIN || 0,
        under_win: s?.UNDER?.WIN || 0,
        total_win: totalWin,
        roi,
      };
    });
  };

  const dataTab = (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col xs={24} sm={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Записей" value={baseStats.total_records || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Турниров" value={baseStats.tournaments_count || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Команд" value={baseStats.teams_count || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
      </Row>
      <Space>
        <Select
          showSearch
          placeholder="Турнир"
          style={{ width: 280 }}
          value={selectedTournament}
          onChange={(v) => {
            setSelectedTournament(v);
            loadMatches(v);
          }}
          options={tournamentOptions}
          filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
        />
      </Space>
      <Table
        dataSource={matches}
        columns={matchColumns}
        rowKey="id"
        loading={loading}
        size="small"
        scroll={{ x: 1300 }}
        pagination={{ pageSize: 50 }}
      />
    </Space>
  );

  const statsTab = (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Select
        showSearch
        placeholder="Турнир"
        style={{ width: 280 }}
        value={selectedDiffTournament}
        onChange={(v) => {
          setSelectedDiffTournament(v);
          loadDiffStats(v);
        }}
        options={tournamentOptions}
        filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
      />
      {diffRows.length ? (
        <Table
          dataSource={diffRows}
          columns={diffColumns}
          rowKey={(row) => String(row.difference)}
          size="small"
          loading={loading}
          scroll={{ x: 1050 }}
          pagination={false}
        />
      ) : (
        !loading && <Empty description="Выберите турнир" />
      )}
    </Space>
  );

  const rangeTab = (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Select
        showSearch
        placeholder="Турнир"
        style={{ width: 280 }}
        value={selectedRangeTournament}
        onChange={(v) => {
          setSelectedRangeTournament(v);
          loadRangeStats(v);
        }}
        options={tournamentOptions}
        filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
      />
      {rangeRows.length ? (
        <Table
          dataSource={rangeRows}
          columns={rangeColumns}
          rowKey={(row) => row.range}
          size="small"
          loading={loading}
          scroll={{ x: 1080 }}
          pagination={false}
        />
      ) : (
        !loading && <Empty description="Выберите турнир" />
      )}
    </Space>
  );

  const halfTab = (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Space wrap>
        <Select
          showSearch
          placeholder="Турнир"
          style={{ width: 280 }}
          value={selectedHalfTournament}
          onChange={(v) => {
            setSelectedHalfTournament(v);
            loadHalfStats(v);
          }}
          options={tournamentOptions}
          filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
        />
        <Button icon={<ReloadOutlined />} onClick={loadHalfAll} loading={loading}>
          Статистика по всем турнирам
        </Button>
      </Space>
      {halfStats ? <HalfSummaryTable stats={halfStats} /> : !loading && <Empty description="Выберите турнир" />}
      {halfAll?.total && (
        <Card size="small" title="Итог по всем турнирам" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
          <HalfSummaryTable stats={halfAll.total} />
        </Card>
      )}
      {halfAll?.tournaments && (
        <Table
          size="small"
          pagination={false}
          rowKey="key"
          dataSource={toTournamentRows(halfAll)}
          columns={tournamentsHalfColumns}
          scroll={{ x: 700 }}
        />
      )}
    </Space>
  );

  const halfChangeTab = (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Space wrap>
        <Select
          showSearch
          placeholder="Турнир"
          style={{ width: 280 }}
          value={selectedHalfChangeTournament}
          onChange={(v) => {
            setSelectedHalfChangeTournament(v);
            loadHalfChangeStats(v);
          }}
          options={tournamentOptions}
          filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
        />
        <Button icon={<ReloadOutlined />} onClick={loadHalfChangeAll} loading={loading}>
          Статистика по всем турнирам
        </Button>
      </Space>
      {halfChangeStats ? <HalfSummaryTable stats={halfChangeStats} /> : !loading && <Empty description="Выберите турнир" />}
      {halfChangeAll?.total && (
        <Card size="small" title="Итог по всем турнирам (CHANGE)" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
          <HalfSummaryTable stats={halfChangeAll.total} />
        </Card>
      )}
      {halfChangeAll?.tournaments && (
        <Table
          size="small"
          pagination={false}
          rowKey="key"
          dataSource={toTournamentRows(halfChangeAll)}
          columns={tournamentsHalfColumns}
          scroll={{ x: 700 }}
        />
      )}
    </Space>
  );

  return (
    <Tabs
      items={[
        { key: 'data', label: 'Управление данными', children: dataTab },
        { key: 'stats', label: 'Статистика', children: statsTab },
        { key: 'ranges', label: 'Статистика по диапазонам', children: rangeTab },
        { key: 'half', label: 'Статистика 4.5+', children: halfTab },
        { key: 'half-change', label: 'Статистика 4.5+ CHANGE', children: halfChangeTab },
      ]}
    />
  );
}
