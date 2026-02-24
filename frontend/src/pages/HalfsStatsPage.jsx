import React, { useEffect, useState } from 'react';
import {
  Table, Select, Space, message, Empty, Tabs, InputNumber, Button, Input,
  Card, Row, Col, Descriptions, AutoComplete,
} from 'antd';
import { CalculatorOutlined } from '@ant-design/icons';
import { halfs } from '../api/client';

// ─────────── Tab 1: Статистика / Коэффициенты ───────────

function StatsCoeffTab() {
  const [tournaments, setTournaments] = useState([]);
  const [selected, setSelected] = useState(null);
  const [stats, setStats] = useState([]);
  const [teams, setTeams] = useState([]);
  const [loading, setLoading] = useState(false);
  // Coefficient calculator
  const [team1, setTeam1] = useState('');
  const [team2, setTeam2] = useState('');
  const [qThr, setQThr] = useState(null);
  const [hThr, setHThr] = useState(null);
  const [mThr, setMThr] = useState(null);
  const [coeffResult, setCoeffResult] = useState(null);
  const [coeffLoading, setCoeffLoading] = useState(false);

  useEffect(() => {
    halfs.getTournaments().then(r => setTournaments(r.data)).catch(() => {});
  }, []);

  useEffect(() => {
    if (!selected) { setStats([]); setTeams([]); return; }
    setLoading(true);
    halfs.getTeamStats(selected)
      .then(r => {
        setStats(r.data);
        setTeams(r.data.map(s => s.team));
      })
      .catch(() => message.error('Ошибка загрузки'))
      .finally(() => setLoading(false));
  }, [selected]);

  const calcCoefficients = async () => {
    if (!selected || !team1 || !team2) {
      message.warning('Выберите турнир и две команды');
      return;
    }
    if (qThr == null && hThr == null && mThr == null) {
      message.warning('Введите хотя бы один порог: четверть, половина или матч');
      return;
    }
    setCoeffLoading(true);
    try {
      const res = await halfs.getCoefficients(selected, team1, team2, qThr, hThr, mThr);
      setCoeffResult(res.data);
    } catch (e) {
      message.error('Команды не найдены или нет данных');
      setCoeffResult(null);
    }
    setCoeffLoading(false);
  };

  const columns = [
    { title: 'Команда', dataIndex: 'team', key: 'team', fixed: 'left', width: 140 },
    { title: 'Игры', dataIndex: 'games', key: 'games', width: 60, sorter: (a, b) => a.games - b.games },
    { title: 'Q1 заб', dataIndex: 'q1_scored', key: 'q1s', width: 70 },
    { title: 'Q2 заб', dataIndex: 'q2_scored', key: 'q2s', width: 70 },
    { title: 'Q3 заб', dataIndex: 'q3_scored', key: 'q3s', width: 70 },
    { title: 'Q4 заб', dataIndex: 'q4_scored', key: 'q4s', width: 70 },
    { title: 'Q1 проп', dataIndex: 'q1_conceded', key: 'q1c', width: 70 },
    { title: 'Q2 проп', dataIndex: 'q2_conceded', key: 'q2c', width: 70 },
    { title: 'Q3 проп', dataIndex: 'q3_conceded', key: 'q3c', width: 70 },
    { title: 'Q4 проп', dataIndex: 'q4_conceded', key: 'q4c', width: 70 },
    { title: '1H заб', dataIndex: 'h1_scored', key: 'h1s', width: 70 },
    { title: '2H заб', dataIndex: 'h2_scored', key: 'h2s', width: 70 },
    { title: '1H проп', dataIndex: 'h1_conceded', key: 'h1c', width: 70 },
    { title: '2H проп', dataIndex: 'h2_conceded', key: 'h2c', width: 70 },
    { title: 'Всего заб', dataIndex: 'total_scored', key: 'ts', width: 85 },
    { title: 'Всего проп', dataIndex: 'total_conceded', key: 'tc', width: 85 },
  ];

  const teamOptions = teams.map(t => ({ value: t }));
  const periods = coeffResult?.requested_periods || [];
  const periodLabels = { q1: 'Q1', q2: 'Q2', q3: 'Q3', q4: 'Q4', h1: '1H', h2: '2H', match: 'Матч' };

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Select
        showSearch
        placeholder="Выберите турнир"
        style={{ width: 300 }}
        value={selected}
        onChange={setSelected}
        options={tournaments.map(t => ({ label: t, value: t }))}
        filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
      />

      {stats.length > 0 && (
        <>
          <Table
            dataSource={stats}
            columns={columns}
            rowKey="team"
            loading={loading}
            size="small"
            scroll={{ x: 1200 }}
            pagination={false}
          />

          <Card title="Калькулятор коэффициентов" size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Space wrap>
              <AutoComplete options={teamOptions} style={{ width: 160 }} placeholder="Команда 1"
                value={team1} onChange={setTeam1}
                filterOption={(input, option) => option.value.toLowerCase().includes(input.toLowerCase())}
              />
              <AutoComplete options={teamOptions} style={{ width: 160 }} placeholder="Команда 2"
                value={team2} onChange={setTeam2}
                filterOption={(input, option) => option.value.toLowerCase().includes(input.toLowerCase())}
              />
              <InputNumber placeholder="Четверть" value={qThr} onChange={setQThr} step={0.5} style={{ width: 100 }} />
              <InputNumber placeholder="Половина" value={hThr} onChange={setHThr} step={0.5} style={{ width: 100 }} />
              <InputNumber placeholder="Матч" value={mThr} onChange={setMThr} step={0.5} style={{ width: 100 }} />
              <Button type="primary" icon={<CalculatorOutlined />} onClick={calcCoefficients} loading={coeffLoading}>
                Рассчитать
              </Button>
            </Space>
            {coeffResult && (
              <Table
                style={{ marginTop: 16 }}
                size="small"
                pagination={false}
                dataSource={periods.map(p => ({
                  key: p,
                  period: periodLabels[p],
                  over: coeffResult.over[p],
                  under: coeffResult.under[p],
                }))}
                columns={[
                  { title: 'Период', dataIndex: 'period', key: 'period', width: 80 },
                  { title: 'Over', dataIndex: 'over', key: 'over', render: v => v?.toFixed(2) },
                  { title: 'Under', dataIndex: 'under', key: 'under', render: v => v?.toFixed(2) },
                ]}
              />
            )}
          </Card>
        </>
      )}

      {!stats.length && !loading && <Empty description="Выберите турнир" />}
    </Space>
  );
}

// ─────────── Tab 2: Отклонения ───────────

function DeviationsTab() {
  const [tournaments, setTournaments] = useState([]);
  const [selected, setSelected] = useState(null);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [team1, setTeam1] = useState('');
  const [team2, setTeam2] = useState('');

  useEffect(() => {
    halfs.getTournaments().then(r => setTournaments(r.data)).catch(() => {});
  }, []);

  useEffect(() => {
    if (!selected) { setData([]); setTeam1(''); setTeam2(''); return; }
    setLoading(true);
    halfs.getDeviations(selected)
      .then(r => setData(r.data))
      .catch(() => message.error('Ошибка'))
      .finally(() => setLoading(false));
  }, [selected]);

  const teamOptions = data.map(d => ({ value: d.team }));

  const t1Data = data.find(d => d.team === team1);
  const t2Data = data.find(d => d.team === team2);
  const pairSummary = (t1Data && t2Data) ? {
    h1: ((t1Data.h1_total + t2Data.h1_total) / 2).toFixed(1),
    h2: ((t1Data.h2_total + t2Data.h2_total) / 2).toFixed(1),
    dev: ((t1Data.deviation + t2Data.deviation) / 2).toFixed(1),
    avg: ((t1Data.average_total + t2Data.average_total) / 2).toFixed(1),
  } : null;

  const columns = [
    { title: 'Команда', dataIndex: 'team', key: 'team', width: 160, sorter: (a, b) => a.team.localeCompare(b.team) },
    { title: 'Игры', dataIndex: 'games', key: 'games', width: 60 },
    { title: '1H Total', dataIndex: 'h1_total', key: 'h1', width: 90 },
    { title: '2H Total', dataIndex: 'h2_total', key: 'h2', width: 90 },
    {
      title: 'Отклонение', dataIndex: 'deviation', key: 'dev', width: 110,
      sorter: (a, b) => a.deviation - b.deviation,
      render: v => <span style={{ color: v > 0 ? '#52c41a' : v < 0 ? '#ff4d4f' : '#e0e0e0', fontWeight: 600 }}>{v > 0 ? '+' : ''}{v}</span>,
    },
    { title: 'Avg Total', dataIndex: 'average_total', key: 'avg', width: 90 },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Select
        showSearch placeholder="Турнир" style={{ width: 300 }} value={selected}
        onChange={setSelected}
        options={tournaments.map(t => ({ label: t, value: t }))}
        filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
      />

      {data.length > 0 && (
        <>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Space wrap>
              <AutoComplete options={teamOptions} style={{ width: 180 }} placeholder="Команда 1"
                value={team1} onChange={setTeam1}
                filterOption={(input, option) => option.value.toLowerCase().includes(input.toLowerCase())}
              />
              <AutoComplete options={teamOptions} style={{ width: 180 }} placeholder="Команда 2"
                value={team2} onChange={setTeam2}
                filterOption={(input, option) => option.value.toLowerCase().includes(input.toLowerCase())}
              />
            </Space>
            {pairSummary && (
              <Descriptions column={{ xs: 2, sm: 4 }} size="small" bordered style={{ marginTop: 12 }}
                labelStyle={{ color: '#999', background: '#16213e' }}
                contentStyle={{ color: '#e0e0e0', background: '#1a1a2e', fontWeight: 600 }}
              >
                <Descriptions.Item label="Ср. 1H">{pairSummary.h1}</Descriptions.Item>
                <Descriptions.Item label="Ср. 2H">{pairSummary.h2}</Descriptions.Item>
                <Descriptions.Item label="Ср. откл.">
                  <span style={{ color: pairSummary.dev > 0 ? '#52c41a' : pairSummary.dev < 0 ? '#ff4d4f' : '#e0e0e0' }}>
                    {pairSummary.dev > 0 ? '+' : ''}{pairSummary.dev}
                  </span>
                </Descriptions.Item>
                <Descriptions.Item label="Ср. тотал">{pairSummary.avg}</Descriptions.Item>
              </Descriptions>
            )}
          </Card>

          <Table dataSource={data} columns={columns} rowKey="team" size="small" pagination={false} loading={loading} />
        </>
      )}
      {!data.length && !loading && <Empty description="Выберите турнир" />}
    </Space>
  );
}

// ─────────── Tab 3: Средние четверти ───────────

function QuarterDistTab() {
  const [tournaments, setTournaments] = useState([]);
  const [teams, setTeams] = useState([]);
  const [selected, setSelected] = useState(null);
  const [team1, setTeam1] = useState('');
  const [team2, setTeam2] = useState('');
  const [total, setTotal] = useState(null);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    halfs.getTournaments().then(r => setTournaments(r.data)).catch(() => {});
  }, []);

  useEffect(() => {
    if (!selected) { setTeams([]); return; }
    halfs.getTeamStats(selected)
      .then(r => setTeams(r.data.map(s => s.team)))
      .catch(() => {});
  }, [selected]);

  const calculate = async () => {
    if (!selected || !team1 || !team2 || !total) {
      message.warning('Заполните все поля');
      return;
    }
    setLoading(true);
    try {
      const res = await halfs.getQuarterDistribution(selected, team1, team2, total);
      setResult(res.data);
    } catch {
      message.error('Команды не найдены');
      setResult(null);
    }
    setLoading(false);
  };

  const teamOptions = teams.map(t => ({ value: t }));

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Select
        showSearch placeholder="Турнир" style={{ width: 300 }} value={selected}
        onChange={setSelected}
        options={tournaments.map(t => ({ label: t, value: t }))}
        filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
      />
      <Space wrap>
        <AutoComplete options={teamOptions} style={{ width: 160 }} placeholder="Команда 1"
          value={team1} onChange={setTeam1}
          filterOption={(input, option) => option.value.toLowerCase().includes(input.toLowerCase())}
        />
        <AutoComplete options={teamOptions} style={{ width: 160 }} placeholder="Команда 2"
          value={team2} onChange={setTeam2}
          filterOption={(input, option) => option.value.toLowerCase().includes(input.toLowerCase())}
        />
        <InputNumber placeholder="Тотал" value={total} onChange={setTotal} step={0.5} style={{ width: 110 }} />
        <Button type="primary" icon={<CalculatorOutlined />} onClick={calculate} loading={loading}>
          Рассчитать
        </Button>
      </Space>
      {result && (
        <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333', maxWidth: 500 }}>
          <Descriptions column={2} size="small" bordered
            labelStyle={{ color: '#999', background: '#16213e' }}
            contentStyle={{ color: '#e0e0e0', background: '#1a1a2e', fontWeight: 600 }}
          >
            <Descriptions.Item label="Q1">{result.q1} ({result.q1_pct}%)</Descriptions.Item>
            <Descriptions.Item label="Q2">{result.q2} ({result.q2_pct}%)</Descriptions.Item>
            <Descriptions.Item label="Q3">{result.q3} ({result.q3_pct}%)</Descriptions.Item>
            <Descriptions.Item label="Q4">{result.q4} ({result.q4_pct}%)</Descriptions.Item>
            <Descriptions.Item label="1-я половина">{result.h1}</Descriptions.Item>
            <Descriptions.Item label="2-я половина">{result.h2}</Descriptions.Item>
          </Descriptions>
        </Card>
      )}
    </Space>
  );
}

// ─────────── Tab 4: Победы / поражения ───────────

function WinsLossesTab() {
  const [tournaments, setTournaments] = useState([]);
  const [selected, setSelected] = useState(null);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    halfs.getTournaments().then(r => setTournaments(r.data)).catch(() => {});
  }, []);

  useEffect(() => {
    if (!selected) { setData([]); return; }
    setLoading(true);
    halfs.getWinsLosses(selected)
      .then(r => setData(r.data))
      .catch(() => message.error('Ошибка'))
      .finally(() => setLoading(false));
  }, [selected]);

  const columns = [
    { title: 'Команда', dataIndex: 'team', key: 'team', width: 160, sorter: (a, b) => a.team.localeCompare(b.team) },
    { title: 'Победы', dataIndex: 'wins', key: 'wins', width: 80, sorter: (a, b) => a.wins - b.wins },
    { title: 'Поражения', dataIndex: 'losses', key: 'losses', width: 100, sorter: (a, b) => a.losses - b.losses },
    { title: 'Всего', dataIndex: 'total', key: 'total', width: 70 },
    {
      title: 'Win %', dataIndex: 'win_pct', key: 'pct', width: 80,
      sorter: (a, b) => a.win_pct - b.win_pct,
      render: v => `${v}%`,
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Select
        showSearch placeholder="Турнир" style={{ width: 300 }} value={selected}
        onChange={setSelected}
        options={tournaments.map(t => ({ label: t, value: t }))}
        filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
      />
      {data.length > 0 ? (
        <Table dataSource={data} columns={columns} rowKey="team" size="small" pagination={false} loading={loading} />
      ) : !loading && <Empty description="Выберите турнир" />}
    </Space>
  );
}

// ─────────── Main page ───────────

export default function HalfsStatsPage() {
  const tabItems = [
    { key: 'stats', label: 'Статистика/коэффициенты', children: <StatsCoeffTab /> },
    { key: 'deviations', label: 'Отклонения', children: <DeviationsTab /> },
    { key: 'quarters', label: 'Средние четверти', children: <QuarterDistTab /> },
    { key: 'wins', label: 'Победы/поражения', children: <WinsLossesTab /> },
  ];

  return <Tabs items={tabItems} />;
}
