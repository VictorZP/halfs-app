import React, { useEffect, useState } from 'react';
import { Table, Select, Space, message, Empty } from 'antd';
import { halfs } from '../api/client';

export default function HalfsStatsPage() {
  const [tournaments, setTournaments] = useState([]);
  const [selected, setSelected] = useState(null);
  const [stats, setStats] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    halfs.getTournaments().then(r => setTournaments(r.data)).catch(() => {});
  }, []);

  useEffect(() => {
    if (!selected) { setStats([]); return; }
    setLoading(true);
    halfs.getTeamStats(selected)
      .then(r => setStats(r.data))
      .catch(() => message.error('Ошибка загрузки статистики'))
      .finally(() => setLoading(false));
  }, [selected]);

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
    { title: 'Всего заб', dataIndex: 'total_scored', key: 'ts', width: 80 },
    { title: 'Всего проп', dataIndex: 'total_conceded', key: 'tc', width: 80 },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Select
        showSearch
        placeholder="Выберите турнир"
        style={{ width: 300 }}
        value={selected}
        onChange={setSelected}
        options={tournaments.map(t => ({ label: t, value: t }))}
        filterOption={(input, option) =>
          option.label.toLowerCase().includes(input.toLowerCase())
        }
      />
      {stats.length > 0 ? (
        <Table
          dataSource={stats}
          columns={columns}
          rowKey="team"
          loading={loading}
          size="small"
          scroll={{ x: 1200 }}
          pagination={false}
        />
      ) : (
        !loading && <Empty description="Выберите турнир для просмотра статистики" />
      )}
    </Space>
  );
}
