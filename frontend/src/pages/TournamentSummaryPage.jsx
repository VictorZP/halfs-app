import React, { useEffect, useState } from 'react';
import { Table, message } from 'antd';
import { halfs } from '../api/client';

export default function TournamentSummaryPage() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    halfs.getSummary()
      .then(r => setData(r.data))
      .catch(() => message.error('Ошибка загрузки'))
      .finally(() => setLoading(false));
  }, []);

  const columns = [
    { title: 'Турнир', dataIndex: 'tournament', key: 'tournament', sorter: (a, b) => a.tournament.localeCompare(b.tournament), fixed: 'left', width: 160 },
    { title: 'Игры', dataIndex: 'games', key: 'games', width: 70, sorter: (a, b) => a.games - b.games },
    { title: 'Команд', dataIndex: 'teams', key: 'teams', width: 80 },
    { title: 'Avg Total', dataIndex: 'avg_total', key: 'avg_total', width: 90 },
    { title: 'Avg 1H', dataIndex: 'avg_h1', key: 'avg_h1', width: 80 },
    { title: 'Avg 2H', dataIndex: 'avg_h2', key: 'avg_h2', width: 80 },
  ];

  return (
    <Table
      dataSource={data}
      columns={columns}
      rowKey="tournament"
      loading={loading}
      size="small"
      scroll={{ x: 600 }}
      pagination={false}
    />
  );
}
