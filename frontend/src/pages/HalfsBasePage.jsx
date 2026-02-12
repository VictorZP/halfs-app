import React, { useEffect, useState } from 'react';
import {
  Table, Button, Input, Space, message, Popconfirm, Select, Statistic, Row, Col, Card,
} from 'antd';
import { ImportOutlined, DeleteOutlined, ClearOutlined, SearchOutlined } from '@ant-design/icons';
import { halfs } from '../api/client';

const { TextArea } = Input;

export default function HalfsBasePage() {
  const [matches, setMatches] = useState([]);
  const [tournaments, setTournaments] = useState([]);
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(false);
  const [importText, setImportText] = useState('');
  const [filterTournament, setFilterTournament] = useState(null);
  const [selectedRows, setSelectedRows] = useState([]);

  const loadData = async () => {
    setLoading(true);
    try {
      const [m, t, s] = await Promise.all([
        halfs.getMatches(filterTournament),
        halfs.getTournaments(),
        halfs.getStatistics(),
      ]);
      setMatches(m.data);
      setTournaments(t.data);
      setStats(s.data);
    } catch (e) {
      message.error('Ошибка загрузки');
    }
    setLoading(false);
  };

  useEffect(() => { loadData(); }, [filterTournament]);

  const handleImport = async () => {
    if (!importText.trim()) return;
    setLoading(true);
    try {
      const res = await halfs.importMatches(importText);
      message.success(`Импортировано: ${res.data.imported}`);
      if (res.data.errors?.length) {
        message.warning(`Ошибки: ${res.data.errors.length} строк`);
      }
      setImportText('');
      loadData();
    } catch (e) {
      message.error('Ошибка импорта');
    }
    setLoading(false);
  };

  const handleDelete = async () => {
    if (!selectedRows.length) return;
    try {
      await halfs.deleteMatches(selectedRows);
      message.success('Удалено');
      setSelectedRows([]);
      loadData();
    } catch (e) {
      message.error('Ошибка удаления');
    }
  };

  const columns = [
    { title: 'Дата', dataIndex: 'date', key: 'date', width: 100 },
    { title: 'Турнир', dataIndex: 'tournament', key: 'tournament', width: 140 },
    { title: 'Дома', dataIndex: 'team_home', key: 'team_home', width: 130 },
    { title: 'Гости', dataIndex: 'team_away', key: 'team_away', width: 130 },
    { title: 'Q1 H', dataIndex: 'q1_home', key: 'q1_home', width: 55 },
    { title: 'Q1 A', dataIndex: 'q1_away', key: 'q1_away', width: 55 },
    { title: 'Q2 H', dataIndex: 'q2_home', key: 'q2_home', width: 55 },
    { title: 'Q2 A', dataIndex: 'q2_away', key: 'q2_away', width: 55 },
    { title: 'Q3 H', dataIndex: 'q3_home', key: 'q3_home', width: 55 },
    { title: 'Q3 A', dataIndex: 'q3_away', key: 'q3_away', width: 55 },
    { title: 'Q4 H', dataIndex: 'q4_home', key: 'q4_home', width: 55 },
    { title: 'Q4 A', dataIndex: 'q4_away', key: 'q4_away', width: 55 },
    { title: 'OT H', dataIndex: 'ot_home', key: 'ot_home', width: 55 },
    { title: 'OT A', dataIndex: 'ot_away', key: 'ot_away', width: 55 },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col span={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Матчей" value={stats.total_matches || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
        <Col span={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Турниров" value={stats.tournaments || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
        <Col span={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Команд" value={stats.teams || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
      </Row>

      <TextArea
        rows={4}
        value={importText}
        onChange={(e) => setImportText(e.target.value)}
        placeholder="Вставьте строки из Excel..."
        style={{ background: '#1a1a2e', color: '#e0e0e0', border: '1px solid #333' }}
      />

      <Space>
        <Button type="primary" icon={<ImportOutlined />} onClick={handleImport} loading={loading}>
          Импортировать
        </Button>
        <Popconfirm title="Удалить выбранные?" onConfirm={handleDelete}>
          <Button danger icon={<DeleteOutlined />} disabled={!selectedRows.length}>
            Удалить ({selectedRows.length})
          </Button>
        </Popconfirm>
        <Popconfirm title="Очистить всю базу?" onConfirm={async () => {
          await halfs.clearAll();
          loadData();
        }}>
          <Button danger icon={<ClearOutlined />}>Очистить</Button>
        </Popconfirm>
        <Select
          allowClear
          placeholder="Турнир"
          style={{ width: 200 }}
          value={filterTournament}
          onChange={setFilterTournament}
          options={tournaments.map(t => ({ label: t, value: t }))}
        />
      </Space>

      <Table
        dataSource={matches}
        columns={columns}
        rowKey="id"
        loading={loading}
        size="small"
        scroll={{ x: 1200 }}
        pagination={{ pageSize: 50, showSizeChanger: true }}
        rowSelection={{
          selectedRowKeys: selectedRows,
          onChange: setSelectedRows,
        }}
        footer={() => <span style={{ color: '#999' }}>Всего: {matches.length}</span>}
      />
    </Space>
  );
}
