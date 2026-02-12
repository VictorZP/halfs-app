import React, { useEffect, useState } from 'react';
import {
  Table, Button, Input, Tabs, Space, message, Modal, Select, Popconfirm, Card,
  Typography, Upload, Statistic, Row, Col,
} from 'antd';
import {
  ImportOutlined, DeleteOutlined, SearchOutlined,
  ClearOutlined, SwapOutlined,
} from '@ant-design/icons';
import { cybers } from '../api/client';

const { TextArea } = Input;
const { Title } = Typography;

export default function CybersBasesPage() {
  const [matches, setMatches] = useState([]);
  const [summary, setSummary] = useState([]);
  const [tournaments, setTournaments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [importText, setImportText] = useState('');
  const [selectedRows, setSelectedRows] = useState([]);
  const [searchText, setSearchText] = useState('');
  const [filterTournament, setFilterTournament] = useState(null);

  const loadData = async () => {
    setLoading(true);
    try {
      const [m, s, t] = await Promise.all([
        cybers.getMatches(filterTournament),
        cybers.getSummary(),
        cybers.getTournaments(),
      ]);
      setMatches(m.data);
      setSummary(s.data);
      setTournaments(t.data);
    } catch (e) {
      message.error('Ошибка загрузки данных');
    }
    setLoading(false);
  };

  useEffect(() => { loadData(); }, [filterTournament]);

  const handleImport = async () => {
    if (!importText.trim()) {
      message.warning('Введите данные для импорта');
      return;
    }
    setLoading(true);
    try {
      const res = await cybers.importMatches(importText);
      message.success(`Импортировано: ${res.data.imported} строк`);
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
    setLoading(true);
    try {
      await cybers.deleteMatches(selectedRows);
      message.success(`Удалено: ${selectedRows.length}`);
      setSelectedRows([]);
      loadData();
    } catch (e) {
      message.error('Ошибка удаления');
    }
    setLoading(false);
  };

  const handleClear = async () => {
    setLoading(true);
    try {
      await cybers.clearAll();
      message.success('База очищена');
      loadData();
    } catch (e) {
      message.error('Ошибка очистки');
    }
    setLoading(false);
  };

  const columns = [
    { title: 'Дата', dataIndex: 'date', key: 'date', width: 100, sorter: (a, b) => a.date?.localeCompare(b.date) },
    { title: 'Турнир', dataIndex: 'tournament', key: 'tournament', width: 120 },
    { title: 'Команда', dataIndex: 'team', key: 'team', width: 120 },
    { title: 'H/A', dataIndex: 'home_away', key: 'home_away', width: 50 },
    { title: '2PTA', dataIndex: 'two_pt_made', key: 'two_pt_made', width: 60, render: v => Math.round(v) },
    { title: '2PTM', dataIndex: 'two_pt_attempt', key: 'two_pt_attempt', width: 60, render: v => Math.round(v) },
    { title: '3PTA', dataIndex: 'three_pt_made', key: 'three_pt_made', width: 60, render: v => Math.round(v) },
    { title: '3PTM', dataIndex: 'three_pt_attempt', key: 'three_pt_attempt', width: 60, render: v => Math.round(v) },
    { title: 'FTA', dataIndex: 'fta_made', key: 'fta_made', width: 60, render: v => Math.round(v) },
    { title: 'FTM', dataIndex: 'fta_attempt', key: 'fta_attempt', width: 60, render: v => Math.round(v) },
    { title: 'OR', dataIndex: 'off_rebound', key: 'off_rebound', width: 50, render: v => Math.round(v) },
    { title: 'TO', dataIndex: 'turnovers', key: 'turnovers', width: 50, render: v => Math.round(v) },
    { title: 'Controls', dataIndex: 'controls', key: 'controls', width: 80, render: v => Number(v).toFixed(1) },
    { title: 'Points', dataIndex: 'points', key: 'points', width: 70, render: v => Math.round(v) },
    { title: 'Opponent', dataIndex: 'opponent', key: 'opponent', width: 120 },
    { title: 'AttakKEF', dataIndex: 'attak_kef', key: 'attak_kef', width: 80, render: v => Number(v).toFixed(2) },
    { title: 'Status', dataIndex: 'status', key: 'status', width: 50 },
  ];

  const summaryColumns = [
    { title: 'Турнир', dataIndex: 'tournament', key: 'tournament', sorter: (a, b) => a.tournament.localeCompare(b.tournament) },
    { title: 'Игры', dataIndex: 'games', key: 'games', sorter: (a, b) => a.games - b.games },
    { title: 'Avg 2PTA', dataIndex: 'avg_2pta', key: 'avg_2pta' },
    { title: 'Avg 2PTM', dataIndex: 'avg_2ptm', key: 'avg_2ptm' },
    { title: 'Avg 3PTA', dataIndex: 'avg_3pta', key: 'avg_3pta' },
    { title: 'Avg 3PTM', dataIndex: 'avg_3ptm', key: 'avg_3ptm' },
    { title: 'Avg FTA', dataIndex: 'avg_fta', key: 'avg_fta' },
    { title: 'Avg FTM', dataIndex: 'avg_ftm', key: 'avg_ftm' },
    { title: 'Avg OR', dataIndex: 'avg_or', key: 'avg_or' },
    { title: 'Avg TO', dataIndex: 'avg_to', key: 'avg_to' },
    { title: 'Avg Controls', dataIndex: 'avg_controls', key: 'avg_controls' },
    { title: 'Avg Points', dataIndex: 'avg_points', key: 'avg_points' },
    { title: 'P/C', dataIndex: 'pc', key: 'pc' },
    { title: '%2PT', dataIndex: 'pct_2pt', key: 'pct_2pt', render: v => `${v}%` },
    { title: '%3PT', dataIndex: 'pct_3pt', key: 'pct_3pt', render: v => `${v}%` },
    { title: '%FT', dataIndex: 'pct_ft', key: 'pct_ft', render: v => `${v}%` },
  ];

  const filteredMatches = searchText
    ? matches.filter(m =>
        Object.values(m).some(v =>
          String(v).toLowerCase().includes(searchText.toLowerCase())
        )
      )
    : matches;

  const tabItems = [
    {
      key: 'base',
      label: 'База',
      children: (
        <div>
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <TextArea
              rows={4}
              value={importText}
              onChange={(e) => setImportText(e.target.value)}
              placeholder="Вставьте строки из Excel (TSV)..."
              style={{ background: '#1a1a2e', color: '#e0e0e0', border: '1px solid #333' }}
            />
            <Space>
              <Button type="primary" icon={<ImportOutlined />} onClick={handleImport} loading={loading}>
                Импортировать
              </Button>
              <Popconfirm title="Удалить выбранные?" onConfirm={handleDelete}>
                <Button danger icon={<DeleteOutlined />} disabled={!selectedRows.length}>
                  Удалить выбранные ({selectedRows.length})
                </Button>
              </Popconfirm>
              <Popconfirm
                title="Очистить всю базу?"
                onConfirm={handleClear}
                okText="Да, очистить"
              >
                <Button danger icon={<ClearOutlined />}>Очистить базу</Button>
              </Popconfirm>
              <Select
                allowClear
                placeholder="Фильтр по турниру"
                style={{ width: 200 }}
                value={filterTournament}
                onChange={setFilterTournament}
                options={tournaments.map(t => ({ label: t, value: t }))}
              />
              <Input
                placeholder="Поиск..."
                prefix={<SearchOutlined />}
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
                style={{ width: 200 }}
              />
            </Space>

            <Table
              dataSource={filteredMatches}
              columns={columns}
              rowKey="id"
              loading={loading}
              size="small"
              scroll={{ x: 1400 }}
              pagination={{ pageSize: 50, showSizeChanger: true }}
              rowSelection={{
                selectedRowKeys: selectedRows,
                onChange: setSelectedRows,
              }}
              footer={() => (
                <span style={{ color: '#999' }}>
                  Всего: {filteredMatches.length} строк | Выбрано: {selectedRows.length}
                </span>
              )}
            />
          </Space>
        </div>
      ),
    },
    {
      key: 'summary',
      label: 'Сводная статистика',
      children: (
        <Table
          dataSource={summary}
          columns={summaryColumns}
          rowKey="tournament"
          loading={loading}
          size="small"
          scroll={{ x: 1200 }}
          pagination={false}
        />
      ),
    },
  ];

  return (
    <div>
      <Tabs items={tabItems} />
    </div>
  );
}
