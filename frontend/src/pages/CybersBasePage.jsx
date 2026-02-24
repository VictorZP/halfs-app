import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Col,
  Input,
  message,
  Modal,
  Popconfirm,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tabs,
} from 'antd';
import { CalendarOutlined, ClearOutlined, DeleteOutlined, EditOutlined, ImportOutlined, ReloadOutlined } from '@ant-design/icons';
import { cyber } from '../api/client';

const { TextArea } = Input;

const summaryColumns = [
  { title: 'Турнир', dataIndex: 'tournament', width: 180 },
  { title: 'Игр', dataIndex: 'games', width: 70 },
  { title: '2PTA', dataIndex: 'two_pt_attempt', width: 70 },
  { title: '2PTM', dataIndex: 'two_pt_made', width: 70 },
  { title: '3PTA', dataIndex: 'three_pt_attempt', width: 70 },
  { title: '3PTM', dataIndex: 'three_pt_made', width: 70 },
  { title: 'FTA', dataIndex: 'fta_attempt', width: 70 },
  { title: 'FTM', dataIndex: 'fta_made', width: 70 },
  { title: 'OR', dataIndex: 'off_rebound', width: 70 },
  { title: 'TO', dataIndex: 'turnovers', width: 70 },
  { title: 'Controls', dataIndex: 'controls', width: 80 },
  { title: 'Points', dataIndex: 'points', width: 80 },
  { title: 'P/C', dataIndex: 'p_per_control', width: 80 },
  { title: '2PT %', dataIndex: 'two_pt_pct', width: 80 },
  { title: '3PT %', dataIndex: 'three_pt_pct', width: 80 },
  { title: 'FT %', dataIndex: 'ft_pct', width: 80 },
];

export default function CybersBasePage() {
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState({});
  const [tournaments, setTournaments] = useState([]);
  const [matches, setMatches] = useState([]);
  const [summary, setSummary] = useState([]);
  const [filterTournament, setFilterTournament] = useState(null);
  const [summaryTournament, setSummaryTournament] = useState(null);
  const [importText, setImportText] = useState('');
  const [selectedRows, setSelectedRows] = useState([]);
  const [dateEditOpen, setDateEditOpen] = useState(false);
  const [dateEditRecord, setDateEditRecord] = useState(null);
  const [dateEditValue, setDateEditValue] = useState('');

  const loadAll = async () => {
    setLoading(true);
    try {
      const [m, t, s, sm] = await Promise.all([
        cyber.getMatches(filterTournament),
        cyber.getTournaments(),
        cyber.getStatistics(),
        cyber.getSummary(summaryTournament),
      ]);
      setMatches(m.data);
      setTournaments(t.data);
      setStats(s.data);
      setSummary(sm.data);
    } catch {
      message.error('Ошибка загрузки Cyber');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadAll();
  }, [filterTournament, summaryTournament]);

  const tournamentOptions = useMemo(
    () => tournaments.map((t) => ({ label: t, value: t })),
    [tournaments]
  );

  const matchColumns = useMemo(
    () => [
      {
        title: 'Дата',
        dataIndex: 'date',
        width: 110,
        render: (_, record) => (
          <Space size={6}>
            <span>{record.date}</span>
            <Button type="text" size="small" icon={<EditOutlined />} onClick={() => {
              setDateEditRecord(record);
              setDateEditValue(record?.date || '');
              setDateEditOpen(true);
            }}
            />
          </Space>
        ),
      },
      { title: 'Турнир', dataIndex: 'tournament', width: 150 },
      { title: 'Команда', dataIndex: 'team', width: 130 },
      { title: 'H/A', dataIndex: 'home_away', width: 60 },
      { title: '2PTM', dataIndex: 'two_pt_made', width: 70 },
      { title: '2PTA', dataIndex: 'two_pt_attempt', width: 70 },
      { title: '3PTM', dataIndex: 'three_pt_made', width: 70 },
      { title: '3PTA', dataIndex: 'three_pt_attempt', width: 70 },
      { title: 'FTM', dataIndex: 'fta_made', width: 70 },
      { title: 'FTA', dataIndex: 'fta_attempt', width: 70 },
      { title: 'OR', dataIndex: 'off_rebound', width: 70 },
      { title: 'TO', dataIndex: 'turnovers', width: 70 },
      { title: 'Controls', dataIndex: 'controls', width: 80 },
      { title: 'Points', dataIndex: 'points', width: 80 },
      { title: 'Opponent', dataIndex: 'opponent', width: 130 },
      { title: 'AttakKEF', dataIndex: 'attak_kef', width: 80 },
      { title: 'Status', dataIndex: 'status', width: 80 },
    ],
    []
  );

  const handleImport = async () => {
    if (!importText.trim()) return;
    setLoading(true);
    try {
      const res = await cyber.importMatches(importText);
      message.success(`Импортировано строк: ${res.data.imported}`);
      if (res.data.skipped) {
        message.warning(`Пропущено строк: ${res.data.skipped}`);
      }
      setImportText('');
      loadAll();
    } catch {
      message.error('Ошибка импорта');
      setLoading(false);
    }
  };

  const handleDeleteSelected = async () => {
    if (!selectedRows.length) return;
    try {
      await cyber.deleteMatches(selectedRows);
      message.success('Выбранные строки удалены');
      setSelectedRows([]);
      loadAll();
    } catch {
      message.error('Ошибка удаления');
    }
  };

  const clearAll = async () => {
    try {
      await cyber.clearAll();
      message.success('База Cyber очищена');
      setSelectedRows([]);
      loadAll();
    } catch {
      message.error('Ошибка очистки');
    }
  };

  const saveDateEdit = async () => {
    if (!dateEditRecord?.id) return;
    try {
      await cyber.updateMatch(dateEditRecord.id, 'date', dateEditValue);
      message.success('Дата обновлена');
      setDateEditOpen(false);
      loadAll();
    } catch {
      message.error('Не удалось сохранить дату');
    }
  };

  const normalizeDates = async () => {
    setLoading(true);
    try {
      const res = await cyber.normalizeDates();
      message.success(`Нормализовано дат: ${res.data?.updated || 0}`);
      loadAll();
    } catch {
      message.error('Ошибка нормализации дат');
    } finally {
      setLoading(false);
    }
  };

  const baseTab = (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col xs={24} sm={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Записей" value={stats.total_records || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Турниров" value={stats.tournaments_count || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic title="Команд" value={stats.teams_count || 0} valueStyle={{ color: '#e0e0e0' }} />
          </Card>
        </Col>
      </Row>

      <TextArea
        rows={5}
        value={importText}
        onChange={(e) => setImportText(e.target.value)}
        placeholder="Вставьте строки из Excel (17 столбцов, табуляция)..."
        style={{ background: '#1a1a2e', color: '#e0e0e0', border: '1px solid #333' }}
      />

      <Space wrap>
        <Button type="primary" icon={<ImportOutlined />} onClick={handleImport} loading={loading}>
          Импортировать
        </Button>
        <Button icon={<ReloadOutlined />} onClick={loadAll} loading={loading}>
          Обновить
        </Button>
        <Select
          allowClear
          showSearch
          placeholder="Фильтр турнира"
          style={{ width: 240 }}
          value={filterTournament}
          onChange={setFilterTournament}
          options={tournamentOptions}
          filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
        />
        <Popconfirm title="Удалить выбранные строки?" onConfirm={handleDeleteSelected}>
          <Button danger icon={<DeleteOutlined />} disabled={!selectedRows.length}>
            Удалить ({selectedRows.length})
          </Button>
        </Popconfirm>
        <Popconfirm title="Очистить всю базу Cyber?" onConfirm={clearAll}>
          <Button danger icon={<ClearOutlined />}>Очистить базу</Button>
        </Popconfirm>
        <Button icon={<CalendarOutlined />} onClick={normalizeDates} loading={loading}>
          Нормализовать даты
        </Button>
      </Space>

      <Table
        dataSource={matches}
        columns={matchColumns}
        rowKey="id"
        loading={loading}
        size="small"
        scroll={{ x: 1800 }}
        pagination={{ pageSize: 50, showSizeChanger: true }}
        rowSelection={{ selectedRowKeys: selectedRows, onChange: setSelectedRows }}
      />
      <Alert
        type="info"
        showIcon
        message="Дату можно отредактировать по иконке карандаша в колонке 'Дата'."
      />
    </Space>
  );

  const summaryTab = (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Space wrap>
        <Select
          allowClear
          showSearch
          placeholder="Турнир"
          style={{ width: 260 }}
          value={summaryTournament}
          onChange={setSummaryTournament}
          options={tournamentOptions}
          filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
        />
        <Button icon={<ReloadOutlined />} onClick={loadAll} loading={loading}>
          Обновить
        </Button>
      </Space>
      <Table
        dataSource={summary}
        columns={summaryColumns}
        rowKey={(row) => `${row.tournament}-${row.games}`}
        loading={loading}
        size="small"
        scroll={{ x: 1400 }}
        pagination={false}
      />
    </Space>
  );

  return (
    <>
      <Tabs
        items={[
          { key: 'base', label: 'База', children: baseTab },
          { key: 'summary', label: 'Сводная статистика', children: summaryTab },
        ]}
      />
      <Modal
        title="Редактирование даты (Cybers)"
        open={dateEditOpen}
        onCancel={() => setDateEditOpen(false)}
        onOk={saveDateEdit}
        okText="Сохранить"
      >
        <Input
          value={dateEditValue}
          onChange={(e) => setDateEditValue(e.target.value)}
          placeholder="Например: 21.02.2026, 02.21.26, 2026-02-21"
        />
      </Modal>
    </>
  );
}
