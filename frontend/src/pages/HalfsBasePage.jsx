import React, { useEffect, useState } from 'react';
import {
  Table, Button, Input, Space, message, Popconfirm, Select, Statistic, Row, Col, Card, Modal, Alert,
} from 'antd';
import { ImportOutlined, DeleteOutlined, ClearOutlined, SearchOutlined, EyeOutlined, EditOutlined, CalendarOutlined } from '@ant-design/icons';
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
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewSourceText, setPreviewSourceText] = useState('');
  const [previewData, setPreviewData] = useState({
    parsed_count: 0,
    error_count: 0,
    parsed_rows: [],
    errors: [],
  });
  const [dateEditOpen, setDateEditOpen] = useState(false);
  const [dateEditRecord, setDateEditRecord] = useState(null);
  const [dateEditValue, setDateEditValue] = useState('');
  const [replaceOpen, setReplaceOpen] = useState(false);
  const [replaceOld, setReplaceOld] = useState('');
  const [replaceNew, setReplaceNew] = useState('');
  const [replaceScope, setReplaceScope] = useState('all');
  const [replaceTournament, setReplaceTournament] = useState(null);
  const [mergeOpen, setMergeOpen] = useState(false);
  const [mergeSources, setMergeSources] = useState([]);
  const [mergeTarget, setMergeTarget] = useState('');

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

  const handlePreviewImport = async () => {
    if (!importText.trim()) {
      message.warning('Вставьте строки для предпросмотра');
      return;
    }
    setPreviewLoading(true);
    try {
      const res = await halfs.previewImport(importText);
      setPreviewData(res.data || {
        parsed_count: 0,
        error_count: 0,
        parsed_rows: [],
        errors: [],
      });
      setPreviewSourceText(importText);
      setPreviewOpen(true);
      if (!res.data?.parsed_count) {
        message.warning('Нет корректных строк для импорта');
      }
    } catch (e) {
      message.error('Ошибка предпросмотра');
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleImportConfirmed = async () => {
    if (!previewSourceText.trim()) {
      message.warning('Нет данных для импорта');
      return;
    }
    setLoading(true);
    try {
      const res = await halfs.importMatches(previewSourceText);
      message.success(`Импортировано: ${res.data.imported}`);
      if (res.data.errors?.length) {
        message.warning(`Ошибки: ${res.data.errors.length} строк`);
      }
      setImportText('');
      setPreviewOpen(false);
      setPreviewSourceText('');
      setPreviewData({
        parsed_count: 0,
        error_count: 0,
        parsed_rows: [],
        errors: [],
      });
      loadData();
    } catch (e) {
      message.error('Ошибка импорта');
    } finally {
      setLoading(false);
    }
  };

  const handleImport = async () => {
    await handlePreviewImport();
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

  const openDateEditor = (record) => {
    setDateEditRecord(record);
    setDateEditValue(record?.date || '');
    setDateEditOpen(true);
  };

  const saveDateEdit = async () => {
    if (!dateEditRecord?.id) return;
    try {
      await halfs.updateMatch(dateEditRecord.id, 'date', dateEditValue);
      message.success('Дата обновлена');
      setDateEditOpen(false);
      loadData();
    } catch (e) {
      message.error('Неверный формат даты');
    }
  };

  const normalizeDates = async () => {
    setLoading(true);
    try {
      const res = await halfs.normalizeDates();
      message.success(`Нормализовано дат: ${res.data?.updated || 0}`);
      loadData();
    } catch (e) {
      message.error('Ошибка нормализации дат');
    } finally {
      setLoading(false);
    }
  };

  const runReplace = async () => {
    if (!replaceOld.trim()) {
      message.warning('Введите, что заменить');
      return;
    }
    if (replaceScope === 'tournament' && !replaceTournament) {
      message.warning('Выберите турнир для замены');
      return;
    }
    setLoading(true);
    try {
      const res = await halfs.replaceValues(
        replaceOld,
        replaceNew,
        replaceScope,
        replaceScope === 'tournament' ? replaceTournament : null
      );
      message.success(`Заменено: ${res.data?.replaced || 0}`);
      setReplaceOpen(false);
      loadData();
    } catch {
      message.error('Ошибка замены');
      setLoading(false);
    }
  };

  const runMergeTournaments = async () => {
    const target = mergeTarget.trim();
    if (!mergeSources.length) {
      message.warning('Выберите хотя бы один турнир');
      return;
    }
    if (!target) {
      message.warning('Введите название целевого турнира');
      return;
    }
    setLoading(true);
    try {
      const res = await halfs.mergeTournaments(mergeSources, target);
      message.success(`Объединено записей: ${res.data?.updated || 0}`);
      setMergeOpen(false);
      setMergeSources([]);
      setMergeTarget('');
      loadData();
    } catch {
      message.error('Ошибка объединения турниров');
      setLoading(false);
    }
  };

  const columns = [
    {
      title: 'Дата',
      dataIndex: 'date',
      key: 'date',
      width: 130,
      render: (_, record) => (
        <Space size={6}>
          <span>{record.date}</span>
          <Button
            type="text"
            size="small"
            icon={<EditOutlined />}
            onClick={() => openDateEditor(record)}
          />
        </Space>
      ),
    },
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

  const previewColumns = [
    { title: 'Дата', dataIndex: 'date', key: 'date', width: 100 },
    { title: 'Турнир', dataIndex: 'tournament', key: 'tournament', width: 170 },
    { title: 'Дома', dataIndex: 'team_home', key: 'team_home', width: 150 },
    { title: 'Гости', dataIndex: 'team_away', key: 'team_away', width: 150 },
    { title: 'Q1', dataIndex: 'q1_home', key: 'q1_home', width: 55 },
    { title: 'Q1', dataIndex: 'q1_away', key: 'q1_away', width: 55 },
    { title: 'Q2', dataIndex: 'q2_home', key: 'q2_home', width: 55 },
    { title: 'Q2', dataIndex: 'q2_away', key: 'q2_away', width: 55 },
    { title: 'Q3', dataIndex: 'q3_home', key: 'q3_home', width: 55 },
    { title: 'Q3', dataIndex: 'q3_away', key: 'q3_away', width: 55 },
    { title: 'Q4', dataIndex: 'q4_home', key: 'q4_home', width: 55 },
    { title: 'Q4', dataIndex: 'q4_away', key: 'q4_away', width: 55 },
    { title: 'OT', dataIndex: 'ot_home', key: 'ot_home', width: 55 },
    { title: 'OT', dataIndex: 'ot_away', key: 'ot_away', width: 55 },
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
        <Button icon={<EyeOutlined />} onClick={handlePreviewImport} loading={previewLoading}>
          Предпросмотр
        </Button>
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
        <Button icon={<CalendarOutlined />} onClick={normalizeDates} loading={loading}>
          Нормализовать даты
        </Button>
        <Button icon={<SearchOutlined />} onClick={() => {
          setReplaceTournament(filterTournament || null);
          setReplaceScope(filterTournament ? 'tournament' : 'all');
          setReplaceOpen(true);
        }}>
          Заменить
        </Button>
        <Button onClick={() => setMergeOpen(true)}>
          Объединить турниры
        </Button>
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

      <Modal
        title="Предпросмотр импорта половин"
        open={previewOpen}
        onCancel={() => setPreviewOpen(false)}
        width={1200}
        footer={[
          <Button key="cancel" onClick={() => setPreviewOpen(false)}>
            Отмена
          </Button>,
          <Button
            key="confirm"
            type="primary"
            icon={<ImportOutlined />}
            onClick={handleImportConfirmed}
            disabled={!previewData.parsed_count}
            loading={loading}
          >
            Подтвердить импорт
          </Button>,
        ]}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Alert
            type={previewData.error_count ? 'warning' : 'success'}
            showIcon
            message={`Распознано: ${previewData.parsed_count} | Ошибок: ${previewData.error_count}`}
          />

          <Table
            size="small"
            dataSource={(previewData.parsed_rows || []).map((row, idx) => ({ key: idx, ...row }))}
            columns={previewColumns}
            scroll={{ x: 1300, y: 350 }}
            pagination={{ pageSize: 20, showSizeChanger: true }}
          />

          {!!previewData.error_count && (
            <TextArea
              rows={6}
              readOnly
              value={(previewData.errors || []).join('\n')}
              style={{ background: '#1a1a2e', color: '#e0e0e0', border: '1px solid #333' }}
            />
          )}
        </Space>
      </Modal>

      <Modal
        title="Редактирование даты"
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

      <Modal
        title="Поиск и замена (Половины)"
        open={replaceOpen}
        onCancel={() => setReplaceOpen(false)}
        onOk={runReplace}
        okText="Заменить"
        confirmLoading={loading}
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          <Input
            value={replaceOld}
            onChange={(e) => setReplaceOld(e.target.value)}
            placeholder="Что заменить"
          />
          <Input
            value={replaceNew}
            onChange={(e) => setReplaceNew(e.target.value)}
            placeholder="На что заменить"
          />
          <Select
            value={replaceScope}
            onChange={setReplaceScope}
            options={[
              { label: 'Во всей таблице', value: 'all' },
              { label: 'Только в выбранном турнире', value: 'tournament' },
            ]}
          />
          {replaceScope === 'tournament' && (
            <Select
              showSearch
              value={replaceTournament}
              onChange={setReplaceTournament}
              options={tournaments.map((t) => ({ label: t, value: t }))}
              placeholder="Выберите турнир"
              filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
            />
          )}
        </Space>
      </Modal>

      <Modal
        title="Объединить турниры (Половины)"
        open={mergeOpen}
        onCancel={() => setMergeOpen(false)}
        onOk={runMergeTournaments}
        okText="Объединить"
        confirmLoading={loading}
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          <Select
            mode="multiple"
            showSearch
            value={mergeSources}
            onChange={setMergeSources}
            options={tournaments.map((t) => ({ label: t, value: t }))}
            placeholder="Какие турниры объединить"
            filterOption={(input, option) => option.label.toLowerCase().includes(input.toLowerCase())}
          />
          <Input
            value={mergeTarget}
            onChange={(e) => setMergeTarget(e.target.value)}
            placeholder="В какой турнир объединить (например EL (W))"
          />
        </Space>
      </Modal>
    </Space>
  );
}
