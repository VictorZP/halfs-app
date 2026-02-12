import React, { useEffect, useState, useCallback } from 'react';
import {
  Table, Button, Input, Tabs, Space, message, Popconfirm, Tag,
} from 'antd';
import {
  PlusOutlined, DeleteOutlined, ClearOutlined, ImportOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import { cybers } from '../api/client';

const { TextArea } = Input;

export default function CyberLivePage() {
  const [matches, setMatches] = useState([]);
  const [loading, setLoading] = useState(false);
  const [importText, setImportText] = useState('');
  const [selectedRows, setSelectedRows] = useState([]);

  const loadMatches = useCallback(async () => {
    setLoading(true);
    try {
      const res = await cybers.getLive();
      setMatches(res.data);
    } catch (e) {
      message.error('Ошибка загрузки матчей');
    }
    setLoading(false);
  }, []);

  useEffect(() => { loadMatches(); }, [loadMatches]);

  const handleAddMatch = async () => {
    try {
      const res = await cybers.addLive({
        tournament: '', team1: '', team2: '', total: null, calc_temp: 0,
      });
      setMatches((prev) => [...prev, res.data]);
    } catch (e) {
      message.error('Ошибка добавления');
    }
  };

  const handleUpdateField = async (id, field, value) => {
    try {
      const res = await cybers.updateLive(id, { [field]: value });
      setMatches((prev) =>
        prev.map((m) => (m.id === id ? res.data : m))
      );
    } catch (e) {
      message.error('Ошибка обновления');
    }
  };

  const handleDelete = async (id) => {
    try {
      await cybers.deleteLive(id);
      setMatches((prev) => prev.filter((m) => m.id !== id));
      message.success('Удалено');
    } catch (e) {
      message.error('Ошибка удаления');
    }
  };

  const handleClear = async () => {
    try {
      await cybers.clearLive();
      setMatches([]);
      message.success('Все матчи удалены');
    } catch (e) {
      message.error('Ошибка очистки');
    }
  };

  const handleImport = async () => {
    if (!importText.trim()) return;
    // Parse TSV: Tournament, Team1, Team2, Total
    const lines = importText.trim().split('\n');
    for (const line of lines) {
      const cells = line.split('\t').map((c) => c.trim()).filter(Boolean);
      if (cells.length < 3) continue;
      const tournament = cells[0];
      const team1 = cells[1];
      const team2 = cells[2];
      let total = null;
      if (cells[3]) {
        const parsed = parseFloat(cells[3].replace(',', '.'));
        if (!isNaN(parsed)) total = parsed;
      }
      try {
        const res = await cybers.addLive({ tournament, team1, team2, total, calc_temp: 0 });
        setMatches((prev) => [...prev, res.data]);
      } catch (e) {
        // skip errors
      }
    }
    setImportText('');
    message.success('Импорт завершён');
    loadMatches();
  };

  const EditableCell = ({ value, onSave, style }) => {
    const [editing, setEditing] = useState(false);
    const [val, setVal] = useState(value ?? '');

    if (!editing) {
      return (
        <div
          style={{ cursor: 'pointer', minHeight: 22, ...style }}
          onClick={() => setEditing(true)}
        >
          {value ?? '—'}
        </div>
      );
    }
    return (
      <Input
        size="small"
        value={val}
        autoFocus
        onChange={(e) => setVal(e.target.value)}
        onBlur={() => { setEditing(false); onSave(val); }}
        onPressEnter={() => { setEditing(false); onSave(val); }}
      />
    );
  };

  const linesColumns = [
    {
      title: 'Турнир', dataIndex: 'tournament', key: 'tournament', width: 120,
      render: (v, record) => (
        <EditableCell value={v} onSave={(val) => handleUpdateField(record.id, 'tournament', val)} />
      ),
    },
    {
      title: 'Команда 1', dataIndex: 'team1', key: 'team1', width: 120,
      render: (v, record) => (
        <EditableCell value={v} onSave={(val) => handleUpdateField(record.id, 'team1', val)} />
      ),
    },
    {
      title: 'Команда 2', dataIndex: 'team2', key: 'team2', width: 120,
      render: (v, record) => (
        <EditableCell value={v} onSave={(val) => handleUpdateField(record.id, 'team2', val)} />
      ),
    },
    {
      title: 'Тотал', dataIndex: 'total', key: 'total', width: 80,
      render: (v, record) => (
        <EditableCell
          value={v != null ? Number(v).toFixed(1) : ''}
          onSave={(val) => {
            const parsed = parseFloat(String(val).replace(',', '.'));
            handleUpdateField(record.id, 'total', isNaN(parsed) ? null : parsed);
          }}
        />
      ),
    },
    { title: 'TEMP', dataIndex: 'temp', key: 'temp', width: 70, render: v => v != null ? Number(v).toFixed(1) : '—' },
    { title: 'Predict', dataIndex: 'predict', key: 'predict', width: 80, render: v => v != null ? Number(v).toFixed(1) : '—' },
    {
      title: 'UNDER', dataIndex: 'under', key: 'under', width: 70,
      render: (v) => v != null ? <span className="under-value">{Number(v).toFixed(1)}</span> : '',
    },
    {
      title: 'OVER', dataIndex: 'over', key: 'over', width: 70,
      render: (v) => v != null ? <span className="over-value">{Number(v).toFixed(1)}</span> : '',
    },
    {
      title: 'CalcTEMP', dataIndex: 'calc_temp', key: 'calc_temp', width: 90,
      className: 'calc-temp-cell',
      render: (v, record) => (
        <EditableCell
          value={v != null ? Number(v).toFixed(1) : '0.0'}
          style={{ background: '#1a2b45', padding: '0 4px' }}
          onSave={(val) => {
            const parsed = parseFloat(String(val).replace(',', '.'));
            handleUpdateField(record.id, 'calc_temp', isNaN(parsed) ? 0 : parsed);
          }}
        />
      ),
    },
    { title: 'T2H', dataIndex: 't2h', key: 't2h', width: 70, render: v => v != null ? Number(v).toFixed(1) : '—' },
    {
      title: '', key: 'actions', width: 50,
      render: (_, record) => (
        <Popconfirm title="Удалить матч?" onConfirm={() => handleDelete(record.id)}>
          <Button size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      ),
    },
  ];

  const predictColumns = [
    { title: 'Турнир', dataIndex: 'tournament', key: 'tournament' },
    { title: 'Команда 1', dataIndex: 'team1', key: 'team1' },
    { title: 'Команда 2', dataIndex: 'team2', key: 'team2' },
    { title: 'TEMP', dataIndex: 'temp', key: 'temp', render: v => v != null ? Number(v).toFixed(1) : '—' },
    { title: 'Predict', dataIndex: 'predict', key: 'predict', render: v => v != null ? Number(v).toFixed(1) : '—' },
    { title: 'IT1', dataIndex: 'it1', key: 'it1', render: v => v != null ? Number(v).toFixed(1) : '—' },
    { title: 'IT2', dataIndex: 'it2', key: 'it2', render: v => v != null ? Number(v).toFixed(1) : '—' },
  ];

  const tabItems = [
    {
      key: 'lines',
      label: 'Lines',
      children: (
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <TextArea
            rows={3}
            value={importText}
            onChange={(e) => setImportText(e.target.value)}
            placeholder="Турнир  Команда1  Команда2  Тотал (TSV)..."
            style={{ background: '#1a1a2e', color: '#e0e0e0', border: '1px solid #333' }}
          />
          <Space>
            <Button type="primary" icon={<ImportOutlined />} onClick={handleImport}>
              Импортировать
            </Button>
            <Button icon={<PlusOutlined />} onClick={handleAddMatch}>
              Добавить матч
            </Button>
            <Button icon={<ReloadOutlined />} onClick={loadMatches}>
              Обновить
            </Button>
            <Popconfirm title="Удалить ВСЕ матчи?" onConfirm={handleClear}>
              <Button danger icon={<ClearOutlined />}>Очистить</Button>
            </Popconfirm>
          </Space>
          <Table
            dataSource={matches}
            columns={linesColumns}
            rowKey="id"
            loading={loading}
            size="small"
            scroll={{ x: 1100 }}
            pagination={false}
          />
        </Space>
      ),
    },
    {
      key: 'predict',
      label: 'Predict',
      children: (
        <Table
          dataSource={matches}
          columns={predictColumns}
          rowKey="id"
          loading={loading}
          size="small"
          pagination={false}
        />
      ),
    },
  ];

  return <Tabs items={tabItems} />;
}
