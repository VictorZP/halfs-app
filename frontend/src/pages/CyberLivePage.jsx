import React, { useEffect, useMemo, useState } from 'react';
import {
  Button,
  Card,
  Empty,
  Input,
  InputNumber,
  message,
  Popconfirm,
  Space,
  Table,
  Tabs,
} from 'antd';
import { ClearOutlined, ImportOutlined, PlusOutlined, ReloadOutlined, SaveOutlined } from '@ant-design/icons';
import { cyber } from '../api/client';

const { TextArea } = Input;

const num = (v) => (v === '' || v == null || Number.isNaN(Number(v)) ? '' : Number(v).toFixed(1));

function parseLines(rawText) {
  const rows = [];
  for (const line of rawText.split('\n')) {
    if (!line.trim()) continue;
    const cells = line.split('\t').map((c) => c.trim()).filter((c) => c !== '');
    if (cells.length < 3) continue;
    const tournament = cells[0] || '';
    const team1 = cells[1] || '';
    const team2 = cells[2] || '';
    let total = null;
    const tryTotal = cells.length >= 5 ? cells[4] : cells[3];
    if (tryTotal != null) {
      const parsed = Number(String(tryTotal).replace(',', '.'));
      total = Number.isNaN(parsed) ? null : parsed;
    }
    rows.push({ tournament, team1, team2, total, calc_temp: 0 });
  }
  return rows;
}

export default function CyberLivePage() {
  const [loading, setLoading] = useState(false);
  const [inputText, setInputText] = useState('');
  const [rows, setRows] = useState([]);
  const [predictMap, setPredictMap] = useState({});

  const loadSaved = async () => {
    setLoading(true);
    try {
      const res = await cyber.getLive();
      const prepared = (res.data || []).map((r, idx) => ({
        key: `${idx}-${r.tournament}-${r.team1}-${r.team2}`,
        tournament: r.tournament || '',
        team1: r.team1 || '',
        team2: r.team2 || '',
        total: r.total ?? null,
        calc_temp: r.calc_temp ?? 0,
      }));
      setRows(prepared);
    } catch {
      message.error('Ошибка загрузки Cyber LIVE');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadSaved();
  }, []);

  useEffect(() => {
    const run = async () => {
      const next = {};
      for (const r of rows) {
        if (!r.tournament || !r.team1 || !r.team2) continue;
        try {
          const res = await cyber.getPredict(r.tournament, r.team1, r.team2);
          next[r.key] = res.data;
        } catch {
          next[r.key] = { predict: 0, temp: 0, it1: 0, it2: 0 };
        }
      }
      setPredictMap(next);
    };
    if (rows.length) run();
    else setPredictMap({});
  }, [rows]);

  const saveRows = async () => {
    setLoading(true);
    try {
      const payload = rows.map((r) => ({
        tournament: r.tournament || '',
        team1: r.team1 || '',
        team2: r.team2 || '',
        total: r.total == null || r.total === '' ? null : Number(r.total),
        calc_temp: r.calc_temp == null || r.calc_temp === '' ? 0 : Number(r.calc_temp),
      }));
      await cyber.saveLive(payload);
      message.success('Cyber LIVE сохранён');
      loadSaved();
    } catch {
      message.error('Ошибка сохранения');
      setLoading(false);
    }
  };

  const clearRows = async () => {
    setLoading(true);
    try {
      await cyber.clearLive();
      setRows([]);
      setPredictMap({});
      message.success('Cyber LIVE очищен');
    } catch {
      message.error('Ошибка очистки');
    } finally {
      setLoading(false);
    }
  };

  const importRows = () => {
    const parsed = parseLines(inputText);
    if (!parsed.length) {
      message.warning('Нет валидных строк для импорта');
      return;
    }
    const merged = [...rows, ...parsed].map((r, idx) => ({ ...r, key: `${idx}-${r.tournament}-${r.team1}-${r.team2}` }));
    merged.sort((a, b) => a.tournament.localeCompare(b.tournament));
    setRows(merged);
    setInputText('');
    message.success(`Добавлено строк: ${parsed.length}`);
  };

  const addManualRow = () => {
    setRows((prev) => [
      ...prev,
      { key: `${Date.now()}-${prev.length}`, tournament: '', team1: '', team2: '', total: null, calc_temp: 0 },
    ]);
  };

  const updateRow = (key, patch) => {
    setRows((prev) => prev.map((r) => (r.key === key ? { ...r, ...patch } : r)));
  };

  const deleteRow = (key) => {
    setRows((prev) => prev.filter((r) => r.key !== key));
  };

  const linesData = useMemo(() => {
    return rows.map((r) => {
      const pred = predictMap[r.key] || { predict: 0, temp: 0, it1: 0, it2: 0 };
      const total = r.total == null || r.total === '' ? pred.predict : Number(r.total);
      const under = total - pred.predict > 3 ? total - pred.predict : '';
      const over = pred.predict - total > 3 ? pred.predict - total : '';
      const temp = Number(pred.temp || 0);
      const calcTemp = Number(r.calc_temp || 0);
      const t2h = temp !== 0 ? (total / (2 * temp)) * ((temp + calcTemp) / 2) : 0;
      const t2hPredict =
        total && Math.abs(pred.predict - total) >= 3 ? t2h * (1 + (pred.predict - total) / total) : '';
      return { ...r, pred, totalComputed: total, under, over, t2h, t2hPredict };
    });
  }, [rows, predictMap]);

  const linesColumns = [
    {
      title: 'Турнир',
      dataIndex: 'tournament',
      width: 160,
      render: (_, row) => (
        <Input value={row.tournament} onChange={(e) => updateRow(row.key, { tournament: e.target.value })} />
      ),
    },
    {
      title: 'Команда 1',
      dataIndex: 'team1',
      width: 150,
      render: (_, row) => <Input value={row.team1} onChange={(e) => updateRow(row.key, { team1: e.target.value })} />,
    },
    {
      title: 'Команда 2',
      dataIndex: 'team2',
      width: 150,
      render: (_, row) => <Input value={row.team2} onChange={(e) => updateRow(row.key, { team2: e.target.value })} />,
    },
    {
      title: 'Тотал',
      dataIndex: 'total',
      width: 90,
      render: (_, row) => (
        <InputNumber
          value={row.total}
          onChange={(v) => updateRow(row.key, { total: v })}
          style={{ width: '100%' }}
          step={0.5}
        />
      ),
    },
    { title: 'TEMP', dataIndex: ['pred', 'temp'], width: 80, render: (_, row) => num(row.pred?.temp) },
    { title: 'Predict', dataIndex: ['pred', 'predict'], width: 90, render: (_, row) => num(row.pred?.predict) },
    {
      title: 'UNDER',
      dataIndex: 'under',
      width: 80,
      render: (v) => <span style={{ color: '#ff4d4f', fontWeight: 700 }}>{num(v)}</span>,
    },
    {
      title: 'OVER',
      dataIndex: 'over',
      width: 80,
      render: (v) => <span style={{ color: '#52c41a', fontWeight: 700 }}>{num(v)}</span>,
    },
    {
      title: 'CalcTEMP',
      dataIndex: 'calc_temp',
      width: 100,
      render: (_, row) => (
        <InputNumber
          value={row.calc_temp}
          onChange={(v) => updateRow(row.key, { calc_temp: v ?? 0 })}
          style={{ width: '100%' }}
          step={0.5}
        />
      ),
    },
    { title: 'T2H', dataIndex: 't2h', width: 80, render: (v) => num(v) },
    { title: 'T2H Predict', dataIndex: 't2hPredict', width: 100, render: (v) => num(v) },
    {
      title: '',
      key: 'actions',
      width: 70,
      render: (_, row) => (
        <Button danger size="small" onClick={() => deleteRow(row.key)}>
          Удалить
        </Button>
      ),
    },
  ];

  const predictColumns = [
    { title: 'Турнир', dataIndex: 'tournament', width: 170 },
    { title: 'Команда 1', dataIndex: 'team1', width: 170 },
    { title: 'Команда 2', dataIndex: 'team2', width: 170 },
    { title: 'TEMP', dataIndex: ['pred', 'temp'], width: 90, render: (_, row) => num(row.pred?.temp) },
    { title: 'Predict', dataIndex: ['pred', 'predict'], width: 90, render: (_, row) => num(row.pred?.predict) },
    { title: 'IT1', dataIndex: ['pred', 'it1'], width: 90, render: (_, row) => num(row.pred?.it1) },
    { title: 'IT2', dataIndex: ['pred', 'it2'], width: 90, render: (_, row) => num(row.pred?.it2) },
  ];

  return (
    <Tabs
      items={[
        {
          key: 'lines',
          label: 'Lines',
          children: (
            <Space direction="vertical" style={{ width: '100%' }}>
              <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
                <TextArea
                  rows={4}
                  value={inputText}
                  onChange={(e) => setInputText(e.target.value)}
                  placeholder="Вставьте строки (Турнир, Команда1, Команда2, Тотал)..."
                  style={{ background: '#101827', color: '#e0e0e0', border: '1px solid #333' }}
                />
                <Space wrap style={{ marginTop: 12 }}>
                  <Button type="primary" icon={<ImportOutlined />} onClick={importRows}>
                    Импортировать
                  </Button>
                  <Button icon={<PlusOutlined />} onClick={addManualRow}>Добавить строку</Button>
                  <Button icon={<SaveOutlined />} onClick={saveRows} loading={loading}>Сохранить</Button>
                  <Button icon={<ReloadOutlined />} onClick={loadSaved} loading={loading}>Перезагрузить</Button>
                  <Popconfirm title="Очистить все LIVE-строки?" onConfirm={clearRows}>
                    <Button danger icon={<ClearOutlined />}>Очистить</Button>
                  </Popconfirm>
                </Space>
              </Card>
              <Table
                dataSource={linesData}
                columns={linesColumns}
                rowKey="key"
                loading={loading}
                size="small"
                scroll={{ x: 1550 }}
                pagination={false}
              />
            </Space>
          ),
        },
        {
          key: 'predict',
          label: 'Predict',
          children: rows.length ? (
            <Table
              dataSource={linesData}
              columns={predictColumns}
              rowKey="key"
              size="small"
              scroll={{ x: 950 }}
              pagination={false}
            />
          ) : (
            <Empty description="Нет матчей в Lines" />
          ),
        },
      ]}
    />
  );
}
