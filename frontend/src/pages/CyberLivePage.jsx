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
const hasCalcTemp = (v) => {
  const parsed = Number(v);
  return Number.isFinite(parsed) && Math.abs(parsed) > 0.000001;
};

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
  const [archiveLoading, setArchiveLoading] = useState(false);
  const [archiveRows, setArchiveRows] = useState([]);
  const [archiveQuery, setArchiveQuery] = useState('');
  const [selectedArchiveRowKeys, setSelectedArchiveRowKeys] = useState([]);

  const loadSaved = async () => {
    setLoading(true);
    try {
      const res = await cyber.getLive();
      const prepared = (res.data || []).map((r, idx) => ({
        key: `${idx}-${r.tournament}-${r.team1}-${r.team2}`,
        id: r.id ?? null,
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

  const loadArchive = async () => {
    setArchiveLoading(true);
    try {
      const res = await cyber.getLiveArchive();
      setArchiveRows(res.data || []);
    } catch {
      message.error('Ошибка загрузки архива Cyber LIVE');
    } finally {
      setArchiveLoading(false);
    }
  };

  useEffect(() => {
    loadArchive();
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
      { key: `${Date.now()}-${prev.length}`, id: null, tournament: '', team1: '', team2: '', total: null, calc_temp: 0 },
    ]);
  };

  const updateRow = (key, patch) => {
    setRows((prev) => prev.map((r) => (r.key === key ? { ...r, ...patch } : r)));
  };

  const archiveRow = async (row) => {
    if (!hasCalcTemp(row.calc_temp)) {
      message.warning('Нельзя архивировать матч без CalcTEMP');
      return;
    }
    try {
      const payload = {
        live_row_id: row.id ?? null,
        tournament: row.tournament || '',
        team1: row.team1 || '',
        team2: row.team2 || '',
        total: row.totalComputed ?? row.total ?? null,
        calc_temp: row.calc_temp ?? 0,
        temp: row.pred?.temp ?? 0,
        predict: row.pred?.predict ?? 0,
        under_value: row.under === '' ? null : Number(row.under),
        over_value: row.over === '' ? null : Number(row.over),
        t2h: row.t2h ?? 0,
        t2h_predict: row.t2hPredict === '' ? null : Number(row.t2hPredict),
      };
      const res = await cyber.archiveLive(payload);
      const result = res.data || {};
      if (result.deleted_from_live > 0) {
        setRows((prev) => prev.filter((r) => r.key !== row.key));
      }
      if (result.archived || result.updated_existing) {
        await loadArchive();
        message.success(result.message || 'Матч отправлен в архив');
      } else {
        message.warning(result.message || 'Матч не добавлен в архив');
      }
    } catch {
      message.error('Ошибка архивирования');
    }
  };

  const clearArchive = async () => {
    setArchiveLoading(true);
    try {
      await cyber.clearLiveArchive();
      setArchiveRows([]);
      message.success('Архив Cyber LIVE очищен');
    } catch {
      message.error('Ошибка очистки архива');
    } finally {
      setArchiveLoading(false);
    }
  };

  const deleteSelectedArchive = async () => {
    if (!selectedArchiveRowKeys.length) return;
    setArchiveLoading(true);
    try {
      const ids = selectedArchiveRowKeys.map((v) => Number(v)).filter((v) => Number.isFinite(v));
      const res = await cyber.deleteLiveArchiveSelected(ids);
      const deleted = res.data?.deleted ?? ids.length;
      message.success(`Удалено из архива: ${deleted}`);
      setSelectedArchiveRowKeys([]);
      await loadArchive();
    } catch {
      message.error('Ошибка удаления выбранных матчей из архива');
      setArchiveLoading(false);
    }
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
      width: 140,
      render: (_, row) => (
        <Space direction="vertical" size={2} style={{ width: '100%' }}>
          <InputNumber
            value={row.calc_temp}
            onChange={(v) => updateRow(row.key, { calc_temp: v ?? 0 })}
            style={{ width: '100%' }}
            step={0.5}
          />
          {!hasCalcTemp(row.calc_temp) && (
            <span style={{ color: '#8c8c8c', fontSize: 11 }}>
              CalcTEMP пустой
            </span>
          )}
        </Space>
      ),
    },
    { title: 'T2H', dataIndex: 't2h', width: 80, render: (v) => num(v) },
    { title: 'T2H Predict', dataIndex: 't2hPredict', width: 100, render: (v) => num(v) },
    {
      title: '',
      key: 'actions',
      width: 90,
      render: (_, row) => (
        <Button
          danger
          size="small"
          onClick={() => archiveRow(row)}
          disabled={!hasCalcTemp(row.calc_temp)}
          title={!hasCalcTemp(row.calc_temp) ? 'Заполните CalcTEMP для архивации' : ''}
        >
          В архив
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

  const filteredArchiveRows = useMemo(() => {
    const q = archiveQuery.trim().toLowerCase();
    if (!q) return archiveRows;
    return archiveRows.filter((r) => (
      String(r.tournament || '').toLowerCase().includes(q)
      || String(r.team1 || '').toLowerCase().includes(q)
      || String(r.team2 || '').toLowerCase().includes(q)
    ));
  }, [archiveRows, archiveQuery]);

  const archiveColumns = [
    { title: 'Архив', dataIndex: 'archived_at', width: 170 },
    { title: 'Турнир', dataIndex: 'tournament', width: 170 },
    { title: 'Команда 1', dataIndex: 'team1', width: 150 },
    { title: 'Команда 2', dataIndex: 'team2', width: 150 },
    { title: 'Тотал', dataIndex: 'total', width: 90, render: (v) => num(v) },
    { title: 'TEMP', dataIndex: 'temp', width: 80, render: (v) => num(v) },
    { title: 'Predict', dataIndex: 'predict', width: 90, render: (v) => num(v) },
    { title: 'UNDER', dataIndex: 'under_value', width: 80, render: (v) => <span style={{ color: '#ff4d4f', fontWeight: 700 }}>{num(v)}</span> },
    { title: 'OVER', dataIndex: 'over_value', width: 80, render: (v) => <span style={{ color: '#52c41a', fontWeight: 700 }}>{num(v)}</span> },
    { title: 'CalcTEMP', dataIndex: 'calc_temp', width: 100, render: (v) => num(v) },
    { title: 'T2H', dataIndex: 't2h', width: 80, render: (v) => num(v) },
    { title: 'T2H Predict', dataIndex: 't2h_predict', width: 100, render: (v) => num(v) },
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
        {
          key: 'archive',
          label: 'Архив',
          children: (
            <Space direction="vertical" style={{ width: '100%' }}>
              <Card size="small" style={{ background: '#1a1a2e', border: '1px solid #333' }}>
                <Space wrap>
                  <Input
                    value={archiveQuery}
                    onChange={(e) => setArchiveQuery(e.target.value)}
                    placeholder="Поиск: турнир / команда"
                    style={{ width: 320 }}
                    allowClear
                  />
                  <Button icon={<ReloadOutlined />} onClick={loadArchive} loading={archiveLoading}>
                    Обновить архив
                  </Button>
                  <Popconfirm
                    title={`Удалить выбранные матчи (${selectedArchiveRowKeys.length}) из архива?`}
                    onConfirm={deleteSelectedArchive}
                    disabled={!selectedArchiveRowKeys.length}
                  >
                    <Button danger disabled={!selectedArchiveRowKeys.length} loading={archiveLoading}>
                      Удалить выбранные
                    </Button>
                  </Popconfirm>
                  <Popconfirm title="Очистить весь архив Cyber LIVE?" onConfirm={clearArchive}>
                    <Button danger icon={<ClearOutlined />} loading={archiveLoading}>
                      Очистить архив
                    </Button>
                  </Popconfirm>
                </Space>
              </Card>
              <Table
                dataSource={filteredArchiveRows}
                columns={archiveColumns}
                rowKey="id"
                loading={archiveLoading}
                rowSelection={{
                  selectedRowKeys: selectedArchiveRowKeys,
                  onChange: (keys) => setSelectedArchiveRowKeys(keys),
                }}
                size="small"
                scroll={{ x: 1650 }}
                pagination={{ pageSize: 50 }}
              />
            </Space>
          ),
        },
      ]}
    />
  );
}
