import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Input,
  Space,
  Table,
  Tabs,
  Typography,
  Upload,
  message,
} from 'antd';
import { UploadOutlined, PlayCircleOutlined, DownloadOutlined, ReloadOutlined } from '@ant-design/icons';
import { sortHalves } from '../api/client';

const { Title, Text } = Typography;
const LINKS_STORAGE_KEY = 'sortHalvesTournamentLinks';

function getFilenameFromDisposition(disposition) {
  if (!disposition) return 'sorted_destination.xlsx';
  const match = disposition.match(/filename="?(.*?)"?$/i);
  return match?.[1] || 'sorted_destination.xlsx';
}

export default function SortHalvesPage() {
  const [sourceFile, setSourceFile] = useState(null);
  const [destinationFile, setDestinationFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [summaryRows, setSummaryRows] = useState([]);
  const [sheetsLoading, setSheetsLoading] = useState(false);
  const [tournamentSheets, setTournamentSheets] = useState([]);
  const [tournamentLinks, setTournamentLinks] = useState({});

  const canRun = useMemo(() => !!sourceFile && !!destinationFile, [sourceFile, destinationFile]);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(LINKS_STORAGE_KEY);
      if (raw) {
        setTournamentLinks(JSON.parse(raw));
      }
    } catch {
      setTournamentLinks({});
    }
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem(LINKS_STORAGE_KEY, JSON.stringify(tournamentLinks));
    } catch {
      // ignore storage errors
    }
  }, [tournamentLinks]);

  const loadSheets = async (file = destinationFile) => {
    if (!file) {
      message.warning('Сначала выберите файл назначения');
      return;
    }
    setSheetsLoading(true);
    try {
      const res = await sortHalves.getSheets(file);
      setTournamentSheets(res.data?.sheets || []);
      message.success(`Загружено турниров: ${res.data?.sheets?.length || 0}`);
    } catch (error) {
      const detail = error?.response?.data?.detail;
      message.error(typeof detail === 'string' ? detail : 'Ошибка чтения листов');
    } finally {
      setSheetsLoading(false);
    }
  };

  const runSort = async () => {
    if (!canRun) {
      message.warning('Выберите исходный и файл назначения');
      return;
    }

    setLoading(true);
    try {
      const response = await sortHalves.process(sourceFile, destinationFile);
      const disposition = response.headers?.['content-disposition'];
      const summaryHeader = response.headers?.['x-games-summary'];
      const fileName = getFilenameFromDisposition(disposition);

      if (summaryHeader) {
        try {
          const parsed = JSON.parse(summaryHeader) || [];
          const withLinks = parsed.map((row, idx) => ({
            key: `${row.tournament}-${idx}`,
            ...row,
            link: tournamentLinks[row.tournament] || '',
          }));
          withLinks.sort((a, b) => {
            const aMismatch = a.inserted !== a.normative ? 0 : 1;
            const bMismatch = b.inserted !== b.normative ? 0 : 1;
            if (aMismatch !== bMismatch) return aMismatch - bMismatch;
            return String(a.tournament).localeCompare(String(b.tournament));
          });
          setSummaryRows(withLinks);
        } catch {
          setSummaryRows([]);
        }
      } else {
        setSummaryRows([]);
      }

      const blob = new Blob([response.data], {
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = fileName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);

      message.success('Сортировка завершена, файл скачан');
    } catch (error) {
      const detail = error?.response?.data?.detail;
      message.error(typeof detail === 'string' ? detail : 'Ошибка при сортировке файлов');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <Title level={3} style={{ marginTop: 0 }}>
        Сортировка половин
      </Title>

      <Tabs
        items={[
          {
            key: 'sort',
            label: 'Сортировка',
            children: (
              <>
                <Card style={{ marginBottom: 16, background: '#1a1a2e', border: '1px solid #333' }}>
                  <Space direction="vertical" size={12} style={{ width: '100%' }}>
                    <Text style={{ color: '#bdbdbd' }}>
                      Загрузите 2 файла: исходный (с листом "Четверти") и файл назначения (по листам турниров).
                    </Text>

                    <Upload
                      maxCount={1}
                      beforeUpload={(file) => {
                        setSourceFile(file);
                        return false;
                      }}
                      onRemove={() => setSourceFile(null)}
                      fileList={sourceFile ? [sourceFile] : []}
                    >
                      <Button icon={<UploadOutlined />}>Выбрать исходный файл</Button>
                    </Upload>

                    <Upload
                      maxCount={1}
                      beforeUpload={(file) => {
                        setDestinationFile(file);
                        setTournamentSheets([]);
                        return false;
                      }}
                      onRemove={() => {
                        setDestinationFile(null);
                        setTournamentSheets([]);
                      }}
                      fileList={destinationFile ? [destinationFile] : []}
                    >
                      <Button icon={<UploadOutlined />}>Выбрать файл назначения</Button>
                    </Upload>

                    <Space>
                      <Button
                        icon={<ReloadOutlined />}
                        onClick={() => loadSheets()}
                        disabled={!destinationFile}
                        loading={sheetsLoading}
                      >
                        Загрузить турниры из файла назначения
                      </Button>
                      <Button
                        type="primary"
                        icon={<PlayCircleOutlined />}
                        onClick={runSort}
                        disabled={!canRun}
                        loading={loading}
                      >
                        Запустить сортировку
                      </Button>
                    </Space>
                  </Space>
                </Card>

                <Alert
                  type="info"
                  showIcon
                  style={{ marginBottom: 16 }}
                  message="После обработки браузер автоматически скачает обновлённый файл назначения."
                />

                <Card
                  title={(
                    <span>
                      Сводка по турнирам <DownloadOutlined />
                    </span>
                  )}
                  style={{ background: '#1a1a2e', border: '1px solid #333' }}
                >
                  <Table
                    size="small"
                    pagination={{ pageSize: 20 }}
                    dataSource={summaryRows}
                    columns={[
                      { title: 'Турнир', dataIndex: 'tournament' },
                      {
                        title: 'Добавлено',
                        dataIndex: 'inserted',
                        width: 140,
                        render: (value, row) => (
                          <span style={{ color: row.inserted !== row.normative ? '#ff7875' : '#d9d9d9', fontWeight: 600 }}>
                            {value}
                          </span>
                        ),
                      },
                      {
                        title: 'Норма',
                        dataIndex: 'normative',
                        width: 140,
                        render: (value, row) => (
                          <span style={{ color: row.inserted !== row.normative ? '#ff7875' : '#d9d9d9', fontWeight: 600 }}>
                            {value}
                          </span>
                        ),
                      },
                      {
                        title: 'Ссылка',
                        dataIndex: 'link',
                        render: (value, row) => (
                          row.inserted !== row.normative && value ? (
                            <a href={value} target="_blank" rel="noreferrer">
                              {value}
                            </a>
                          ) : (
                            <span style={{ color: '#777' }}>{value || '-'}</span>
                          )
                        ),
                      },
                    ]}
                  />
                </Card>
              </>
            ),
          },
          {
            key: 'links',
            label: 'Ссылки на турниры',
            children: (
              <Card style={{ background: '#1a1a2e', border: '1px solid #333' }}>
                <Space direction="vertical" size={12} style={{ width: '100%' }}>
                  <Text style={{ color: '#bdbdbd' }}>
                    Укажите ссылку на расписание для каждого турнира из файла назначения.
                  </Text>
                  <Space>
                    <Button
                      icon={<ReloadOutlined />}
                      onClick={() => loadSheets()}
                      disabled={!destinationFile}
                      loading={sheetsLoading}
                    >
                      Загрузить турниры
                    </Button>
                    <Text style={{ color: '#888' }}>
                      {destinationFile ? `Файл: ${destinationFile.name}` : 'Файл назначения не выбран'}
                    </Text>
                  </Space>
                  <Table
                    size="small"
                    pagination={{ pageSize: 20 }}
                    dataSource={tournamentSheets.map((name) => ({ key: name, tournament: name }))}
                    columns={[
                      { title: 'Турнир', dataIndex: 'tournament', width: 280 },
                      {
                        title: 'Ссылка на расписание',
                        render: (_, row) => (
                          <Input
                            value={tournamentLinks[row.tournament] || ''}
                            placeholder="https://..."
                            onChange={(e) => {
                              const value = e.target.value;
                              setTournamentLinks((prev) => ({ ...prev, [row.tournament]: value }));
                            }}
                          />
                        ),
                      },
                    ]}
                  />
                </Space>
              </Card>
            ),
          },
        ]}
      />
    </div>
  );
}
