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
import { UploadOutlined, PlayCircleOutlined, DownloadOutlined, ReloadOutlined, FolderOpenOutlined } from '@ant-design/icons';
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
  const [sourceHandle, setSourceHandle] = useState(null);
  const [destinationHandle, setDestinationHandle] = useState(null);
  const [loading, setLoading] = useState(false);
  const [summaryRows, setSummaryRows] = useState([]);
  const [sheetsLoading, setSheetsLoading] = useState(false);
  const [tournamentSheets, setTournamentSheets] = useState([]);
  const [tournamentLinks, setTournamentLinks] = useState({});
  const [processedBlob, setProcessedBlob] = useState(null);
  const [processedFileName, setProcessedFileName] = useState('sorted_destination.xlsx');

  const canRun = useMemo(() => !!sourceFile && !!destinationFile, [sourceFile, destinationFile]);
  const fileAccessSupported = typeof window !== 'undefined' && typeof window.showOpenFilePicker === 'function';

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

  const triggerDownload = (blob, fileName) => {
    if (!blob) return;
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = fileName || 'sorted_destination.xlsx';
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  };

  const pickFileWithHandle = async ({ asDestination }) => {
    if (!fileAccessSupported) {
      message.warning('Ваш браузер не поддерживает прямую работу с локальным файлом. Используйте загрузку и скачивание.');
      return;
    }
    try {
      const [handle] = await window.showOpenFilePicker({
        multiple: false,
        excludeAcceptAllOption: false,
        types: [
          {
            description: 'Excel Files',
            accept: {
              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
              'application/vnd.ms-excel.sheet.macroEnabled.12': ['.xlsm'],
            },
          },
        ],
      });
      if (!handle) return;
      const file = await handle.getFile();
      if (asDestination) {
        setDestinationHandle(handle);
        setDestinationFile(file);
        setTournamentSheets([]);
        message.success(`Выбран файл назначения: ${file.name}`);
      } else {
        setSourceHandle(handle);
        setSourceFile(file);
        message.success(`Выбран исходный файл: ${file.name}`);
      }
    } catch (e) {
      if (e?.name !== 'AbortError') {
        message.error('Не удалось открыть файл');
      }
    }
  };

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
      const blob = new Blob([response.data], {
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      });
      setProcessedBlob(blob);
      setProcessedFileName(fileName);

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

      if (destinationHandle && typeof destinationHandle.createWritable === 'function') {
        try {
          const writable = await destinationHandle.createWritable();
          await writable.write(blob);
          await writable.close();
          const updatedFile = await destinationHandle.getFile();
          setDestinationFile(updatedFile);
          message.success('Сортировка завершена: файл назначения обновлён (как в локальной версии)');
        } catch {
          triggerDownload(blob, fileName);
          message.warning('Не удалось записать в файл назначения. Результат скачан отдельным файлом.');
        }
      } else {
        triggerDownload(blob, fileName);
        message.success('Сортировка завершена: результат скачан');
      }
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
                        setSourceHandle(null);
                        return false;
                      }}
                      onRemove={() => {
                        setSourceFile(null);
                        setSourceHandle(null);
                      }}
                      fileList={sourceFile ? [sourceFile] : []}
                    >
                      <Button icon={<UploadOutlined />}>Выбрать исходный файл</Button>
                    </Upload>
                    {fileAccessSupported && (
                      <Button icon={<FolderOpenOutlined />} onClick={() => pickFileWithHandle({ asDestination: false })}>
                        Открыть исходный как локальный файл
                      </Button>
                    )}

                    <Upload
                      maxCount={1}
                      beforeUpload={(file) => {
                        setDestinationFile(file);
                        setDestinationHandle(null);
                        setTournamentSheets([]);
                        return false;
                      }}
                      onRemove={() => {
                        setDestinationFile(null);
                        setDestinationHandle(null);
                        setTournamentSheets([]);
                      }}
                      fileList={destinationFile ? [destinationFile] : []}
                    >
                      <Button icon={<UploadOutlined />}>Выбрать файл назначения</Button>
                    </Upload>
                    {fileAccessSupported && (
                      <Button icon={<FolderOpenOutlined />} onClick={() => pickFileWithHandle({ asDestination: true })}>
                        Открыть файл назначения как локальный файл
                      </Button>
                    )}

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
                      <Button
                        icon={<DownloadOutlined />}
                        disabled={!processedBlob}
                        onClick={() => triggerDownload(processedBlob, processedFileName)}
                      >
                        Скачать результат ещё раз
                      </Button>
                    </Space>
                    <Text style={{ color: '#888' }}>
                      {destinationHandle
                        ? 'Режим локального файла: результат записывается прямо в выбранный файл назначения.'
                        : 'Режим браузера: результат сохраняется как скачанный файл.'}
                    </Text>
                  </Space>
                </Card>

                <Alert
                  type="info"
                  showIcon
                  style={{ marginBottom: 16 }}
                  message="Логика сортировки полностью как в локальной версии; отличается только способ сохранения файла в браузере."
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
