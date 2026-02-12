import React, { useEffect, useState } from 'react';
import { Card, Row, Col, Statistic, Typography } from 'antd';
import {
  DatabaseOutlined,
  FundOutlined,
} from '@ant-design/icons';
import { halfs, royka } from '../api/client';

const { Title, Paragraph } = Typography;

export default function HomePage() {
  const [stats, setStats] = useState({});

  useEffect(() => {
    Promise.all([
      halfs.getStatistics().catch(() => ({ data: {} })),
      royka.getStatistics().catch(() => ({ data: {} })),
    ]).then(([h, r]) => {
      setStats({
        halfsMatches: h.data.total_matches || 0,
        halfsTournaments: h.data.tournaments || 0,
        roykaRecords: r.data.total_records || 0,
        roykaTournaments: r.data.tournaments_count || 0,
      });
    });
  }, []);

  return (
    <div>
      <Title level={2} style={{ color: '#e0e0e0', marginBottom: 24 }}>
        Добро пожаловать
      </Title>
      <Paragraph style={{ color: '#999', marginBottom: 32, fontSize: 16 }}>
        Excel Analyzer Pro — анализ баскетбольной статистики. Выберите раздел в меню слева.
      </Paragraph>

      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12}>
          <Card style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic
              title={<span style={{ color: '#999' }}>База половин</span>}
              value={stats.halfsMatches || 0}
              suffix="матчей"
              prefix={<DatabaseOutlined style={{ color: '#2EC4B6' }} />}
              valueStyle={{ color: '#e0e0e0' }}
            />
            <div style={{ color: '#666', marginTop: 8 }}>
              {stats.halfsTournaments || 0} турниров
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12}>
          <Card style={{ background: '#1a1a2e', border: '1px solid #333' }}>
            <Statistic
              title={<span style={{ color: '#999' }}>Ройка</span>}
              value={stats.roykaRecords || 0}
              suffix="записей"
              prefix={<FundOutlined style={{ color: '#f5a623' }} />}
              valueStyle={{ color: '#e0e0e0' }}
            />
            <div style={{ color: '#666', marginTop: 8 }}>
              {stats.roykaTournaments || 0} турниров
            </div>
          </Card>
        </Col>
      </Row>
    </div>
  );
}
