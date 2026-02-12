import React from 'react';
import { Routes, Route, useNavigate, useLocation } from 'react-router-dom';
import { Layout, Menu } from 'antd';
import {
  BarChartOutlined,
  DatabaseOutlined,
  TableOutlined,
  LineChartOutlined,
  FundOutlined,
} from '@ant-design/icons';

import RoykaPage from './pages/RoykaPage';
import HalfsBasePage from './pages/HalfsBasePage';
import HalfsStatsPage from './pages/HalfsStatsPage';
import TournamentSummaryPage from './pages/TournamentSummaryPage';
import HomePage from './pages/HomePage';

const { Sider, Content, Header } = Layout;

const menuItems = [
  { key: '/',             icon: <BarChartOutlined />,       label: 'Главная' },
  { key: '/royka',        icon: <FundOutlined />,           label: 'Ройка' },
  { key: '/halfs',        icon: <DatabaseOutlined />,       label: 'База половин' },
  { key: '/halfs-stats',  icon: <LineChartOutlined />,      label: 'Статистика из половин' },
  { key: '/summary',      icon: <TableOutlined />,          label: 'Сводная таблица' },
];

export default function App() {
  const navigate = useNavigate();
  const location = useLocation();

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        width={220}
        style={{
          background: '#0a0a1a',
          borderRight: '1px solid #222',
          overflow: 'auto',
          height: '100vh',
          position: 'fixed',
          left: 0,
          top: 0,
          bottom: 0,
        }}
      >
        <div
          style={{
            height: 56,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            borderBottom: '1px solid #222',
            color: '#2EC4B6',
            fontWeight: 700,
            fontSize: 16,
          }}
        >
          Excel Analyzer Pro
        </div>
        <Menu
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{ background: 'transparent', borderRight: 'none' }}
        />
      </Sider>

      <Layout style={{ marginLeft: 220 }}>
        <Header
          style={{
            background: '#0f0f1a',
            borderBottom: '1px solid #222',
            padding: '0 24px',
            display: 'flex',
            alignItems: 'center',
            height: 56,
          }}
        >
          <span style={{ fontSize: 18, fontWeight: 600 }}>
            {menuItems.find((m) => m.key === location.pathname)?.label || 'Excel Analyzer Pro'}
          </span>
        </Header>

        <Content style={{ padding: 24, minHeight: 'calc(100vh - 56px)' }}>
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/royka" element={<RoykaPage />} />
            <Route path="/halfs" element={<HalfsBasePage />} />
            <Route path="/halfs-stats" element={<HalfsStatsPage />} />
            <Route path="/summary" element={<TournamentSummaryPage />} />
          </Routes>
        </Content>
      </Layout>
    </Layout>
  );
}
