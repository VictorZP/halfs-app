import React, { useState, useCallback, useEffect } from 'react';
import { Routes, Route, useNavigate, useLocation } from 'react-router-dom';
import { Layout, Menu, Button, Tooltip, Drawer, Grid } from 'antd';
import {
  BarChartOutlined,
  DatabaseOutlined,
  TableOutlined,
  LineChartOutlined,
  FundOutlined,
  LogoutOutlined,
  MenuOutlined,
} from '@ant-design/icons';

import LoginPage from './pages/LoginPage';
import RoykaPage from './pages/RoykaPage';
import HalfsBasePage from './pages/HalfsBasePage';
import HalfsStatsPage from './pages/HalfsStatsPage';
import TournamentSummaryPage from './pages/TournamentSummaryPage';
import HomePage from './pages/HomePage';

const { Content, Header, Sider } = Layout;
const { useBreakpoint } = Grid;

const menuItems = [
  { key: '/',             icon: <BarChartOutlined />,  label: 'Главная' },
  { key: '/royka',        icon: <FundOutlined />,      label: 'Ройка' },
  { key: '/halfs',        icon: <DatabaseOutlined />,  label: 'База половин' },
  { key: '/halfs-stats',  icon: <LineChartOutlined />, label: 'Статистика из половин' },
  { key: '/summary',      icon: <TableOutlined />,     label: 'Сводная таблица' },
];

function SideMenu({ selectedKey, onSelect, onLogout }) {
  return (
    <>
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
        selectedKeys={[selectedKey]}
        items={menuItems}
        onClick={({ key }) => onSelect(key)}
        style={{ background: 'transparent', borderRight: 'none', flex: 1 }}
      />
      <div style={{ padding: 16, textAlign: 'center', borderTop: '1px solid #222' }}>
        <Button type="text" icon={<LogoutOutlined />} onClick={onLogout} style={{ color: '#888' }}>
          Выйти
        </Button>
      </div>
    </>
  );
}

export default function App() {
  const navigate = useNavigate();
  const location = useLocation();
  const screens = useBreakpoint();
  const isMobile = !screens.md;
  const [token, setToken] = useState(() => localStorage.getItem('token'));
  const [drawerOpen, setDrawerOpen] = useState(false);

  const handleLogin = useCallback((newToken) => {
    setToken(newToken);
  }, []);

  const handleLogout = useCallback(() => {
    localStorage.removeItem('token');
    setToken(null);
    setDrawerOpen(false);
  }, []);

  const handleMenuSelect = useCallback((key) => {
    navigate(key);
    setDrawerOpen(false);
  }, [navigate]);

  useEffect(() => {
    setDrawerOpen(false);
  }, [location.pathname]);

  if (!token) {
    return <LoginPage onLogin={handleLogin} />;
  }

  return (
    <Layout style={{ minHeight: '100vh' }}>
      {isMobile ? (
        <Drawer
          placement="left"
          open={drawerOpen}
          onClose={() => setDrawerOpen(false)}
          width={260}
          bodyStyle={{ background: '#0a0a1a', padding: 0, display: 'flex', flexDirection: 'column' }}
          headerStyle={{ display: 'none' }}
        >
          <SideMenu selectedKey={location.pathname} onSelect={handleMenuSelect} onLogout={handleLogout} />
        </Drawer>
      ) : (
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
            display: 'flex',
            flexDirection: 'column',
          }}
        >
          <SideMenu selectedKey={location.pathname} onSelect={handleMenuSelect} onLogout={handleLogout} />
        </Sider>
      )}

      <Layout style={{ marginLeft: isMobile ? 0 : 220 }}>
        <Header
          style={{
            background: '#0f0f1a',
            borderBottom: '1px solid #222',
            padding: '0 16px',
            display: 'flex',
            alignItems: 'center',
            height: 56,
            gap: 12,
          }}
        >
          {isMobile && (
            <Button
              type="text"
              icon={<MenuOutlined />}
              onClick={() => setDrawerOpen(true)}
              style={{ color: '#e0e0e0', fontSize: 18 }}
            />
          )}
          <span style={{ fontSize: isMobile ? 15 : 18, fontWeight: 600 }}>
            {menuItems.find((m) => m.key === location.pathname)?.label || 'Excel Analyzer Pro'}
          </span>
        </Header>

        <Content style={{ padding: isMobile ? 12 : 24, minHeight: 'calc(100vh - 56px)', overflow: 'auto' }}>
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
