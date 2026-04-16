import React from 'react'
import ReactDOM from 'react-dom/client'
import { ConfigProvider, theme } from 'antd'
import App from './App'
import { useThemeStore } from './store/themeStore'
import 'antd/dist/reset.css'
import 'reactflow/dist/style.css'
import './styles/theme.css'

const { darkAlgorithm, defaultAlgorithm } = theme

function AppThemeProvider() {
  const mode = useThemeStore((state) => state.mode)
  const isDark = mode === 'dark'

  React.useEffect(() => {
    document.documentElement.setAttribute('data-app-theme', mode)
    document.body.style.background = 'var(--app-shell-bg)'
    document.body.style.color = 'var(--app-text)'
  }, [isDark, mode])

  const config = React.useMemo(() => {
    if (isDark) {
      return {
        algorithm: darkAlgorithm,
        token: {
          colorPrimary: '#6366f1',
          colorBgBase: 'var(--app-shell-bg)',
          colorBgContainer: 'var(--app-card-bg)',
          colorBgElevated: '#22222f',
          colorBorder: 'var(--app-border-strong)',
          colorBorderSecondary: 'var(--app-border)',
          borderRadius: 8,
          fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
        },
        components: {
          Layout: {
            siderBg: 'var(--app-panel-bg)',
            headerBg: 'var(--app-panel-bg)',
            bodyBg: 'var(--app-shell-bg)',
          },
          Menu: {
            darkItemBg: 'var(--app-panel-bg)',
            darkSubMenuItemBg: 'var(--app-panel-bg)',
            darkItemSelectedBg: 'rgba(99, 102, 241, 0.15)',
            darkItemSelectedColor: '#6366f1',
          },
          Card: {
            colorBgContainer: 'var(--app-card-bg)',
          },
          Table: {
            colorBgContainer: 'var(--app-card-bg)',
            headerBg: 'var(--app-input-bg)',
          },
        },
      }
    }

    return {
      algorithm: defaultAlgorithm,
      token: {
        colorPrimary: '#4f46e5',
        colorBgBase: '#f5f7fb',
        colorBgContainer: '#ffffff',
        colorBgElevated: '#ffffff',
        colorBorder: '#dbe4f0',
        colorBorderSecondary: '#e2e8f0',
        borderRadius: 8,
        fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
      },
      components: {
        Layout: {
          siderBg: '#ffffff',
          headerBg: '#ffffff',
          bodyBg: '#f5f7fb',
        },
        Menu: {
          itemBg: '#ffffff',
          itemColor: '#334155',
          itemHoverColor: '#1f2937',
          itemSelectedBg: 'rgba(79, 70, 229, 0.12)',
          itemSelectedColor: '#4338ca',
          subMenuItemBg: '#ffffff',
        },
        Card: {
          colorBgContainer: '#ffffff',
        },
        Table: {
          colorBgContainer: '#ffffff',
          headerBg: '#f8fafc',
        },
      },
    }
  }, [darkAlgorithm, defaultAlgorithm, isDark])

  return (
    <ConfigProvider theme={config}>
      <App />
    </ConfigProvider>
  )
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <AppThemeProvider />
  </React.StrictMode>
)
