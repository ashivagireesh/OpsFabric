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
  const isDark = mode === 'dark' || mode === 'enterprise-dark'
  const isEnterpriseDark = mode === 'enterprise-dark'

  React.useEffect(() => {
    document.documentElement.setAttribute('data-app-theme', mode)
    document.body.style.background = 'var(--app-shell-bg)'
    document.body.style.color = 'var(--app-text)'
  }, [mode])

  const config = React.useMemo(() => {
    if (isDark) {
      const accent = isEnterpriseDark ? '#007acc' : '#6366f1'
      const accentHover = isEnterpriseDark ? '#3794ff' : '#818cf8'
      const selectedBg = isEnterpriseDark ? 'rgba(0, 122, 204, 0.22)' : 'rgba(99, 102, 241, 0.15)'
      const elevatedBg = isEnterpriseDark ? '#252526' : '#22222f'

      return {
        algorithm: darkAlgorithm,
        token: {
          colorPrimary: accent,
          colorInfo: accent,
          colorBgBase: 'var(--app-shell-bg)',
          colorBgContainer: 'var(--app-card-bg)',
          colorBgElevated: elevatedBg,
          colorBorder: 'var(--app-border-strong)',
          colorBorderSecondary: 'var(--app-border)',
          colorText: 'var(--app-text)',
          colorTextSecondary: 'var(--app-text-muted)',
          colorTextTertiary: 'var(--app-text-subtle)',
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
            darkItemSelectedBg: selectedBg,
            darkItemSelectedColor: isEnterpriseDark ? '#ffffff' : accent,
            darkItemHoverBg: isEnterpriseDark ? '#2a2d2e' : 'rgba(99, 102, 241, 0.1)',
            darkItemHoverColor: isEnterpriseDark ? '#ffffff' : accent,
          },
          Card: {
            colorBgContainer: 'var(--app-card-bg)',
          },
          Table: {
            colorBgContainer: 'var(--app-card-bg)',
            headerBg: isEnterpriseDark ? '#252526' : 'var(--app-input-bg)',
            rowHoverBg: isEnterpriseDark ? '#2a2d2e' : undefined,
          },
          Select: {
            optionSelectedBg: selectedBg,
            optionActiveBg: isEnterpriseDark ? '#2a2d2e' : undefined,
          },
          Tabs: {
            itemSelectedColor: accent,
            itemHoverColor: accent,
            inkBarColor: accent,
          },
          Button: {
            colorPrimary: accent,
            colorPrimaryHover: accentHover,
            colorPrimaryActive: isEnterpriseDark ? '#005a9e' : '#4f46e5',
            primaryColor: '#ffffff',
            primaryShadow: 'none',
          },
          Input: {
            activeBorderColor: accent,
            hoverBorderColor: accentHover,
          },
          InputNumber: {
            activeBorderColor: accent,
            hoverBorderColor: accentHover,
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
  }, [darkAlgorithm, defaultAlgorithm, isDark, isEnterpriseDark])

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
