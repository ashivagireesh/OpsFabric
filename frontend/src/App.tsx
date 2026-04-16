import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { Component, ReactNode } from 'react'
import AppLayout from './layouts/AppLayout'
import Dashboard from './pages/Dashboard'
import PipelineList from './pages/PipelineList'
import PipelineEditor from './pages/PipelineEditor'
import ExecutionList from './pages/ExecutionList'
import CredentialManager from './pages/CredentialManager'
import Settings from './pages/Settings'
import NLPChatStudio from './pages/NLPChatStudio'
import WorkflowChatStudio from './pages/WorkflowChatStudio'
import DashboardGallery from './pages/viz/DashboardGallery'
import DashboardEditor from './pages/viz/DashboardEditor'
import DashboardViewer from './pages/viz/DashboardViewer'
import SharedDashboardPage from './pages/viz/SharedDashboardPage'
import PythonAnalyticsStudio from './pages/viz/PythonAnalyticsStudio'
import MLOpsList from './pages/mlops/MLOpsList'
import MLOpsEditor from './pages/mlops/MLOpsEditor'
import BusinessWorkflowList from './pages/business/BusinessWorkflowList'
import BusinessWorkflowEditor from './pages/business/BusinessWorkflowEditor'
import AdminPanel from './pages/AdminPanel'

class ErrorBoundary extends Component<{ children: ReactNode }, { error: string | null }> {
  state = { error: null }
  static getDerivedStateFromError(e: Error) { return { error: e.message } }
  render() {
    if (this.state.error) return (
      <div style={{ padding: 40, color: '#ef4444', fontFamily: 'monospace', background: 'var(--app-shell-bg-2)', minHeight: '100vh' }}>
        <h2 style={{ color: '#ef4444' }}>Render Error</h2>
        <pre style={{ whiteSpace: 'pre-wrap', color: '#fca5a5', fontSize: 13 }}>{this.state.error}</pre>
        <button onClick={() => { this.setState({ error: null }); window.history.back(); }} style={{ marginTop: 16, padding: '8px 16px', background: '#6366f1', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>← Go Back</button>
      </div>
    )
    return this.props.children
  }
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* Full-screen editors — no sidebar */}
        <Route path="/pipelines/:id/edit" element={<PipelineEditor />} />
        <Route path="/dashboards/:id/edit" element={<ErrorBoundary><DashboardEditor /></ErrorBoundary>} />
        <Route path="/mlops/:id/edit" element={<MLOpsEditor />} />
        <Route path="/business/:id/edit" element={<BusinessWorkflowEditor />} />
        <Route path="/share/:token" element={<SharedDashboardPage />} />
        <Route path="/embed/:token" element={<SharedDashboardPage />} />

        {/* All other pages use the main layout */}
        <Route element={<AppLayout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/pipelines" element={<PipelineList />} />
          <Route path="/executions" element={<ExecutionList />} />
          <Route path="/mlops" element={<MLOpsList />} />
          <Route path="/business" element={<BusinessWorkflowList />} />
          <Route path="/credentials" element={<CredentialManager />} />
          <Route path="/settings" element={<Settings />} />
          <Route path="/dashboards" element={<DashboardGallery />} />
          <Route path="/python-analytics" element={<PythonAnalyticsStudio />} />
          <Route path="/nlp-chat" element={<NLPChatStudio />} />
          <Route path="/workflow-chat" element={<WorkflowChatStudio />} />
          <Route path="/dashboards/:id" element={<DashboardViewer />} />
          <Route path="/admin" element={<AdminPanel />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
