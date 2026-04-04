import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './styles.css'

import { SimpleErrorBoundary } from './App'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <SimpleErrorBoundary>
      <App />
    </SimpleErrorBoundary>
  </React.StrictMode>,
)
