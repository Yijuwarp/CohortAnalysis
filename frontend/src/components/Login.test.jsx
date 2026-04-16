import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import Login from './Login'

// Mock the API
vi.mock('../api', () => ({
  login: vi.fn(),
}))

import { login as loginMock } from '../api'

describe('Login Component', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorage.clear()
  })

  it('renders login page correctly', () => {
    render(<Login onLogin={() => {}} />)
    expect(screen.getByText(/Login/i)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/Enter email/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Continue/i })).toBeInTheDocument()
  })

  it('calls login API and sets localStorage on success', async () => {
    loginMock.mockResolvedValue({ user_id: 'abcdef12' })
    const onLogin = vi.fn()

    render(<Login onLogin={onLogin} />)
    
    const input = screen.getByPlaceholderText(/Enter email/i)
    const button = screen.getByRole('button', { name: /Continue/i })

    fireEvent.change(input, { target: { value: 'test@example.com' } })
    fireEvent.click(button)

    await waitFor(() => {
      expect(loginMock).toHaveBeenCalledWith('test@example.com')
      expect(localStorage.getItem('user_id')).toBe('abcdef12')
      expect(onLogin).toHaveBeenCalledWith('abcdef12')
    })
  })

  it('shows error if login fails', async () => {
    loginMock.mockRejectedValue(new Error('Login failed'))
    
    render(<Login onLogin={() => {}} />)
    
    const input = screen.getByPlaceholderText(/Enter email/i)
    const button = screen.getByRole('button', { name: /Continue/i })

    fireEvent.change(input, { target: { value: 'test@example.com' } })
    fireEvent.click(button)

    await waitFor(() => {
      expect(screen.getByText(/Login failed/i)).toBeInTheDocument()
    })
  })
})
