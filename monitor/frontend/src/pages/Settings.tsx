import { useState } from 'react'
import { useAuth } from '../auth/AuthContext'
import api from '../api/client'

export default function Settings() {
  const { user } = useAuth()
  const [oldPassword, setOldPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setMessage('')
    setError('')
    try {
      await api.post('/auth/me/password', {
        old_password: oldPassword,
        new_password: newPassword,
      })
      setMessage('Password changed successfully')
      setOldPassword('')
      setNewPassword('')
    } catch {
      setError('Failed to change password')
    }
  }

  return (
    <div className="max-w-lg space-y-8">
      <h2 className="text-2xl font-bold text-gray-100">Settings</h2>

      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-lg font-semibold text-gray-200 mb-4">Profile</h3>
        <div className="space-y-2 text-sm">
          <p><span className="text-gray-500">Username:</span> <span className="text-gray-200">{user?.username}</span></p>
          <p><span className="text-gray-500">Email:</span> <span className="text-gray-200">{user?.email}</span></p>
          <p><span className="text-gray-500">Role:</span> <span className="text-gray-200">{user?.role}</span></p>
        </div>
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-lg font-semibold text-gray-200 mb-4">Change Password</h3>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm text-gray-400 mb-1">Current password</label>
            <input
              type="password"
              value={oldPassword}
              onChange={(e) => setOldPassword(e.target.value)}
              className="w-full px-4 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 focus:outline-none focus:border-brand-500 transition"
              required
            />
          </div>
          <div>
            <label className="block text-sm text-gray-400 mb-1">New password</label>
            <input
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              className="w-full px-4 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 focus:outline-none focus:border-brand-500 transition"
              required
              minLength={8}
            />
          </div>
          {message && <p className="text-sm text-green-400">{message}</p>}
          {error && <p className="text-sm text-red-400">{error}</p>}
          <button
            type="submit"
            className="px-6 py-2 bg-brand-600 hover:bg-brand-500 rounded-lg text-white text-sm font-medium transition"
          >
            Update password
          </button>
        </form>
      </div>
    </div>
  )
}
