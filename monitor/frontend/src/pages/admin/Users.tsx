import { useEffect, useState } from 'react'
import api from '../../api/client'
import { Plus, Trash2, Edit2, X, Check } from 'lucide-react'

interface User {
  id: string
  username: string
  email: string
  role: string
  is_active: boolean
  created_at: string
  last_login: string | null
}

export default function AdminUsers() {
  const [users, setUsers] = useState<User[]>([])
  const [showCreate, setShowCreate] = useState(false)
  const [form, setForm] = useState({ username: '', email: '', password: '', role: 'user' })
  const [error, setError] = useState('')

  const load = () => api.get('/admin/users').then((r) => setUsers(r.data))

  useEffect(() => { load() }, [])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    try {
      await api.post('/admin/users', form)
      setForm({ username: '', email: '', password: '', role: 'user' })
      setShowCreate(false)
      load()
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to create user')
    }
  }

  const toggleActive = async (u: User) => {
    await api.patch(`/admin/users/${u.id}`, { is_active: !u.is_active })
    load()
  }

  const deleteUser = async (u: User) => {
    if (!confirm(`Delete user "${u.username}"?`)) return
    await api.delete(`/admin/users/${u.id}`)
    load()
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-gray-100">Users</h2>
        <button
          onClick={() => setShowCreate(!showCreate)}
          className="flex items-center gap-2 px-4 py-2 bg-brand-600 hover:bg-brand-500 rounded-lg text-white text-sm font-medium transition"
        >
          <Plus size={16} /> New user
        </button>
      </div>

      {/* Create form */}
      {showCreate && (
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <form onSubmit={handleCreate} className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <input
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
              placeholder="Username"
              required
              minLength={3}
              className="px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
            />
            <input
              value={form.email}
              onChange={(e) => setForm({ ...form, email: e.target.value })}
              placeholder="Email"
              type="email"
              required
              className="px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
            />
            <input
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
              placeholder="Password"
              type="password"
              required
              minLength={8}
              className="px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
            />
            <div className="flex gap-2">
              <select
                value={form.role}
                onChange={(e) => setForm({ ...form, role: e.target.value })}
                className="flex-1 px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
              >
                <option value="user">User</option>
                <option value="admin">Admin</option>
              </select>
              <button type="submit" className="px-4 py-2 bg-green-600 hover:bg-green-500 rounded-lg text-white text-sm transition">
                <Check size={16} />
              </button>
              <button type="button" onClick={() => setShowCreate(false)} className="px-3 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg text-gray-300 text-sm transition">
                <X size={16} />
              </button>
            </div>
          </form>
          {error && <p className="text-sm text-red-400 mt-2">{error}</p>}
        </div>
      )}

      {/* Users table */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-500 border-b border-gray-800">
              <th className="text-left py-2 px-2">Username</th>
              <th className="text-left py-2 px-2">Email</th>
              <th className="text-left py-2 px-2">Role</th>
              <th className="text-center py-2 px-2">Active</th>
              <th className="text-left py-2 px-2">Last login</th>
              <th className="text-right py-2 px-2">Actions</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.id} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                <td className="py-2 px-2 text-gray-200 font-medium">{u.username}</td>
                <td className="py-2 px-2 text-gray-400">{u.email}</td>
                <td className="py-2 px-2">
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                    u.role === 'admin' ? 'bg-brand-600/20 text-brand-400' : 'bg-gray-700 text-gray-300'
                  }`}>
                    {u.role}
                  </span>
                </td>
                <td className="py-2 px-2 text-center">
                  <button onClick={() => toggleActive(u)}>
                    <span className={`w-2.5 h-2.5 rounded-full inline-block ${u.is_active ? 'bg-green-400' : 'bg-gray-600'}`} />
                  </button>
                </td>
                <td className="py-2 px-2 text-gray-500 text-xs">
                  {u.last_login ? new Date(u.last_login).toLocaleString() : 'Never'}
                </td>
                <td className="py-2 px-2 text-right">
                  <button onClick={() => deleteUser(u)} className="text-gray-500 hover:text-red-400 transition">
                    <Trash2 size={16} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
