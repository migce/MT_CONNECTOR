import { useEffect, useState } from 'react'
import api from '../../api/client'
import { Plus, Trash2, Check, X } from 'lucide-react'

interface Account {
  id: number
  label: string
  description?: string
  mt5_login: number
  mt5_server: string
  enabled: boolean
}

export default function AdminAccounts() {
  const [accounts, setAccounts] = useState<Account[]>([])
  const [showCreate, setShowCreate] = useState(false)
  const [form, setForm] = useState({
    label: '', mt5_login: '', mt5_password: '', mt5_server: '', description: '', enabled: true,
  })
  const [error, setError] = useState('')

  const load = () => api.get('/trading/accounts').then((r) => setAccounts(r.data))

  useEffect(() => { load() }, [])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    try {
      await api.post('/trading/accounts', {
        ...form,
        mt5_login: Number(form.mt5_login),
      })
      setForm({ label: '', mt5_login: '', mt5_password: '', mt5_server: '', description: '', enabled: true })
      setShowCreate(false)
      load()
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to create account')
    }
  }

  const toggleEnabled = async (acc: Account) => {
    await api.patch(`/trading/accounts/${acc.id}`, { enabled: !acc.enabled })
    load()
  }

  const deleteAccount = async (acc: Account) => {
    if (!confirm(`Delete account "${acc.label}"?`)) return
    await api.delete(`/trading/accounts/${acc.id}`)
    load()
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-gray-100">MT5 Accounts</h2>
        <button
          onClick={() => setShowCreate(!showCreate)}
          className="flex items-center gap-2 px-4 py-2 bg-brand-600 hover:bg-brand-500 rounded-lg text-white text-sm font-medium transition"
        >
          <Plus size={16} /> New account
        </button>
      </div>

      {showCreate && (
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <form onSubmit={handleCreate} className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <input
              value={form.label}
              onChange={(e) => setForm({ ...form, label: e.target.value })}
              placeholder="Label"
              required
              className="px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
            />
            <input
              value={form.mt5_login}
              onChange={(e) => setForm({ ...form, mt5_login: e.target.value })}
              placeholder="MT5 Login"
              type="number"
              required
              className="px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
            />
            <input
              value={form.mt5_password}
              onChange={(e) => setForm({ ...form, mt5_password: e.target.value })}
              placeholder="MT5 Password"
              type="password"
              required
              className="px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
            />
            <input
              value={form.mt5_server}
              onChange={(e) => setForm({ ...form, mt5_server: e.target.value })}
              placeholder="MT5 Server"
              required
              className="px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500"
            />
            <textarea
              value={form.description}
              onChange={(e) => setForm({ ...form, description: e.target.value })}
              placeholder="Description (optional, up to 255 chars)"
              maxLength={255}
              rows={2}
              className="px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-sm focus:outline-none focus:border-brand-500 resize-none md:col-span-2"
            />

            <div className="flex gap-2">
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

      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-500 border-b border-gray-800">
              <th className="text-left py-2 px-2">ID</th>
              <th className="text-left py-2 px-2">Label</th>
              <th className="text-left py-2 px-2">Description</th>
              <th className="text-left py-2 px-2">Login</th>
              <th className="text-left py-2 px-2">Server</th>
              <th className="text-center py-2 px-2">Enabled</th>
              <th className="text-right py-2 px-2">Actions</th>
            </tr>
          </thead>
          <tbody>
            {accounts.map((acc) => (
              <tr key={acc.id} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                <td className="py-2 px-2 text-gray-500">{acc.id}</td>
                <td className="py-2 px-2 text-gray-200 font-medium">{acc.label}</td>
                <td className="py-2 px-2 text-gray-400 truncate max-w-[220px]" title={acc.description ?? ''}>{acc.description ?? <span className="text-gray-600 italic">—</span>}</td>
                <td className="py-2 px-2 text-gray-400">{acc.mt5_login}</td>
                <td className="py-2 px-2 text-gray-400 truncate max-w-[200px]">{acc.mt5_server}</td>
                <td className="py-2 px-2 text-center">
                  <button onClick={() => toggleEnabled(acc)}>
                    <span className={`w-2.5 h-2.5 rounded-full inline-block ${acc.enabled ? 'bg-green-400' : 'bg-gray-600'}`} />
                  </button>
                </td>
                <td className="py-2 px-2 text-right">
                  <button onClick={() => deleteAccount(acc)} className="text-gray-500 hover:text-red-400 transition">
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
