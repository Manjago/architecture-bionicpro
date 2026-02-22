import React, { useState, useEffect } from 'react';

// BFF URL — все запросы проксируются через bionicpro-auth
const BFF_URL = process.env.REACT_APP_BFF_URL || 'http://localhost:8000';

const ReportPage: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);

  // Проверяем авторизацию при загрузке — дёргаем /health или /api
  // Если BFF вернёт 401 — пользователь не авторизован
  useEffect(() => {
    const checkAuth = async () => {
      try {
        const response = await fetch(`${BFF_URL}/api/reports/me`, {
          credentials: 'include', // Отправляет BIONIC_SESSION cookie
        });
        setAuthenticated(response.ok);
      } catch {
        setAuthenticated(false);
      }
    };
    checkAuth();
  }, []);

  const login = () => {
    // Просто редирект на BFF — он сгенерирует PKCE и перенаправит на Keycloak
    window.location.href = `${BFF_URL}/auth/login`;
  };

  const logout = async () => {
    // POST на BFF logout — он удалит сессию и перенаправит на Keycloak logout
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = `${BFF_URL}/auth/logout`;
    document.body.appendChild(form);
    form.submit();
  };

  const downloadReport = async () => {
    try {
      setLoading(true);
      setError(null);

      // Запрос к BFF — кука отправляется автоматически (credentials: 'include')
      // BFF проксирует к Report Service с Bearer Token
      const response = await fetch(`${BFF_URL}/api/reports/me`, {
        credentials: 'include',
      });

      if (response.status === 401) {
        // Сессия истекла — редирект на логин
        setAuthenticated(false);
        setError('Session expired. Please login again.');
        return;
      }

      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }

      const data = await response.json();
      // TODO: отобразить или скачать отчёт
      console.log('Report data:', data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  // Загрузка
  if (authenticated === null) {
    return <div>Loading...</div>;
  }

  // Не авторизован
  if (!authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={login}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  // Авторизован
  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">Usage Reports</h1>
          <button
            onClick={logout}
            className="px-3 py-1 text-sm bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
          >
            Logout
          </button>
        </div>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Generating Report...' : 'Download Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
