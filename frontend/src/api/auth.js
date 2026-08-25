import {
  finishLogout,
  markSessionEstablished,
  prepareForLogout,
  request,
} from './client'

export function register({ username, email, password }) {
  return request('/auth/register', {
    method: 'POST',
    body: JSON.stringify({ username, email, password }),
  })
}

export async function login({ email, password }) {
  const result = await request('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ email, password }),
  })
  markSessionEstablished()
  return result
}

export async function logout() {
  await prepareForLogout()
  try {
    return await request('/auth/logout', { method: 'POST' })
  } finally {
    finishLogout()
  }
}

export function getMe() {
  return request('/auth/me')
}

export async function verifyEmail(token) {
  const result = await request(`/auth/verify-email?token=${encodeURIComponent(token)}`)
  markSessionEstablished()
  return result
}

export function forgotPassword({ email }) {
  return request('/auth/forgot-password', {
    method: 'POST',
    body: JSON.stringify({ email }),
  })
}

export function resetPassword({ token, newPassword }) {
  return request('/auth/reset-password', {
    method: 'POST',
    body: JSON.stringify({ token, new_password: newPassword }),
  })
}
