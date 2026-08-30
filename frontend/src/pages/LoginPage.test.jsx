import {
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react'
import {
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest'

const mocks = vi.hoisted(() => ({
  login: vi.fn(),
  navigate: vi.fn(),
}))

const routerState = vi.hoisted(() => ({
  pathname: '/login',
  search: '',
}))

vi.mock('../context/useAuth', () => ({
  useAuth: () => ({
    login: mocks.login,
  }),
}))

vi.mock('../router/useRouter', () => ({
  useRouter: () => ({
    pathname: routerState.pathname,
    search: routerState.search,
    navigate: mocks.navigate,
  }),
}))

import LoginPage from './LoginPage'

function submitLogin() {
  fireEvent.change(
    screen.getByLabelText('Email'),
    {
      target: {
        value: 'alice@example.com',
      },
    },
  )
  fireEvent.change(
    screen.getByLabelText('Password'),
    {
      target: {
        value: 'correct-password',
      },
    },
  )
  fireEvent.click(
    screen.getByRole(
      'button',
      { name: 'Sign in' },
    ),
  )
}

describe(
  'LoginPage post-authentication destination',
  () => {
    beforeEach(() => {
      vi.clearAllMocks()
      routerState.pathname = '/login'
      routerState.search = ''
      mocks.login.mockResolvedValue({
        id: 1,
      })
    })

    it('returns to a successful upload handoff investigation after re-authentication', async () => {
      routerState.pathname =
        '/investigation/42'

      render(<LoginPage />)
      submitLogin()

      await waitFor(() => {
        expect(
          mocks.navigate,
        ).toHaveBeenCalledWith(
          '/investigation/42',
          { replace: true },
        )
      })
      expect(
        mocks.login,
      ).toHaveBeenCalledOnce()
      expect(
        mocks.navigate,
      ).not.toHaveBeenCalledWith(
        '/new',
        expect.anything(),
      )
    })

    it('keeps the normal login destination as /new when no investigation handoff exists', async () => {
      render(<LoginPage />)
      submitLogin()

      await waitFor(() => {
        expect(
          mocks.navigate,
        ).toHaveBeenCalledWith(
          '/new',
          { replace: true },
        )
      })
      expect(
        mocks.login,
      ).toHaveBeenCalledOnce()
    })
  },
)
