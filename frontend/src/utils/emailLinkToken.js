export function readEmailLinkToken(search) {
  const fragment = new URLSearchParams(window.location.hash.slice(1)).get('token')
  return fragment || new URLSearchParams(search).get('token')
}

export function clearEmailLinkToken(cleanPath) {
  window.history.replaceState(window.history.state, '', cleanPath)
}
