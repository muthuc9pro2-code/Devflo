import { act, fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  uploadFiles: vi.fn(),
}))

vi.mock('../api/analysis', () => ({ uploadFiles: mocks.uploadFiles }))

vi.mock('../components/FileDropzone', () => ({
  default: ({ onAddFiles }) => (
    <button
      type="button"
      onClick={() => onAddFiles([{ file: new File(['x'], 'app.log'), displayPath: 'app.log' }])}
    >
      Add app.log
    </button>
  ),
}))

vi.mock('../components/SourceCodeSection', () => ({
  default: () => <div>Source code section stub</div>,
}))

import NewInvestigationPage from './NewInvestigationPage'

// Each click gets its OWN act() boundary: React needs to flush the file
// selection's state update before the "Start investigation" click's
// closure reads a fresh (non-empty) `files` value.
async function addFileAndStart() {
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: 'Add app.log' }))
  })
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: 'Start investigation' }))
  })
}

describe('NewInvestigationPage - beforeunload lifecycle during active upload', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('registers a beforeunload guard only while the upload is actually in flight', async () => {
    let resolveUpload
    mocks.uploadFiles.mockReturnValue(new Promise((resolve) => { resolveUpload = resolve }))
    const addSpy = vi.spyOn(window, 'addEventListener')
    const removeSpy = vi.spyOn(window, 'removeEventListener')

    render(<NewInvestigationPage onUploaded={() => {}} />)
    expect(addSpy).not.toHaveBeenCalledWith('beforeunload', expect.any(Function))

    await addFileAndStart()

    expect(mocks.uploadFiles).toHaveBeenCalled()
    expect(addSpy).toHaveBeenCalledWith('beforeunload', expect.any(Function))
    expect(removeSpy).not.toHaveBeenCalledWith('beforeunload', expect.any(Function))

    await act(async () => {
      resolveUpload({ id: 1 })
      await Promise.resolve()
    })

    expect(removeSpy).toHaveBeenCalledWith('beforeunload', expect.any(Function))

    addSpy.mockRestore()
    removeSpy.mockRestore()
  })

  it('removes the guard when the upload fails (settles), not just on success', async () => {
    let rejectUpload
    mocks.uploadFiles.mockReturnValue(new Promise((_resolve, reject) => { rejectUpload = reject }))
    const removeSpy = vi.spyOn(window, 'removeEventListener')

    render(<NewInvestigationPage onUploaded={() => {}} />)
    await addFileAndStart()

    expect(removeSpy).not.toHaveBeenCalledWith('beforeunload', expect.any(Function))

    await act(async () => {
      rejectUpload(new Error('network error'))
      await Promise.resolve().catch(() => {})
    })

    expect(removeSpy).toHaveBeenCalledWith('beforeunload', expect.any(Function))
    // The upload failure keeps the user on /new with their form/error
    // state intact - never treated as if the page navigated away.
    expect(screen.getByText('Devflo could not start this investigation. Please try again.')).toBeTruthy()
    removeSpy.mockRestore()
  })

  it('removes the guard on unmount even if the upload never resolves', async () => {
    mocks.uploadFiles.mockReturnValue(new Promise(() => {})) // never settles
    const removeSpy = vi.spyOn(window, 'removeEventListener')

    const { unmount } = render(<NewInvestigationPage onUploaded={() => {}} />)
    await addFileAndStart()

    expect(removeSpy).not.toHaveBeenCalledWith('beforeunload', expect.any(Function))

    unmount()

    expect(removeSpy).toHaveBeenCalledWith('beforeunload', expect.any(Function))
    removeSpy.mockRestore()
  })

  it('the registered handler triggers the native prompt without setting custom text', async () => {
    mocks.uploadFiles.mockReturnValue(new Promise(() => {}))

    render(<NewInvestigationPage onUploaded={() => {}} />)
    await addFileAndStart()

    const event = new Event('beforeunload', { cancelable: true })
    Object.defineProperty(event, 'returnValue', { value: '', writable: true })
    const preventDefaultSpy = vi.spyOn(event, 'preventDefault')

    window.dispatchEvent(event)

    expect(preventDefaultSpy).toHaveBeenCalled()
    expect(event.returnValue).toBe('')
  })
})
