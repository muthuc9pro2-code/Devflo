import { act, fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  uploadFiles: vi.fn(),
}))

vi.mock('../api/analysis', () => ({ uploadFiles: mocks.uploadFiles }))

vi.mock('../components/FileDropzone', () => ({
  default: ({ onAddFiles }) => (
    <>
      <button
        type="button"
        onClick={() => onAddFiles([{ file: new File(['x'], 'app.log'), displayPath: 'app.log' }])}
      >
        Add app.log
      </button>
      <button
        type="button"
        onClick={() => onAddFiles([{
          file: new File(
            ['#Version: 1.0\n#Fields: date time x-edge-location sc-status\n'],
            'cloudfront.tsv',
            { type: 'text/tab-separated-values' },
          ),
          displayPath: 'cloudfront.tsv',
        }])}
      >
        Add cloudfront.tsv
      </button>
    </>
  ),
}))

vi.mock('../components/SourceCodeSection', () => ({
  default: () => <div>Source code section stub</div>,
}))

import NewInvestigationPage from './NewInvestigationPage'
import { ApiError } from '../api/client'

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

describe('NewInvestigationPage - backend admission errors', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('shows the active-investigation quota and releases the upload lock', async () => {
    const onUploadingChange = vi.fn()
    mocks.uploadFiles.mockRejectedValue(
      new ApiError(
        'You already have 3 active investigations. Wait for one to finish or cancel one before starting another.',
        429,
      ),
    )

    render(
      <NewInvestigationPage
        onUploaded={() => {}}
        onUploadingChange={onUploadingChange}
      />,
    )
    await addFileAndStart()

    expect(
      screen.getByText(
        'You already have 3 active investigations. Wait for one to finish or cancel one before starting another.',
      ),
    ).toBeTruthy()
    expect(onUploadingChange).toHaveBeenNthCalledWith(1, true)
    expect(onUploadingChange).toHaveBeenLastCalledWith(false)
    expect(
      screen.getByRole('button', { name: 'Start investigation' }).disabled,
    ).toBe(false)
  })
})

describe('NewInvestigationPage - supported diagnostic formats', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('accepts a CloudFront TSV artifact and submits it unchanged', async () => {
    mocks.uploadFiles.mockResolvedValue({ id: 41 })

    render(<NewInvestigationPage onUploaded={() => {}} />)

    await act(async () => {
      fireEvent.click(
        screen.getByRole('button', { name: 'Add cloudfront.tsv' }),
      )
    })
    await act(async () => {
      fireEvent.click(
        screen.getByRole('button', { name: 'Start investigation' }),
      )
    })

    expect(mocks.uploadFiles).toHaveBeenCalledTimes(1)
    const [files, source] = mocks.uploadFiles.mock.calls[0]
    expect(files).toHaveLength(1)
    expect(files[0].name).toBe('cloudfront.tsv')
    expect(source).toEqual({
      githubUrl: '',
      sourceZip: null,
    })
  })
})
