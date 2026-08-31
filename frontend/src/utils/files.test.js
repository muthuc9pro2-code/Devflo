import { describe, expect, it } from 'vitest'
import {
  DIAGNOSTIC_ACCEPT,
  isSupportedDiagnosticFilename,
} from './files'

describe('diagnostic file support', () => {
  it('accepts CloudFront TSV diagnostics exposed by the backend', () => {
    expect(DIAGNOSTIC_ACCEPT.split(',')).toContain('.tsv')
    expect(isSupportedDiagnosticFilename('cloudfront.tsv')).toBe(true)
    expect(isSupportedDiagnosticFilename('cloudfront.tsv.1')).toBe(true)
  })

  it('still rejects source ZIPs as diagnostic artifacts', () => {
    expect(isSupportedDiagnosticFilename('repository.zip')).toBe(false)
  })
})
