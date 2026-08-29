import { useContext } from 'react'
import { CriticalOperationContext } from './critical-operation-context'

export function useCriticalOperation() {
  const ctx = useContext(CriticalOperationContext)
  if (!ctx) {
    throw new Error('useCriticalOperation must be used within CriticalOperationProvider')
  }
  return ctx
}
