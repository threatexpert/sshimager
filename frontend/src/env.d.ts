export {}

declare global {
  interface Window {
    go: {
      main: {
        GUIApp: {
          GetVersion(): Promise<string>
          OpenProjectPage(): Promise<void>
          Connect(request: unknown): Promise<unknown>
          Disconnect(): Promise<void>
          ScanDisk(request: unknown): Promise<unknown>
          SelectOutputPath(format: string, suggestedName: string): Promise<string>
          StartBackup(request: unknown): Promise<void>
          CancelBackup(): Promise<boolean>
          RevealOutput(path: string): Promise<void>
        }
      }
    }
    runtime: {
      EventsOn(eventName: string, callback: (...args: unknown[]) => void): () => void
    }
  }
}
