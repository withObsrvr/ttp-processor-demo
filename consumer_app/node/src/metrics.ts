/**
 * ConsumerMetrics - Class for tracking metrics in the consumer application
 */
export class ConsumerMetrics {
  private _successCount: number = 0;
  private _errorCount: number = 0;
  private _totalProcessed: number = 0;
  private _totalEventsReceived: number = 0;
  private _lastError: Error | null = null;
  private _lastErrorTime: Date | null = null;
  private _startTime: Date = new Date();
  private _lastProcessedLedger: number = 0;
  private _processingLatency: number = 0; // in milliseconds
  private _latencyMeasurements: number = 0;

  /**
   * Record a successful event processing
   * @param ledgerSequence The ledger sequence number
   * @param eventCount Number of events received
   * @param processingTimeMs Processing time in milliseconds
   */
  public recordSuccess(ledgerSequence: number, eventCount: number = 1, processingTimeMs: number = 0): void {
    this._successCount++;
    this._totalProcessed++;
    this._totalEventsReceived += eventCount;
    this._lastProcessedLedger = Math.max(this._lastProcessedLedger, ledgerSequence);
    
    // Update average processing latency
    if (processingTimeMs > 0) {
      this._processingLatency = 
        (this._processingLatency * this._latencyMeasurements + processingTimeMs) / 
        (this._latencyMeasurements + 1);
      this._latencyMeasurements++;
    }
  }

  /**
   * Record an error during event processing
   * @param error The error that occurred
   */
  public recordError(error: Error): void {
    this._errorCount++;
    this._lastError = error;
    this._lastErrorTime = new Date();
  }

  /**
   * Get all metrics as an object
   */
  public getMetrics(): {
    successCount: number;
    errorCount: number;
    totalProcessed: number;
    lastProcessedLedger: number;
    averageProcessingTime: number;
  } {
    return {
      successCount: this._successCount,
      errorCount: this._errorCount,
      totalProcessed: this._totalProcessed,
      lastProcessedLedger: this._lastProcessedLedger,
      averageProcessingTime: this._processingLatency,
    };
  }

  /**
   * Get all metrics as an object (for JSON serialization)
   */
  public toJSON(): Record<string, any> {
    return {
      success_count: this._successCount,
      error_count: this._errorCount,
      total_processed: this._totalProcessed,
      total_events_received: this._totalEventsReceived,
      uptime_ms: Date.now() - this._startTime.getTime(),
      last_processed_ledger: this._lastProcessedLedger,
      processing_latency_ms: this._processingLatency,
      last_error: this._lastError ? this._lastError.message : null,
      last_error_time: this._lastErrorTime ? this._lastErrorTime.toISOString() : null
    };
  }

  // Getters
  get successCount(): number { return this._successCount; }
  get errorCount(): number { return this._errorCount; }
  get totalProcessed(): number { return this._totalProcessed; }
  get totalEventsReceived(): number { return this._totalEventsReceived; }
  get lastError(): Error | null { return this._lastError; }
  get lastErrorTime(): Date | null { return this._lastErrorTime; }
  get startTime(): Date { return this._startTime; }
  get lastProcessedLedger(): number { return this._lastProcessedLedger; }
  get processingLatency(): number { return this._processingLatency; }
  get uptime(): number { return Date.now() - this._startTime.getTime(); }
}

// Create a singleton instance
export const metrics = new ConsumerMetrics();