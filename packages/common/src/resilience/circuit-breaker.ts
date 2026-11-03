export type CircuitState = "closed" | "open" | "half-open";

export type CircuitBreakerOptions = {
  name: string;
  volumeThreshold?: number;
  errorThresholdPercentage?: number;
  resetTimeoutMs?: number;
  timeoutMs?: number;
};

export class CircuitOpenError extends Error {
  constructor(name: string) {
    super(`Circuit breaker "${name}" está abierto`);
    this.name = "CircuitOpenError";
  }
}

export class CircuitBreaker {
  private state: CircuitState = "closed";
  private failures = 0;
  private successes = 0;
  private openedAt = 0;
  private halfOpenInFlight = false;
  private readonly options: Required<CircuitBreakerOptions>;

  constructor(options: CircuitBreakerOptions) {
    this.options = {
      volumeThreshold: 5,
      errorThresholdPercentage: 50,
      resetTimeoutMs: 10_000,
      timeoutMs: 10_000,
      ...options,
    };
  }

  get name(): string {
    return this.options.name;
  }

  get currentState(): CircuitState {
    return this.state;
  }

  async execute<T>(
    operation: () => Promise<T>,
    fallback?: () => Promise<T>,
  ): Promise<T> {
    let state = this.currentState;
    if (
      state === "open" &&
      Date.now() - this.openedAt >= this.options.resetTimeoutMs
    ) {
      this.state = "half-open";
      state = "half-open";
    }
    if (state === "open" || (state === "half-open" && this.halfOpenInFlight)) {
      if (fallback) return fallback();
      throw new CircuitOpenError(this.options.name);
    }

    if (state === "half-open") this.halfOpenInFlight = true;
    try {
      const result = await this.withTimeout(operation());
      this.recordSuccess();
      return result;
    } catch (error) {
      this.recordFailure();
      if (fallback && this.currentState === "open") return fallback();
      throw error;
    } finally {
      this.halfOpenInFlight = false;
    }
  }

  private async withTimeout<T>(promise: Promise<T>): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        promise,
        new Promise<T>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`Circuit breaker "${this.name}" timeout`)),
            this.options.timeoutMs,
          );
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private recordSuccess(): void {
    if (this.currentState === "half-open") {
      this.state = "closed";
      this.failures = 0;
      this.successes = 0;
      return;
    }
    this.successes += 1;
    this.evaluate();
  }

  private recordFailure(): void {
    this.failures += 1;
    if (this.state === "half-open" || this.shouldOpen()) {
      this.state = "open";
      this.openedAt = Date.now();
    }
  }

  private shouldOpen(): boolean {
    const total = this.failures + this.successes;
    return (
      total >= this.options.volumeThreshold &&
      (this.failures / total) * 100 >= this.options.errorThresholdPercentage
    );
  }

  private evaluate(): void {
    if (this.shouldOpen()) {
      this.state = "open";
      this.openedAt = Date.now();
    }
  }
}
