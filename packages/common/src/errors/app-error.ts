export class AppError extends Error {
  constructor(
    readonly statusCode: number,
    message: string,
    readonly isOperational = true,
    readonly code = `HTTP_${statusCode}`,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "AppError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}