import 'reflect-metadata';
import { ArgumentsHost, Body, CanActivate, Catch, Controller, ExceptionFilter,
  Get, Header, Headers, HttpException, HttpStatus, Injectable, Module, Param, Post, Query,
  Res, UseGuards } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { Response } from 'express';
import { db } from './db';
import { DomainError, MoneyProps } from './domain';
import { WagerInput, WagerService } from './service';
import { metrics } from './metrics';
import { Workers, prometheus, readiness } from './transport';

// Replace with an OIDC guard. The expected identity claims are providerId/playerId.
@Injectable()
class NoopAuthGuard implements CanActivate {
  canActivate(): boolean { return true; }
}

@Catch()
class ApiErrorFilter implements ExceptionFilter {
  catch(error: any, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();
    if (error instanceof HttpException) {
      response.status(error.getStatus()).json({ code: 'HTTP_ERROR', message: error.message });
      return;
    }
    const code = error instanceof DomainError ? error.code :
      error?.code === '23505' ? 'CONFLICT' : 'INFRASTRUCTURE_UNAVAILABLE';
    const bad = ['INVALID_PAYLOAD', 'INVALID_MONEY', 'INVALID_AMOUNT', 'INVALID_REFERENCE',
      'MISSING_IDEMPOTENCY_KEY', 'INVALID_CURSOR', 'OPENING_INTERNAL_ONLY'].includes(code);
    const conflict = ['IDEMPOTENCY_CONFLICT', 'INBOX_MESSAGE_CONFLICT',
      'WALLET_ALREADY_EXISTS', 'CONFLICT'].includes(code);
    const missing = ['WALLET_NOT_FOUND', 'TRANSACTION_NOT_FOUND'].includes(code);
    const business = error instanceof DomainError && !bad && !conflict && !missing;
    const status = bad ? HttpStatus.BAD_REQUEST : conflict ? HttpStatus.CONFLICT :
      missing ? HttpStatus.NOT_FOUND : business ? HttpStatus.UNPROCESSABLE_ENTITY :
      HttpStatus.SERVICE_UNAVAILABLE;
    if (status === HttpStatus.SERVICE_UNAVAILABLE) console.error(JSON.stringify({
      level: 'error', event: 'request_failed', code: error?.code, message: String(error) }));
    response.status(status).json({ code, message: code });
  }
}

@Controller()
@UseGuards(NoopAuthGuard)
class ApiController {
  constructor(private readonly wagers: WagerService) {}

  @Post('wallets')
  async createWallet(@Body() body: { playerId: string; initialBalance: MoneyProps },
    @Res({ passthrough: true }) response: Response) {
    const result = await this.wagers.createWallet(body?.playerId, body?.initialBalance);
    response.status(201);
    return result;
  }
  @Get('wallets/:walletId')
  wallet(@Param('walletId') id: string) { return this.wagers.wallet(id); }
  @Get('wallets/:walletId/ledger')
  ledger(@Param('walletId') id: string, @Query('cursor') cursor?: string,
    @Query('limit') limit?: string) { return this.wagers.ledger(id, cursor, limit); }
  @Post('wallets/:walletId/reconciliation')
  reconcile(@Param('walletId') id: string) { return this.wagers.reconcile(id); }

  @Post('wagering/transactions')
  async submit(@Body() body: WagerInput, @Headers('idempotency-key') key: string,
    @Res({ passthrough: true }) response: Response) {
    const result = await this.wagers.submit(body, key);
    if (result.idempotentReplay) metrics.duplicates++;
    else metrics.transactions[result.status as keyof typeof metrics.transactions]++;
    response.status(result.status === 'PENDING_REFERENCE' ? 202 :
      result.status === 'REJECTED' ? 422 : 200);
    return result;
  }
  @Get('wagering/transactions/:transactionId')
  transaction(@Param('transactionId') id: string) { return this.wagers.transaction(id); }
  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  providerTransaction(@Param('providerId') providerId: string,
    @Param('externalTransactionId') externalId: string) {
    return this.wagers.providerTransaction(providerId, externalId);
  }
}

@Controller()
class OperationsController {
  @Get('health/live') live() { return { status: 'alive' }; }
  @Get('health/ready')
  async ready(@Res({ passthrough: true }) response: Response) {
    const ready = await readiness();
    response.status(ready ? 200 : 503);
    return { status: ready ? 'ready' : 'unavailable' };
  }
  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4')
  metrics() { return prometheus(); }
}

@Module({ controllers: [ApiController, OperationsController],
  providers: [NoopAuthGuard, WagerService, Workers] })
class AppModule {}

await db.initialize();
const app = await NestFactory.create(AppModule, { logger: ['error', 'warn'] });
app.useGlobalFilters(new ApiErrorFilter());
app.enableShutdownHooks();
await app.listen(Number(process.env.PORT ?? 3000), '0.0.0.0');
