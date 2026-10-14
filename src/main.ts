import 'reflect-metadata';
import { join } from 'node:path';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { ArgumentsHost, Body, CanActivate, Catch, Controller, ExceptionFilter,
  Get, Header, Headers, HttpException, HttpStatus, Injectable, Module, Param, Post, Query,
  Res, UseGuards } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ApiBody, ApiExcludeEndpoint, ApiHeader, ApiOperation, ApiQuery, ApiResponse,
  ApiTags, DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import type { Request, Response, NextFunction } from 'express';
import { db } from './db';
import { DomainError } from './domain';
import type { MoneyProps } from './domain';
import { WagerService } from './service';
import type { WagerInput } from './service';
import { log } from './metrics';
import { Workers, prometheus, readiness } from './transport';

// TODO(security): Replace with OIDC and rate limiting before any public deployment.
// Authentication is explicitly optional in section 2 of this local challenge.
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
      level: 'error', event: 'request_failed', code: error?.code ?? 'UNEXPECTED_ERROR' }));
    response.status(status).json({ code, message: code });
  }
}

@Controller()
@UseGuards(NoopAuthGuard)
class ApiController {
  constructor(private readonly wagers: WagerService) {}

  @ApiTags('Wallets')
  @ApiOperation({ summary: 'Cria uma wallet com saldo inicial opcional' })
  @ApiBody({ schema: { type: 'object', required: ['playerId', 'initialBalance'], properties: {
    playerId: { type: 'string', format: 'uuid', example: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1' },
    initialBalance: { type: 'object', required: ['amount', 'currency'], properties: {
      amount: { type: 'string', example: '100.00' }, currency: { type: 'string', example: 'BRL' },
    } },
  } } })
  @ApiResponse({ status: 201, description: 'Wallet criada; abertura positiva gera ledger e outbox.',
    schema: { type: 'object', example: { id: '0192f291-27dd-7d3f-8071-5f8685deef37',
      playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
      balance: { amount: '100.00', currency: 'BRL' }, version: 1 } } })
  @ApiResponse({ status: 409, description: 'Já existe wallet para o jogador e a moeda.' })
  @Post('wallets')
  async createWallet(@Body() body: { playerId: string; initialBalance: MoneyProps },
    @Res({ passthrough: true }) response: Response) {
    const result = await this.wagers.createWallet(body?.playerId, body?.initialBalance);
    response.status(201);
    return result;
  }
  @ApiTags('Wallets')
  @ApiOperation({ summary: 'Consulta saldo e versão da wallet' })
  @Get('wallets/:walletId')
  wallet(@Param('walletId') id: string) { return this.wagers.wallet(id); }
  @ApiTags('Wallets')
  @ApiOperation({ summary: 'Consulta o ledger com paginação por cursor' })
  @ApiQuery({ name: 'cursor', required: false, description: 'Cursor opaco devolvido pela página anterior.' })
  @ApiQuery({ name: 'limit', required: false, description: 'De 1 a 100; padrão 50.', example: 50 })
  @Get('wallets/:walletId/ledger')
  ledger(@Param('walletId') id: string, @Query('cursor') cursor?: string,
    @Query('limit') limit?: string) { return this.wagers.ledger(id, cursor, limit); }
  @ApiTags('Wallets')
  @ApiOperation({ summary: 'Reconcilia saldo materializado com o ledger' })
  @Post('wallets/:walletId/reconciliation')
  reconcile(@Param('walletId') id: string) { return this.wagers.reconcile(id); }

  @ApiTags('Wagering')
  @ApiOperation({ summary: 'Processa uma operação de aposta com idempotência persistente' })
  @ApiHeader({ name: 'Idempotency-Key', required: true, description: 'Reutilize a chave somente com o mesmo payload.' })
  @ApiBody({ schema: { type: 'object', required: [
    'providerId', 'externalTransactionId', 'playerId', 'walletId', 'roundId', 'gameId', 'kind', 'money',
  ], properties: {
    providerId: { type: 'string', example: 'provider-a' },
    externalTransactionId: { type: 'string', example: 'transaction-123' },
    playerId: { type: 'string', format: 'uuid', example: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1' },
    walletId: { type: 'string', format: 'uuid', example: '0192f291-27dd-7d3f-8071-5f8685deef37' },
    roundId: { type: 'string', example: 'round-987' }, gameId: { type: 'string', example: 'fortune-chimp' },
    kind: { type: 'string', enum: ['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'], example: 'BET' },
    money: { type: 'object', required: ['amount', 'currency'], properties: {
      amount: { type: 'string', example: '25.00' }, currency: { type: 'string', example: 'BRL' },
    } },
    referenceExternalTransactionId: { type: 'string', description: 'Obrigatória para REFUND e ROLLBACK.' },
  } } })
  @ApiResponse({ status: 200, description: 'Processada ou resposta idempotente armazenada.',
    schema: { type: 'object', example: { transactionId: '0192f298-345e-7e38-af88-e43f851a819d',
      status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false } } })
  @ApiResponse({ status: 202, description: 'Referência ainda pendente.' })
  @ApiResponse({ status: 409, description: 'Chave de idempotência ou identidade externa em conflito.' })
  @ApiResponse({ status: 422, description: 'Rejeição por regra de negócio, com failureCode.' })
  @ApiResponse({ status: 503, description: 'Falha transitória de infraestrutura.' })
  @Post('wagering/transactions')
  async submit(@Body() body: WagerInput, @Headers('idempotency-key') key: string,
    @Res({ passthrough: true }) response: Response) {
    const result = await this.wagers.submit(body, key);
    log('wager_http_completed', { correlationId: result.transactionId,
      transactionId: result.transactionId, walletId: body.walletId, providerId: body.providerId,
      status: result.status, idempotentReplay: result.idempotentReplay });
    response.status(result.status === 'PENDING_REFERENCE' ? 202 :
      result.status === 'REJECTED' ? 422 : 200);
    return result;
  }
  @ApiTags('Wagering')
  @ApiOperation({ summary: 'Consulta uma transação pelo ID interno' })
  @Get('wagering/transactions/:transactionId')
  transaction(@Param('transactionId') id: string) { return this.wagers.transaction(id); }
  @ApiTags('Wagering')
  @ApiOperation({ summary: 'Consulta uma transação pela identidade do provedor' })
  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  providerTransaction(@Param('providerId') providerId: string,
    @Param('externalTransactionId') externalId: string) {
    return this.wagers.providerTransaction(providerId, externalId);
  }
}

@Controller()
class OperationsController {
  @ApiTags('Operação')
  @ApiOperation({ summary: 'Liveness do processo' })
  @Get('health/live') live() { return { status: 'alive' }; }
  @ApiTags('Operação')
  @ApiOperation({ summary: 'Readiness de PostgreSQL e SQS' })
  @Get('health/ready')
  async ready(@Res({ passthrough: true }) response: Response) {
    const ready = await readiness();
    response.status(ready ? 200 : 503);
    return { status: ready ? 'ready' : 'unavailable' };
  }
  @ApiTags('Operação')
  @ApiOperation({ summary: 'Métricas Prometheus por instância' })
  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4')
  metrics() { return prometheus(); }

  @ApiExcludeEndpoint()
  @Get('demo')
  demo(@Res() response: Response) {
    response.sendFile(join(process.cwd(), 'public', 'demo.html'));
  }
}

@Module({ controllers: [ApiController, OperationsController],
  providers: [NoopAuthGuard, WagerService, Workers] })
class AppModule {}

await db.initialize();
const app = await NestFactory.create(AppModule, { logger: ['error', 'warn'] });
const demoHtml = readFileSync(join(process.cwd(), 'public', 'demo.html'), 'utf8');
const inlineHashes = (tag: string) => [...demoHtml.matchAll(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'g'))]
  .map(match => `'sha256-${createHash('sha256').update(match[1]).digest('base64')}'`).join(' ');
app.use((request: Request, response: Response, next: NextFunction) => {
  response.setHeader('X-Content-Type-Options', 'nosniff');
  response.setHeader('X-Frame-Options', 'DENY');
  response.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  response.setHeader('Cache-Control', 'no-store');
  if (request.path === '/demo') response.setHeader('Content-Security-Policy',
    `default-src 'none'; script-src ${inlineHashes('script')}; style-src ${inlineHashes('style')}; ` +
    "connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'none'");
  next();
});
app.useGlobalFilters(new ApiErrorFilter());
app.enableShutdownHooks();
const docs = new DocumentBuilder()
  .setTitle('Distributed Wagering Processor')
  .setDescription('API interativa para demonstrar wallets, apostas, ledger e reconciliação.')
  .setVersion('1.0.0')
  .build();
SwaggerModule.setup('api', app, () => SwaggerModule.createDocument(app, docs));
await app.listen(Number(process.env.PORT ?? 3000), process.env.HOST ?? '127.0.0.1');
