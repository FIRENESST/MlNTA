// Type definitions for @minta/beep
/// <reference types="node" />
import { EventEmitter } from 'events';
import type { IncomingMessage as HttpIncomingMessage, ServerResponse as HttpServerResponse } from 'http';

export const RPC_PROFILE: 'urn:minta:beep:rpc:1';
export const TLS_PROFILE: 'http://iana.org/beep/TLS';

export type HeaderPairs = Array<[string, string]>;
export type HeadersInit = HeaderPairs | Record<string, string | number> | Map<string, string>;
export type BodyInit = Buffer | Uint8Array | string | null | undefined;

export interface BeepConfig {
  recvWindow?: number;
  maxFrameSize?: number;
  maxMessageSize?: number;
  greetingTimeout?: number;
  closeTimeout?: number;
}

export interface TlsClientOptions {
  servername: string;
  ca?: string | Buffer;
  caFile?: string;
  tlsOptions?: import('tls').ConnectionOptions;
}

export interface TlsServerOptions {
  cert?: string | Buffer;
  key?: string | Buffer;
  certFile?: string;
  keyFile?: string;
}

export class BeepError extends Error {}
export class SessionClosed extends BeepError {}
export class ProtocolError extends Error {}
export class Refused extends BeepError {
  code: number;
  beepMessage: string;
}
export class RemoteError extends BeepError {
  code: number;
  beepMessage: string;
  readonly httpStatus: number;
}
export class RpcTimeout extends BeepError {}
export class StreamAborted extends BeepError {}
export class RpcFailure extends BeepError {
  constructor(code: number, message: string);
  code: number;
  beepMessage: string;
}

export function beepCodeToHttp(code: number): number;

export class Response {
  status: number;
  headers: HeaderPairs;
  body: Buffer;
  header(name: string): string | undefined;
  text(): string;
  json<T = any>(): T;
}

export class StreamingResponse implements AsyncIterable<Buffer> {
  status: number;
  headers: HeaderPairs;
  header(name: string): string | undefined;
  [Symbol.asyncIterator](): AsyncIterator<Buffer>;
  read(): Promise<Buffer>;
  close(): void;
}

export interface CallOptions {
  headers?: HeadersInit;
  body?: BodyInit;
  json?: unknown;
  /** milliseconds; null disables */
  timeout?: number | null;
}

export interface RpcClientOptions {
  sessions?: number;
  maxChannels?: number;
  timeout?: number | null;
  connectTimeout?: number;
  tls?: TlsClientOptions;
  config?: BeepConfig;
  profile?: string;
}

export class RpcClient {
  constructor(host?: string, port?: number, opts?: RpcClientOptions);
  connect(): Promise<void>;
  call(method: string, target: string, opts?: CallOptions): Promise<Response>;
  stream(method: string, target: string, opts?: CallOptions): Promise<StreamingResponse>;
  get(target: string, opts?: CallOptions): Promise<Response>;
  post(target: string, body?: BodyInit, opts?: CallOptions): Promise<Response>;
  raw(profile: string, payload: BodyInit): Promise<Reply>;
  status(): number[];
  close(): Promise<void>;
}

export class Request {
  method: string;
  target: string;
  headers: HeaderPairs;
  body: Buffer;
  channel: number;
  readonly path: string;
  readonly id: string | undefined;
  readonly timeout: number | undefined;
  header(name: string): string | undefined;
  text(): string;
  json<T = any>(): T;
}

export interface HandlerResult {
  status?: number;
  headers?: HeadersInit;
  /** Buffer/string → single RPY; AsyncIterable → streamed as ANS…NUL */
  body?: BodyInit | AsyncIterable<Buffer | string>;
}
export type Handler = (req: Request) => HandlerResult | BodyInit | Promise<HandlerResult | BodyInit>;

export interface RpcServerOptions {
  config?: BeepConfig;
  tls?: TlsServerOptions;
  profiles?: Record<string, ProfileHandler>;
}

export class RpcServer {
  constructor(handler: Handler, opts?: RpcServerOptions);
  listen(port?: number, host?: string): Promise<this>;
  readonly port: number;
  close(): Promise<void>;
}

export function rpcProfile(handler: Handler): ProfileHandler;
export function httpHandler(listener: (req: HttpIncomingMessage, res: HttpServerResponse) => void, opts?: { streamAfterMs?: number }): Handler;

// ---- low-level session API ----

export type Reply = { kind: 'RPY' | 'ERR'; payload: Buffer } | { kind: 'ANS'; payload: Array<[number, Buffer]> };
export type ReplyEvent =
  | { type: 'RPY' | 'ERR'; data: Buffer; last: boolean }
  | { type: 'ANS'; ansno: number; data: Buffer; last: boolean }
  | { type: 'NUL' };

export interface ReplyStream {
  readonly done: boolean;
  next(): Promise<ReplyEvent | null>;
  collect(max?: number): Promise<Reply>;
  discard(): void;
}

export interface IncomingMessage {
  readonly channel: number;
  readonly msgno: number;
  read(): Promise<Buffer | null>;
  collect(max?: number): Promise<Buffer>;
  discard(): void;
}

export interface Responder {
  readonly channel: number;
  readonly msgno: number;
  rpy(payload: Buffer): Promise<void>;
  err(payload: Buffer): Promise<void>;
  error(code: number, message: string): Promise<void>;
  ans(payload: Buffer): Promise<number>;
  nul(): Promise<void>;
}

export interface ProfileHandler {
  onStart?(session: Session, channel: number, init: string | null): Promise<string | null | void> | string | null | void;
  onMessage(session: Session, msg: IncomingMessage, reply: Responder): Promise<void>;
}

export class Channel {
  readonly session: Session;
  readonly number: number;
  readonly profile: string;
  readonly isOpen: boolean;
  readonly pending: number;
  send(payload: Buffer): Promise<ReplyStream>;
  request(payload: Buffer): Promise<Reply>;
  close(code?: number): Promise<void>;
}

export class Session extends EventEmitter {
  static connect(host: string, port: number, opts?: { config?: BeepConfig; profiles?: Record<string, ProfileHandler>; tls?: TlsClientOptions }): Promise<Session>;
  static establish(socket: import('net').Socket | import('tls').TLSSocket, initiator: boolean, config?: BeepConfig, profiles?: Record<string, ProfileHandler>): Promise<Session>;
  readonly config: Required<BeepConfig>;
  readonly initiator: boolean;
  readonly peerProfiles: string[];
  readonly closed: boolean;
  readonly channelCount: number;
  waitClosed(): Promise<Error>;
  startChannel(uri: string | Array<{ uri: string; data?: string | null }>, serverName?: string): Promise<Channel>;
  startChannelWith(uri: string | Array<{ uri: string; data?: string | null }>, serverName?: string): Promise<{ channel: Channel; init: string | null }>;
  close(): Promise<void>;
  abort(): void;
}

export class Listener extends EventEmitter {
  constructor(profiles: Record<string, ProfileHandler>, opts?: { config?: BeepConfig; tls?: TlsServerOptions });
  listen(port?: number, host?: string): Promise<this>;
  readonly port: number;
  close(): Promise<void>;
}

export const wire: {
  encodeEntity(headers: HeaderPairs, body?: BodyInit): Buffer;
  parseEntity(payload: Buffer): { headers: HeaderPairs; body: Buffer };
  getHeader(headers: HeaderPairs, name: string): string | undefined;
  [k: string]: any;
};
