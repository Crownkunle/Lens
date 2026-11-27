import type { FastifyInstance } from 'fastify'
import { resolveDecimalFormat, formatDecimal } from '../utils/decimals'
import { listCrashes } from '../ingester/restart'
import { listSourceFailures } from '../ingest/sourceFailures'
import { isBootComplete } from '../health'
import { price_requests_total } from '../metrics'
import { getCachedPrice, setCachedPrice } from '../redis'
import { getAggregatedPrice } from '../aggregator/vwap'
import { getBestRoute } from '../aggregator/bestRoute'
import { pgPool } from '../db'
import { config, getNetworkConfig, activeNetwork, type NetworkName } from '../config'
import { getPairsForNetwork } from '../pairsRegistry'
import { isAdminConfigured } from './auth'
import '../middleware/network'
import {
  statusResponseSchema,
  statusErrorResponseSchema,
  priceResponseSchema,
  routeResponseSchema,
  historyResponseSchema,
  depthResponseSchema,
  impactResponseSchema,
  installResponseValidation,
} from './schemas'
import { findOverlappingGaps } from '../backfill/gaps'
import { getDepth, getImpact } from '../pricing/depth'
import { getRetentionPath } from '../jobs/retentionStatus'
import { apiError } from '../errors'

function makePairKey(a: string, b: string): string {
  return [a, b].sort().join('/')
}

export const HISTORY_DEFAULT_LIMIT = 100
export const HISTORY_MAX_LIMIT = 1000

/**
 * Resolve a requested pair against the ones this network watches.
 *
 * The issuer is honoured when the caller supplies one. Asset codes are not
 * unique on Stellar, so the issuer is the only thing that identifies an asset.
 * A bare code still matches on code alone.
 */
export function findPair(assetA: string, assetB: string, network: NetworkName) {
  const parse = (a: string) => {
    if (a.toLowerCase() === 'native') return { code: 'XLM', issuer: null as string | null }
    const [code, issuer] = a.split(':')
    return { code: (code ?? '').toUpperCase(), issuer: issuer ?? null }
  }
  const qA = parse(assetA)
  const qB = parse(assetB)

  const matches = (
    q: { code: string; issuer: string | null },
    side: { code: string; issuer?: string | null },
  ) => {
    if (q.code !== side.code.toUpperCase()) return false
    if (!q.issuer || !side.issuer) return true
    return q.issuer === side.issuer
  }

  return getPairsForNetwork(network).find(
    p =>
      (matches(qA, p.assetA) && matches(qB, p.assetB)) ||
      (matches(qA, p.assetB) && matches(qB, p.assetA)),
  )
}

export async function registerRESTRoutes(app: FastifyInstance) {
  installResponseValidation(app)

  const INGEST_STALE_AFTER_SECONDS = 300

  app.get(
    '/status',
    {
      config: { public: true },
      schema: {
        response: {
          200: statusResponseSchema,
          503: statusErrorResponseSchema,
        },
      },
    },
    async (req, reply) => {
      const network = req.network ?? activeNetwork
      const mem = process.memoryUsage()
      const uptimeSeconds = Math.round(process.uptime())
      const memoryUsageMb = Math.round((mem.heapUsed / 1024 / 1024) * 100) / 100

      let result
      try {
        result = await pgPool.query(
          `SELECT last_ledger, last_processed_at
             FROM indexer_state
            WHERE network = $1
            ORDER BY updated_at DESC
            LIMIT 1`,
          [network]
        )
      } catch (err: unknown) {
        req.log.error({ err }, 'status: database query failed')
        return reply.status(503).send({
          ok: false,
          status: 'unhealthy',
          error: 'Database unavailable',
          pagingCondition: 'DATABASE_DOWN',
        })
      }

      const lastProcessedAt = result.rows[0]?.last_processed_at ?? null
      const lastIndexedLedger = result.rows[0]?.last_ledger ?? null
      const ingestLagSeconds = lastProcessedAt
        ? Math.max(0, Math.round((Date.now() - new Date(lastProcessedAt).getTime()) / 1000))
        : null
      const isStale = ingestLagSeconds !== null && ingestLagSeconds > INGEST_STALE_AFTER_SECONDS
      const processHealth = isBootComplete() ? ('healthy' as const) : ('degraded' as const)
      const dataHealth = isStale ? ('stale' as const) : ('healthy' as const)
      const overallStatus =
        processHealth === 'degraded' || isStale ? ('degraded' as const) : ('healthy' as const)

      return {
        ok: true,
        status: overallStatus,
        network,
        health: {
          process: processHealth,
          data: dataHealth,
        },
        process: {
          uptimeSeconds,
          memoryUsageMb,
          status: processHealth,
        },
        data: {
          lastIndexedLedger,
          lastProcessedAt,
          ingestLagSeconds,
          isStale,
        },
        watchedPairs: getNetworkConfig(network).pairs.map(p => p.pairKey),
        lastIndexedLedger,
        lastProcessedAt,
        ingestLagSeconds,
        retentionPath: getRetentionPath(),
        adminConfigured: isAdminConfigured(),
        crashes: listCrashes(network).map(c => ({
          name: c.name,
          network: c.network,
          message: c.message,
          at: c.at.toISOString(),
        })),
        failedSources: listSourceFailures(network).map(f => ({
          source: f.source,
          network: f.network,
          subject: f.subject,
          at: f.at.toISOString(),
        })),
      }
    }
  )

  app.get<{
    Params: { assetA: string; assetB: string }
    Querystring: {
      decimals?: string
      decimalFormat?: string
      staleThreshold?: string
      staleThresholdSeconds?: string
    }
  }>(
    '/price/:assetA/:assetB',
    { config: { scope: 'read:prices' }, schema: { response: { 200: priceResponseSchema } } },
    async (req, reply) => {
      price_requests_total.inc()
      const { assetA, assetB } = req.params
      const network = req.network ?? activeNetwork
      const pair = findPair(assetA, assetB, network)
      if (!pair) return reply.status(404).send(apiError('unwatched_pair', `Pair ${assetA}/${assetB} not watched on ${network}`))

      const rawThreshold = req.query?.staleThreshold ?? req.query?.staleThresholdSeconds
      let staleThreshold: number | undefined
      if (rawThreshold !== undefined) {
        staleThreshold = parseInt(rawThreshold, 10)
        if (isNaN(staleThreshold) || staleThreshold <= 0) {
          return reply.status(400).send(apiError('invalid_parameter', 'staleThreshold must be a positive integer'))
        }
      }

      const decimalFormat = resolveDecimalFormat(req)
      if (decimalFormat === 'string') {
        reply.header('x-decimal-format', 'string')
      }

      const cacheParts = [network, pair.pairKey]
      if (decimalFormat === 'string') cacheParts.push('decimals_string')
      if (staleThreshold !== undefined) cacheParts.push(`stale_${staleThreshold}`)
      const cacheKey = cacheParts.join(':')

      const cached = await getCachedPrice(cacheKey)
      if (cached) {
        try {
          reply.header('X-Cache', 'HIT')
          return JSON.parse(cached)
        } catch { /* fall through */ }
      }

      const agg = await getAggregatedPrice(pair.pairKey, network, { decimalFormat, staleThresholdSeconds: staleThreshold })
      const route = await getBestRoute(pair.assetA, pair.assetB, pair.pairKey, 1000, network)
      const result = {
        assetA: pair.assetA.code,
        assetB: pair.assetB.code,
        pairKey: pair.pairKey,
        network,
        ...agg,
        bestRoute: route.route,
        lastUpdated: new Date().toISOString(),
      }

      await setCachedPrice(cacheKey, result, config.cache.priceTtl)
      reply.header('X-Cache', 'MISS')
      return result
    }
  )

  app.get<{
    Params: { assetA: string; assetB: string }
    Querystring: { amount?: string; network?: string }
  }>(
    '/price/:assetA/:assetB/route',
    { config: { scope: 'read:prices' }, schema: { response: { 200: routeResponseSchema } } },
    async (req, reply) => {
      const { assetA, assetB } = req.params
      const amount = parseFloat(req.query.amount ?? '1000')
      if (isNaN(amount) || amount <= 0) return reply.status(400).send(apiError('invalid_parameter', 'amount must be a positive number'))
      const network = req.network ?? activeNetwork
      const pair = findPair(assetA, assetB, network)
      if (!pair) return reply.status(404).send(apiError('unwatched_pair', `Pair ${assetA}/${assetB} not watched on ${network}`))

      const decimalFormat = resolveDecimalFormat(req)
      const routeInfo = await getBestRoute(pair.assetA, pair.assetB, pair.pairKey, amount, network)
      if (decimalFormat === 'string') {
        reply.header('x-decimal-format', 'string')
      }
      return routeInfo.route
    }
  )
}
