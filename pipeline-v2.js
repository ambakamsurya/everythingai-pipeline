// ============================================================
// ∞AI — PIPELINE v5
// Smart filtering + detailed per-run logging
//
// KEY CHANGES FROM v4:
// 1. Tier 1 + Tier 2 sources SKIP keyword filter entirely
//    (they're AI-specific feeds — everything is relevant)
// 2. Tier 3 sources use keyword filter (mixed content)
// 3. Every skipped article logged with exact reason
// 4. Log file written after each run
// 5. Supabase pipeline_runs updated with full breakdown
// ============================================================

import Anthropic from '@anthropic-ai/sdk'
import { createClient } from '@supabase/supabase-js'
import Parser from 'rss-parser'
import * as dotenv from 'dotenv'
import fs from 'fs'
import path from 'path'
dotenv.config()

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
const supabase  = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY)
const parser    = new Parser({ timeout: 10000 })

// ── SOURCES ───────────────────────────────────────────────
const SOURCES = [
  // Tier 1 — Official lab blogs — ALL content is AI, skip keyword filter
  { name: 'OpenAI Blog',        url: 'https://openai.com/blog/rss.xml',                                tier: 1 },
  { name: 'Google DeepMind',    url: 'https://deepmind.google/blog/rss.xml',                           tier: 1 },
  { name: 'Google AI Blog',     url: 'https://blog.google/technology/ai/rss/',                         tier: 1 },
  { name: 'Hugging Face Blog',  url: 'https://huggingface.co/blog/feed.xml',                           tier: 1 },
  { name: 'GitHub Blog',        url: 'https://github.blog/feed/',                                      tier: 1 },
  { name: 'Microsoft Research', url: 'https://www.microsoft.com/en-us/research/feed/',                 tier: 1 },

  // Tier 2 — AI-specific journalism — ALL content is AI, skip keyword filter
  { name: 'TechCrunch AI',      url: 'https://techcrunch.com/category/artificial-intelligence/feed/',  tier: 2 },
  { name: 'The Verge',          url: 'https://www.theverge.com/rss/index.xml',                         tier: 2 },
  { name: 'Ars Technica AI',    url: 'https://arstechnica.com/ai/feed/',                               tier: 2 },
  { name: 'VentureBeat AI',     url: 'https://venturebeat.com/category/ai/feed/',                      tier: 2 },
  { name: 'MIT Tech Review',    url: 'https://www.technologyreview.com/feed/',                         tier: 2 },
  { name: 'Wired AI',           url: 'https://www.wired.com/feed/category/artificial-intelligence/rss', tier: 2 },
  { name: 'ZDNet AI',           url: 'https://www.zdnet.com/topic/artificial-intelligence/rss.xml',    tier: 2 },
  { name: 'AI News',            url: 'https://www.artificialintelligence-news.com/feed/rss/',           tier: 2 },
  { name: 'SiliconAngle AI',    url: 'https://siliconangle.com/category/ai/feed/',                     tier: 2 },
  { name: 'The Decoder',        url: 'https://the-decoder.com/feed/',                                  tier: 2 },
  { name: 'Synced Review',      url: 'https://syncedreview.com/feed/',                                 tier: 2 },

  // Tier 3 — Mixed content — USE keyword filter
  { name: 'Hacker News',        url: 'https://news.ycombinator.com/rss',                               tier: 3 },
  { name: 'Reddit LocalLLaMA',  url: 'https://www.reddit.com/r/LocalLLaMA/.rss',                       tier: 3 },
  { name: 'Reddit ML',          url: 'https://www.reddit.com/r/MachineLearning/.rss',                  tier: 3 },
  { name: 'Reddit AI',          url: 'https://www.reddit.com/r/artificial/.rss',                       tier: 3 },
  { name: 'ArXiv AI',           url: 'https://arxiv.org/rss/cs.AI',                                    tier: 3 },
  { name: 'ArXiv ML',           url: 'https://arxiv.org/rss/cs.LG',                                    tier: 3 },
  { name: 'Import AI',          url: 'https://importai.substack.com/feed',                             tier: 3 },
  { name: 'BAIR Blog',          url: 'https://bair.berkeley.edu/blog/feed.xml',                        tier: 3 },
  { name: 'Towards Data Science', url: 'https://towardsdatascience.com/feed',                          tier: 3 },
  { name: 'Last Week in AI',    url: 'https://lastweekin.ai/feed',                                     tier: 3 },
]

// ── KEYWORD FILTER — only used for tier 3 mixed sources ──
const AI_KEYWORDS = [
  'AI', 'artificial intelligence', 'machine learning', 'LLM', 'GPT',
  'Claude', 'Gemini', 'model', 'neural', 'deep learning', 'OpenAI',
  'Anthropic', 'DeepMind', 'Meta AI', 'Mistral', 'diffusion',
  'image generation', 'video generation', 'agent', 'transformer',
  'fine-tuning', 'benchmark', 'training', 'inference', 'Llama',
  'Stable Diffusion', 'Midjourney', 'Runway', 'ElevenLabs', 'Cursor',
  'Copilot', 'Sora', 'xAI', 'Grok', 'Hugging Face', 'foundation model',
  'language model', 'generative', 'multimodal', 'reasoning model',
  'Flux', 'Kling', 'Pika', 'Suno', 'Udio', 'Devin', 'ChatGPT',
  'Perplexity', 'Cohere', 'DALL-E', 'Replit', 'GitHub Copilot',
  'large language', 'neural network', 'computer vision', 'NLP',
  'generative AI', 'autonomous', 'robotics', 'dataset', 'weights'
]

function isAIRelated(title = '', content = '') {
  const text = `${title} ${content.slice(0, 300)}`.toLowerCase()
  return AI_KEYWORDS.some(kw => text.includes(kw.toLowerCase()))
}

// ── CLAUDE CALL ───────────────────────────────────────────
async function callClaude(system, user, maxTokens = 500) {
  const res = await anthropic.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: maxTokens,
    system,
    messages: [{ role: 'user', content: user }]
  })
  const clean = res.content[0].text.trim().replace(/```json\n?|```\n?/g, '').trim()
  return JSON.parse(clean)
}

// ── PROMPTS ───────────────────────────────────────────────
const CLASSIFY_SYSTEM = `You are the editorial classifier for ∞AI — the single source of truth for all AI news.

Classify into exactly one tab:
- FEED: Industry news, funding, regulation, policy, executive statements, Big Tech AI moves
- MODELS: Model releases, updates, benchmarks, architecture research, evals, training techniques
- TOOLS: Product launches, AI apps, feature updates, APIs, image/video/audio/coding tools, agents

Rules:
1. Not AI-related → is_ai_related: false
2. Model inside product → MODELS. Product feature → TOOLS. Business/policy/people → FEED
3. Importance 1-10: 10=GPT-5 launch, 9=major model, 8=big funding/reg, 7=notable, 6=relevant, 1-5=minor
4. push_notify: true only if importance >= 7
5. duplicate_check_string: 4-6 words core topic only

Return ONLY raw JSON. No markdown.`

const CLASSIFY_USER = (a) =>
`TITLE: ${a.title}
SOURCE: ${a.source}
CONTENT: ${(a.content || '').slice(0, 1500)}
Return: {"is_ai_related":boolean,"tab":"FEED"|"MODELS"|"TOOLS","importance":1-10,"push_notify":boolean,"duplicate_check_string":"4-6 words","tags":["tag1","tag2","tag3"]}`

const CARD_SYSTEM = `You write news cards for ∞AI. Be direct, specific, intelligent.

HEADLINE: Max 12 words, present tense, lead with subject, specific detail. Never: revolutionizes, game-changing, groundbreaking, unveils.
SUMMARY: Exactly 3 sentences, max 60 words. S1=what happened. S2=why it matters. S3=availability/pricing/next.
KEY_FACT: Most surprising specific stat/detail, max 15 words, no prefix.
ACCENT_COLOR:
OpenAI=#10a37f Anthropic=#d97706 Google/DeepMind=#4285f4 Meta=#0866ff
Mistral=#ff6b35 Microsoft=#00a4ef StabilityAI=#8b5cf6 HuggingFace=#ff9d00
ElevenLabs=#6366f1 Runway=#ec4899 Midjourney=#f59e0b xAI=#1da1f2
TechCrunch=#e11d48 TheVerge=#ff4500 Other=#6b7280
USE_IMAGE: true ONLY for image/video gen with sample output or physical AI hardware. Otherwise false.
Return ONLY raw JSON. No markdown.`

const CARD_USER = (a, c) =>
`TITLE: ${a.title}
SOURCE: ${a.source}
TAB: ${c.tab}
CONTENT: ${(a.content || '').slice(0, 1500)}
Return: {"headline":"...","summary":"...","key_fact":"...","accent_color":"#hex","use_image":boolean}`

const MERGE_SYSTEM = `Update existing news cards when new source covers same story. Return ONLY raw JSON.`
const MERGE_USER = (e, n) =>
`EXISTING: headline="${e.headline}" key_fact="${e.key_fact}"
NEW SOURCE: ${n.source}
NEW CONTENT: ${(n.content || '').slice(0, 800)}
Return: {"has_new_info":boolean,"updated_key_fact":"...","additional_detail":"one sentence or null"}`

// ── FETCH STATE ───────────────────────────────────────────
async function getLastFetchTime(sourceName) {
  const { data } = await supabase
    .from('source_fetch_state')
    .select('last_fetched_at')
    .eq('source_name', sourceName)
    .maybeSingle()

  if (data?.last_fetched_at) return new Date(data.last_fetched_at)

  console.log(`    First run for ${sourceName} — seeding last 24 hours`)
  return new Date(Date.now() - 24 * 60 * 60 * 1000)
}

async function saveLastFetchTime(sourceName, fetchStartTime) {
  await supabase
    .from('source_fetch_state')
    .upsert({
      source_name:     sourceName,
      last_fetched_at: fetchStartTime.toISOString(),
      updated_at:      new Date().toISOString()
    }, { onConflict: 'source_name' })
}

async function urlAlreadyProcessed(url) {
  const { data } = await supabase
    .from('stories')
    .select('id')
    .eq('source_url', url)
    .maybeSingle()
  return !!data
}

// ── FETCH RSS ─────────────────────────────────────────────
async function fetchFeed(source, skippedLog) {
  const fetchStartTime = new Date()

  try {
    const feed = await parser.parseURL(source.url)
    const lastFetchTime = await getLastFetchTime(source.name)

    console.log(`    Window: after ${lastFetchTime.toLocaleTimeString()}`)

    const newItems = []

    for (const item of feed.items) {
      const pubDate = new Date(item.pubDate || item.isoDate || 0)
      const url = item.link || ''
      const title = item.title || ''

      // Skip if no valid date
      if (isNaN(pubDate.getTime())) {
        skippedLog.push({
          title,
          source: source.name,
          url,
          reason: 'no_date',
          publishedAt: null
        })
        continue
      }

      // Layer 1: time window filter
      if (pubDate <= lastFetchTime) {
        skippedLog.push({
          title,
          source: source.name,
          url,
          reason: 'time_window',
          publishedAt: pubDate.toISOString()
        })
        continue
      }

      // Layer 2: URL dedup
      if (url) {
        const seen = await urlAlreadyProcessed(url)
        if (seen) {
          skippedLog.push({
            title,
            source: source.name,
            url,
            reason: 'url_duplicate',
            publishedAt: pubDate.toISOString()
          })
          continue
        }
      }

      newItems.push(item)
    }

    if (newItems.length === 0) {
      console.log(`    → No new articles in window`)
      await saveLastFetchTime(source.name, fetchStartTime)
      return []
    }

    console.log(`    → ${newItems.length} new articles to process`)
    await saveLastFetchTime(source.name, fetchStartTime)

    return newItems.map(item => ({
      title:       item.title || '',
      content:     item.contentSnippet || item.content || item.summary || '',
      url:         item.link || '',
      publishedAt: item.pubDate || item.isoDate || new Date().toISOString(),
      source:      source.name,
      tier:        source.tier
    }))

  } catch (err) {
    console.error(`    ✗ Error: ${err.message}`)
    return []
  }
}

// ── PROCESS ARTICLE ───────────────────────────────────────
async function processArticle(article, stats, skippedLog, insertedLog) {
  stats.articles_seen++

  // Keyword filter — ONLY for tier 3 mixed sources
  if (article.tier === 3) {
    if (!isAIRelated(article.title, article.content)) {
      stats.skipped_keyword++
      skippedLog.push({
        title:       article.title,
        source:      article.source,
        url:         article.url,
        reason:      'keyword_filter',
        publishedAt: article.publishedAt
      })
      return
    }
  }

  // Claude classification
  let classification
  try {
    classification = await callClaude(CLASSIFY_SYSTEM, CLASSIFY_USER(article), 300)
  } catch (err) {
    stats.errors.push({ step: 'classify', title: article.title, error: err.message })
    return
  }

  if (!classification.is_ai_related) {
    stats.skipped_claude++
    skippedLog.push({
      title:       article.title,
      source:      article.source,
      url:         article.url,
      reason:      'claude_not_ai',
      publishedAt: article.publishedAt
    })
    return
  }

  // Content dedup
  const since = new Date(Date.now() - 86400000).toISOString()
  const { data: existing } = await supabase
    .from('stories')
    .select('id, headline, summary, key_fact, source_count')
    .ilike('duplicate_check_string', `%${classification.duplicate_check_string}%`)
    .gte('created_at', since)
    .maybeSingle()

  if (existing) {
    stats.articles_duped++
    skippedLog.push({
      title:       article.title,
      source:      article.source,
      url:         article.url,
      reason:      'content_dedup_merged',
      publishedAt: article.publishedAt,
      mergedInto:  existing.headline
    })
    try {
      const merge = await callClaude(MERGE_SYSTEM, MERGE_USER(existing, article), 200)
      await supabase.from('stories').update({
        source_count: existing.source_count + 1,
        key_fact:     merge.updated_key_fact || existing.key_fact,
        summary:      merge.has_new_info && merge.additional_detail
                        ? `${existing.summary} ${merge.additional_detail}`
                        : existing.summary
      }).eq('id', existing.id)
      console.log(`    ↗ Merged: "${existing.headline.slice(0, 50)}..."`)
    } catch {
      await supabase.from('stories')
        .update({ source_count: existing.source_count + 1 })
        .eq('id', existing.id)
    }
    return
  }

  // Generate card
  let card
  try {
    card = await callClaude(CARD_SYSTEM, CARD_USER(article, classification), 400)
  } catch (err) {
    stats.errors.push({ step: 'card', title: article.title, error: err.message })
    return
  }

  // Insert
  const { error } = await supabase.from('stories').insert({
    tab:                    classification.tab,
    importance:             classification.importance,
    tags:                   classification.tags || [],
    source_tier:            article.tier,
    headline:               card.headline,
    summary:                card.summary,
    key_fact:               card.key_fact,
    accent_color:           card.accent_color || '#6b7280',
    use_image:              card.use_image || false,
    source_name:            article.source,
    source_url:             article.url,
    original_title:         article.title,
    duplicate_check_string: classification.duplicate_check_string,
    push_notify:            classification.push_notify || false,
    push_sent:              false,
    source_count:           1,
    published_at:           article.publishedAt,
  })

  if (error) {
    stats.errors.push({ step: 'insert', title: article.title, error: error.message })
  } else {
    stats.articles_inserted++
    const push = classification.push_notify ? ' 🔔' : ''
    console.log(`    ✓ [${classification.tab}] i=${classification.importance}${push} — ${card.headline}`)
    insertedLog.push({
      headline:    card.headline,
      tab:         classification.tab,
      importance:  classification.importance,
      source:      article.source,
      url:         article.url,
      publishedAt: article.publishedAt
    })
  }

  await new Promise(r => setTimeout(r, 300))
}

// ── WRITE LOG FILE ────────────────────────────────────────
function writeLogFile(stats, skippedLog, insertedLog, runStartTime) {
  try {
    // Create logs directory if it doesn't exist
    if (!fs.existsSync('logs')) fs.mkdirSync('logs')

    const timestamp = runStartTime.toISOString()
      .replace('T', '_').replace(/:/g, '-').slice(0, 16)
    const filename = `logs/run_${timestamp}.json`

    // Count skip reasons
    const skipReasons = skippedLog.reduce((acc, s) => {
      acc[s.reason] = (acc[s.reason] || 0) + 1
      return acc
    }, {})

    const logData = {
      runTime:          runStartTime.toISOString(),
      duration_seconds: Math.round((Date.now() - runStartTime) / 1000),
      summary: {
        total_fetched:       stats.articles_seen,
        inserted:            stats.articles_inserted,
        merged:              stats.articles_duped,
        skipped_time_window: skipReasons.time_window || 0,
        skipped_url_dedup:   skipReasons.url_duplicate || 0,
        skipped_keyword:     skipReasons.keyword_filter || 0,
        skipped_claude:      skipReasons.claude_not_ai || 0,
        skipped_no_date:     skipReasons.no_date || 0,
        errors:              stats.errors.length,
        efficiency_pct:      stats.articles_seen > 0
          ? Math.round(stats.articles_inserted / stats.articles_seen * 100)
          : 0
      },
      inserted_articles: insertedLog,
      skipped_articles:  skippedLog,
      errors:            stats.errors
    }

    fs.writeFileSync(filename, JSON.stringify(logData, null, 2))
    console.log(`\n📝 Log written: ${filename}`)
    return logData.summary

  } catch (err) {
    console.error(`Failed to write log: ${err.message}`)
    return null
  }
}

// ── MAIN ──────────────────────────────────────────────────
async function runPipeline() {
  const runStartTime = new Date()
  console.log(`\n🚀 ∞AI Pipeline v5 — ${runStartTime.toLocaleString()}`)
  console.log(`   Sources: ${SOURCES.length} | Smart filter: tier1+2 bypass keyword\n`)

  const { data: run } = await supabase
    .from('pipeline_runs')
    .insert({ status: 'running' })
    .select().single()

  const stats = {
    sources_fetched:   0,
    articles_seen:     0,
    articles_inserted: 0,
    articles_duped:    0,
    skipped_keyword:   0,
    skipped_claude:    0,
    errors:            []
  }

  const skippedLog  = []  // every skipped article with reason
  const insertedLog = []  // every inserted article

  for (const source of SOURCES) {
    console.log(`📡 ${source.name} [tier ${source.tier}]`)
    const articles = await fetchFeed(source, skippedLog)

    if (articles.length > 0) {
      stats.sources_fetched++
      for (const article of articles) {
        await processArticle(article, stats, skippedLog, insertedLog)
      }
    }
  }

  // Write detailed log file
  const summary = writeLogFile(stats, skippedLog, insertedLog, runStartTime)

  // Update Supabase pipeline run with full breakdown
  const skipReasons = skippedLog.reduce((acc, s) => {
    acc[s.reason] = (acc[s.reason] || 0) + 1
    return acc
  }, {})

  await supabase.from('pipeline_runs').update({
    finished_at:       new Date().toISOString(),
    status:            stats.errors.length > 5 ? 'failed' : 'done',
    sources_fetched:   stats.sources_fetched,
    articles_seen:     stats.articles_seen,
    articles_skipped:  skippedLog.length,
    articles_duped:    stats.articles_duped,
    articles_inserted: stats.articles_inserted,
    errors:            stats.errors,
    // Store full breakdown in errors column as structured data
    // (reusing jsonb column — rename later if needed)
    errors: {
      breakdown: {
        skipped_time_window: skipReasons.time_window || 0,
        skipped_url_dedup:   skipReasons.url_duplicate || 0,
        skipped_keyword:     skipReasons.keyword_filter || 0,
        skipped_claude:      skipReasons.claude_not_ai || 0,
      },
      actual_errors: stats.errors
    }
  }).eq('id', run.id)

  // Print full summary to console
  console.log(`\n${'═'.repeat(50)}`)
  console.log(`  ∞AI Pipeline Run Summary`)
  console.log(`${'═'.repeat(50)}`)
  console.log(`  Sources fetched:    ${stats.sources_fetched}`)
  console.log(`  Total articles:     ${stats.articles_seen}`)
  console.log(`  ─────────────────────────────────────────`)
  console.log(`  ⏱  Skipped (time):  ${skipReasons.time_window || 0}   — published before last run`)
  console.log(`  🔗  Skipped (url):   ${skipReasons.url_duplicate || 0}   — URL already in DB`)
  console.log(`  🔍  Skipped (kwrd):  ${skipReasons.keyword_filter || 0}   — failed AI keyword filter (tier 3 only)`)
  console.log(`  🤖  Skipped (AI?):   ${skipReasons.claude_not_ai || 0}   — Claude said not AI related`)
  console.log(`  ↗   Merged:          ${stats.articles_duped}   — same story, source count updated`)
  console.log(`  ✅  Inserted:        ${stats.articles_inserted}   — new stories added`)
  console.log(`  ─────────────────────────────────────────`)
  console.log(`  Efficiency:         ${stats.articles_seen > 0
    ? Math.round(stats.articles_inserted / stats.articles_seen * 100) : 0}%`)
  console.log(`  Errors:             ${stats.errors.length}`)
  console.log(`${'═'.repeat(50)}\n`)
}

runPipeline().catch(console.error)
