/**
 * FLASH NEWS BOT v11.1 — HARDENED PRODUCTION BUILD
 * Логика не менялась. Только стабильность, скорость и защита.
 */

require('dotenv').config();
const { Telegraf, Markup } = require('telegraf');
const RSSParser = require('rss-parser');
const axios = require('axios');
const cheerio = require('cheerio');
const Database = require('better-sqlite3');
const cron = require('node-cron');
const pino = require('pino');
const crypto = require('crypto');

// ---------------- CONFIG ----------------

const CONFIG = {
    DB_PATH: 'flash_news_prod.db',
    RSS_TIMEOUT: 12000,
    SCRAPE_TIMEOUT: 8000,
    CONCURRENCY: 5,
    NEWS_TTL_DAYS: 3,
    RETRIES: 2,
    USER_AGENT: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
};

const SOURCE_COOLDOWN_MS = 60 * 60 * 1000;
const SOURCE_FAILURES = new Map();

const REQUEST_HEADERS = {
    'User-Agent': CONFIG.USER_AGENT,
    'Accept': 'application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, text/html;q=0.7, */*;q=0.5',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache'
};

const CONTENT_LIMITS = {
    MIN_BODY: 320,
    MAX_BODY: 1200,
    PARAGRAPHS_MAX: 8
};

const logger = pino({
    transport: { target: 'pino-pretty', options: { colorize: true } }
});

// ---------------- HELPERS ----------------

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function retry(fn, retries = CONFIG.RETRIES) {
    try {
        return await fn();
    } catch (e) {
        if (!retries) throw e;
        await sleep(800);
        return retry(fn, retries - 1);
    }
}

const safeDate = d => {
    const t = new Date(d).getTime();
    return Number.isFinite(t) ? t : Date.now();
};

const normalizeMediaUrl = (candidate, baseUrl) => {
    if (!candidate || typeof candidate !== 'string') return null;
    const cleaned = candidate.trim();
    if (!cleaned || cleaned.startsWith('data:')) return null;

    try {
        return new URL(cleaned, baseUrl).toString();
    } catch {
        return null;
    }
};

const cleanBodyText = (raw, title = '') => {
    if (!raw) return '';

    const banned = /(подписыв|подпиш|реклама|erid|промокод|ggsel|vk\.com|t\.me|telegram|дзен|youtube|rutube|@|новости партнеров|подробн[её]е|опубликовано в)/i;
    const titleNorm = (title || '').trim().toLowerCase();

    const normalized = String(raw)
        .replace(/<[^>]+>/g, ' ')
        .replace(/\r/g, '\n')
        .replace(/\t/g, ' ')
        .replace(/\u00a0/g, ' ')
        .replace(/[ ]{2,}/g, ' ')
        .replace(/\n{3,}/g, '\n\n')
        .trim();

    const parts = normalized
        .split(/\n{1,2}/)
        .map(v => v.trim())
        .filter(Boolean)
        .filter(v => v.toLowerCase() !== titleNorm)
        .filter(v => !banned.test(v))
        .filter(v => /[.!?…]/.test(v) || v.length > 140);

    return parts.join('\n\n').trim();
};

const isRussianText = (value, minCyrillicRatio = 0.45) => {
    const text = (value || '').replace(/\s+/g, ' ').trim();
    if (!text) return false;

    const letters = text.match(/[A-Za-zА-Яа-яЁё]/g) || [];
    if (!letters.length) return false;

    const cyr = text.match(/[А-Яа-яЁё]/g) || [];
    return (cyr.length / letters.length) >= minCyrillicRatio;
};

const extractMediaFromItem = item => {
    const possibleImage = [
        item.enclosure?.url,
        item['media:content']?.url,
        item['media:thumbnail']?.url,
        item.image?.url,
        item.itunes?.image,
        item.thumbnail
    ];

    const possibleVideo = [
        item.video,
        item.enclosure?.type?.startsWith('video/') ? item.enclosure?.url : null,
        item['media:content']?.type?.startsWith('video/') ? item['media:content']?.url : null
    ];

    return {
        img: possibleImage.map(v => normalizeMediaUrl(v, item.link)).find(Boolean) || null,
        video: possibleVideo.map(v => normalizeMediaUrl(v, item.link)).find(Boolean) || null
    };
};

const buildNewsBody = (title, text, fallbackText) => {
    const sanitize = value => cleanBodyText(value, title)
        .replace(/\s+/g, ' ')
        .trim();

    const splitBySentence = input => input
        .split(/(?<=[.!?…])\s+/)
        .map(s => s.trim())
        .filter(Boolean);

    const textValue = sanitize(text);
    const fallbackValue = sanitize(fallbackText);
    const textLooksBad = isLowQualityText(textValue);
    const candidate = !textLooksBad && textValue.length >= 180
        ? textValue
        : (fallbackValue || textValue);

    if (!candidate) return '';

    let body = '';
    const sentences = splitBySentence(candidate);

    for (const sentence of sentences) {
        const next = body ? `${body} ${sentence}` : sentence;
        if (next.length > CONTENT_LIMITS.MAX_BODY) break;
        body = next;
    }

    if (!body || body.length < CONTENT_LIMITS.MIN_BODY) {
        body = candidate.slice(0, CONTENT_LIMITS.MAX_BODY);
    }

    body = cleanBodyText(body, title).trim();
    if (!body) return '';

    if (body.length > CONTENT_LIMITS.MAX_BODY) {
        body = `${body.slice(0, CONTENT_LIMITS.MAX_BODY - 1).trimEnd()}…`;
    }

    if (body.toLowerCase() === (title || '').trim().toLowerCase()) return '';

    return body;
};

// ---------------- DATABASE ----------------

const db = new Database(CONFIG.DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

db.exec(`
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    categories TEXT DEFAULT '[]',
    regions TEXT DEFAULT '[]',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT UNIQUE,
    title TEXT,
    body TEXT,
    image_url TEXT,
    video_url TEXT,
    source_name TEXT,
    category TEXT,
    region TEXT,
    published_at INTEGER
);

CREATE TABLE IF NOT EXISTS seen_log (
    user_id INTEGER,
    news_id INTEGER,
    PRIMARY KEY (user_id, news_id)
);

CREATE TABLE IF NOT EXISTS user_state (
    user_id INTEGER PRIMARY KEY,
    recent_sources TEXT DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_news_pub ON news(published_at);
`);

// prepared statements (быстрее)
const STMT = {
    getUser: db.prepare('SELECT * FROM users WHERE id=?'),
    insertUser: db.prepare('INSERT INTO users (id) VALUES (?)'),
    updateCats: db.prepare('UPDATE users SET categories=? WHERE id=?'),
    updateRegs: db.prepare('UPDATE users SET regions=? WHERE id=?'),
    insertSeen: db.prepare('INSERT OR IGNORE INTO seen_log VALUES (?, ?)'),
    getUserState: db.prepare('SELECT recent_sources FROM user_state WHERE user_id=?'),
    upsertUserState: db.prepare('INSERT INTO user_state (user_id, recent_sources) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET recent_sources=excluded.recent_sources'),
    cleanupNews: db.prepare('DELETE FROM news WHERE published_at < ?'),
    cleanupSeen: db.prepare('DELETE FROM seen_log WHERE news_id NOT IN (SELECT id FROM news)'),
    insertNews: db.prepare(`
        INSERT INTO news
        (hash,title,body,image_url,video_url,source_name,category,region,published_at)
        VALUES (@hash,@title,@body,@image,@video,@source,@cat,@reg,@pub)
    `)
};

const Repo = {

    getUser(id) {
        let u = STMT.getUser.get(id);

        if (!u) {
            STMT.insertUser.run(id);
            STMT.upsertUserState.run(id, '[]');
            return { id, categories: [], regions: [] };
        }

        if (!STMT.getUserState.get(id)) STMT.upsertUserState.run(id, '[]');

        return {
            ...u,
            categories: JSON.parse(u.categories || '[]'),
            regions: JSON.parse(u.regions || '[]')
        };
    },

    saveNews(n) {
        try {
            STMT.insertNews.run(n);
            return true;
        } catch {
            return false;
        }
    },

    markSeen(uid, nid, sourceName) {
        STMT.insertSeen.run(uid, nid);

        if (!sourceName) return;

        const row = STMT.getUserState.get(uid);
        const recent = JSON.parse(row?.recent_sources || '[]');
        const next = [sourceName, ...recent].slice(0, 2);
        STMT.upsertUserState.run(uid, JSON.stringify(next));
    },

    getRecentSources(uid) {
        const row = STMT.getUserState.get(uid);
        return JSON.parse(row?.recent_sources || '[]');
    },

    getUnseen(uid, cats, regs) {

        if (!cats.length && !regs.length) return null;

        const conditions = [];
        const args = [];

        if (cats.length) {
            conditions.push(`category IN (${cats.map(() => '?').join(',')})`);
            args.push(...cats);
        }

        if (regs.length) {
            conditions.push(`region IN (${regs.map(() => '?').join(',')})`);
            args.push(...regs);
        }

        const baseWhere = `(${conditions.join(' OR ')})
        AND id NOT IN (SELECT news_id FROM seen_log WHERE user_id=?)`;

        const recent = this.getRecentSources(uid);
        if (recent.length === 2 && recent[0] === recent[1]) {
            const preferred = db.prepare(`
                SELECT * FROM news
                WHERE ${baseWhere}
                AND source_name != ?
                ORDER BY published_at DESC LIMIT 1
            `).get(...args, uid, recent[0]);

            if (preferred) return preferred;
        }

        return db.prepare(`
            SELECT * FROM news
            WHERE ${baseWhere}
            ORDER BY published_at DESC LIMIT 1
        `).get(...args, uid);
    }
};

// ---------------- SOURCES ----------------

const CATEGORIES = {
    world: {
        name: '⚡️ Молнии',
        urls: [
            { u: 'https://tass.ru/rss/v2.xml', n: 'ТАСС' },
            { u: 'https://www.vedomosti.ru/rss/news', n: 'Ведомости' },
            { u: 'https://rssexport.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК' },
            { u: 'https://ria.ru/export/rss2/archive/index.xml', n: 'РИА' },
            { u: 'https://lenta.ru/rss', n: 'Lenta' },
            { u: 'https://iz.ru/xml/rss/all.xml', n: 'Известия' },
            { u: 'https://www.interfax.ru/rss.asp', n: 'Интерфакс' },
            { u: 'https://www.rt.com/rss/news/', n: 'RT' },
            { u: 'http://feeds.bbci.co.uk/news/world/rss.xml', n: 'BBC World' },
            { u: 'https://www.theguardian.com/world/rss', n: 'Guardian World' }
        ]
    },
    tech: {
        name: '📱 Техно',
        urls: [
            { u: 'https://www.cnews.ru/inc/rss/news.xml', n: 'CNews' },
            { u: 'https://www.ixbt.com/export/news.rss', n: 'iXBT' },
            { u: 'https://habr.com/ru/rss/news/', n: 'Хабр' },
            { u: 'https://vc.ru/rss/all', n: 'vc.ru' },
            { u: 'https://www.ferra.ru/export/rss.xml', n: 'Ferra' },
            { u: 'https://www.overclockers.ru/rss/all.xml', n: 'Overclockers' },
            { u: 'https://naked-science.ru/feed', n: 'Naked Science' },
            { u: 'https://www.theverge.com/rss/index.xml', n: 'The Verge' },
            { u: 'https://techcrunch.com/feed/', n: 'TechCrunch' },
            { u: 'https://www.wired.com/feed/rss', n: 'Wired' }
        ]
    },
    games: {
        name: '🎮 Игры',
        urls: [
            { u: 'https://stopgame.ru/rss/rss_all.xml', n: 'StopGame' },
            { u: 'https://dtf.ru/rss/all', n: 'DTF' },
            { u: 'https://feeds.ign.com/ign/all', n: 'IGN' },
            { u: 'https://www.gamespot.com/feeds/mashup/', n: 'GameSpot' },
            { u: 'https://www.pcgamer.com/rss/', n: 'PC Gamer' },
            { u: 'https://www.eurogamer.net/rss.xml', n: 'Eurogamer' },
            { u: 'https://www.polygon.com/rss/index.xml', n: 'Polygon' },
            { u: 'https://kotaku.com/rss', n: 'Kotaku' },
            { u: 'https://www.rockpapershotgun.com/feed', n: 'Rock Paper Shotgun' },
            { u: 'https://www.gamesradar.com/rss/', n: 'GamesRadar' }
        ]
    },
    sport: {
        name: '⚽ Спорт',
        urls: [
            { u: 'https://www.sports.ru/rss/all_news.xml', n: 'Sports.ru' },
            { u: 'https://sport24.ru/rss/news', n: 'Sport24' },
            { u: 'https://www.eurosport.com/rss.xml', n: 'Eurosport' },
            { u: 'https://www.espn.com/espn/rss/news', n: 'ESPN' },
            { u: 'http://feeds.bbci.co.uk/sport/rss.xml', n: 'BBC Sport' },
            { u: 'https://www.skysports.com/rss/12040', n: 'Sky Sports' },
            { u: 'https://www.sport-express.ru/services/materials/news/se/', n: 'Спорт-Экспресс' },
            { u: 'https://sportrbc.ru/rss/news', n: 'РБК Спорт' },
            { u: 'https://lenta.ru/rss/news/sport', n: 'Lenta Sport' },
            { u: 'https://www.theguardian.com/uk/sport/rss', n: 'Guardian Sport' }
        ]
    },
    music: {
        name: '🎵 Музыка',
        urls: [
            { u: 'https://www.intermedia.ru/rss/news', n: 'InterMedia' },
            { u: 'https://newsmuz.com/rss.xml', n: 'NewsMuz' },
            { u: 'https://www.billboard.com/feed/', n: 'Billboard' },
            { u: 'https://www.rollingstone.com/music/music-news/feed/', n: 'Rolling Stone' },
            { u: 'https://www.nme.com/news/music/feed', n: 'NME' },
            { u: 'https://pitchfork.com/rss/news/', n: 'Pitchfork' },
            { u: 'https://www.stereogum.com/feed/', n: 'Stereogum' },
            { u: 'https://consequence.net/feed/', n: 'Consequence' },
            { u: 'https://www.music-news.com/rss/UK/news?includeCover=true', n: 'Music-News' },
            { u: 'https://www.classicfm.com/discover-music/latest/rss.xml', n: 'Classic FM' }
        ]
    }
};

const REGIONS = {
    moscow: { name: '🏰 Москва', keywords: ['москва', 'москве', 'москвы', 'московск', 'подмосков', 'собянин'] },
    spb: { name: '⚓️ СПб', keywords: ['санкт-петербург', 'санкт петербург', 'петербург', 'спб', 'ленобл', 'ленинградск'] },
    nsk: { name: '❄️ Новосибирск', keywords: ['новосибирск', 'новосибирске', 'новосибирской области', 'нсо'] },
    ekb: { name: '⛰ Екатеринбург', keywords: ['екатеринбург', 'екатеринбурге', 'свердловск', 'свердловской области'] },
    kzn: { name: '🕌 Казань', keywords: ['казань', 'казани', 'татарстан', 'татарстане'] },
    nn: { name: '🏰 НН', keywords: ['нижний новгород', 'нижнем новгороде', 'нижегородск', 'нижегородской области'] },
    chel: { name: '🚜 Челябинск', keywords: ['челябинск', 'челябинске', 'челябинской области', 'южный урал'] },
    rostov: { name: '⚓️ Ростов', keywords: ['ростов-на-дону', 'ростов на дону', 'ростовской области', 'ростове', 'дон'] },
    vrn: { name: '🌳 Воронеж', keywords: ['воронеж', 'воронеже', 'воронежской области'] }
};

const REGION_BACKBONE_SOURCES = [
    { u: 'https://tass.ru/rss/v2.xml', n: 'ТАСС Регионы' },
    { u: 'https://ria.ru/export/rss2/archive/index.xml', n: 'РИА Регионы' },
    { u: 'https://rssexport.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК Регионы' },
    { u: 'https://lenta.ru/rss', n: 'Lenta Регионы' },
    { u: 'https://aif.ru/rss/all.php', n: 'АиФ Регионы' }
];

const normalizeText = value => (value || '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[^\p{L}\p{N}\s-]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const hasTestWord = item => {
    const haystack = normalizeText([
        item?.title,
        item?.contentSnippet,
        item?.content,
        item?.summary
    ].filter(Boolean).join(' '));

    return /(^|\s)тест(\s|$)/i.test(haystack);
};

const isItemMatchingRegion = (item, keywords = [], sourceName = '') => {
    if (!keywords.length) return false;

    const haystack = normalizeText([
        item.title,
        item.contentSnippet,
        item.content,
        item.summary,
        item.creator,
        sourceName
    ].filter(Boolean).join(' '));

    return keywords.some(k => haystack.includes(normalizeText(k)));
};

const isLowQualityText = text => {
    if (!text) return true;

    const lines = text
        .split('\n')
        .map(l => l.trim())
        .filter(Boolean);

    const paragraphs = text
        .split(/\n{2,}/)
        .map(p => p.trim())
        .filter(Boolean);

    if (!lines.length) return true;

    const punctuationCount = (text.match(/[.!?…]/g) || []).length;
    const shortHeadlineLines = lines.filter(line => line.length <= 120 && !/[.!?…]/.test(line)).length;

    const avgParagraphLength = paragraphs.length
        ? Math.round(paragraphs.reduce((acc, p) => acc + p.length, 0) / paragraphs.length)
        : 0;

    const headlineListLike =
        paragraphs.length >= 4 &&
        punctuationCount <= 2 &&
        avgParagraphLength <= 150;

    const tooManyHeadlineStarts = (text.match(/\n[А-ЯA-ZЁ][^.!?\n]{20,120}\n/g) || []).length >= 3;

    return headlineListLike || tooManyHeadlineStarts || (lines.length >= 5 && punctuationCount < 2 && shortHeadlineLines >= Math.ceil(lines.length * 0.6));
};


// ---------------- INGESTER ----------------

const Ingester = {

    parser: new RSSParser({
        timeout: CONFIG.RSS_TIMEOUT,
        headers: REQUEST_HEADERS
    }),

    async scrape(url, title) {
        try {
            const { data } = await retry(() =>
                axios.get(url, {
                    timeout: CONFIG.SCRAPE_TIMEOUT,
                    headers: REQUEST_HEADERS
                })
            );

            const $ = cheerio.load(data);

            let img =
                $('meta[property="og:image"]').attr('content') ||
                $('meta[name="twitter:image"]').attr('content') ||
                $('article img').first().attr('src');

            const bad = ['logo', 'favicon', 'social', 'share'];
            if (img && bad.some(p => img.toLowerCase().includes(p))) img = null;

            const video =
                $('meta[property="og:video"]').attr('content') ||
                $('video source').first().attr('src') ||
                $('video').first().attr('src') ||
                null;

            let paragraphs = [];
            $('article p, .article__text p, p').each((i, el) => {
                const t = $(el).text().trim();
                if (t.length > 60 && t !== title && paragraphs.length < CONTENT_LIMITS.PARAGRAPHS_MAX)
                    paragraphs.push(t);
            });

            const scrapedText = paragraphs.join('\n\n');

            return {
                img: normalizeMediaUrl(img, url),
                video: normalizeMediaUrl(video, url),
                text: isLowQualityText(scrapedText) ? null : scrapedText
            };

        } catch {
            return { img: null, video: null, text: null };
        }
    },

    async run() {
        logger.info('Ingester cycle started');

        let added = 0;
        const sources = [];
        const feedCache = new Map();
        const feedErrorCache = new Map();
        const detailsCache = new Map();

        Object.keys(CATEGORIES)
            .forEach(k => CATEGORIES[k].urls.forEach(u =>
                sources.push({ ...u, cat: k, reg: null })
            ));

        Object.keys(REGIONS)
            .forEach(k => {
                const region = REGIONS[k];

                REGION_BACKBONE_SOURCES.forEach(src => sources.push({
                    ...src,
                    cat: null,
                    reg: k,
                    keywords: region.keywords
                }));
            });

        for (let i = 0; i < sources.length; i += CONFIG.CONCURRENCY) {
            const chunk = sources.slice(i, i + CONFIG.CONCURRENCY);

            await Promise.all(chunk.map(async src => {
                try {
                    const failedAt = SOURCE_FAILURES.get(src.u);
                    if (failedAt && (Date.now() - failedAt) < SOURCE_COOLDOWN_MS) return;

                    if (feedErrorCache.has(src.u)) return;

                    let feedPromise = feedCache.get(src.u);
                    if (!feedPromise) {
                        feedPromise = retry(() => this.parser.parseURL(src.u));
                        feedCache.set(src.u, feedPromise);
                    }

                    const feed = await feedPromise;

                    for (const item of (feed.items || []).slice(0, 14)) {
                        if (src.reg && src.keywords && !isItemMatchingRegion(item, src.keywords, src.n)) continue;
                        if (!item.link || !item.title) continue;

                        const hash = crypto
                            .createHash('md5')
                            .update(item.link + item.title)
                            .digest('hex');

                        let details = detailsCache.get(item.link);
                        if (!details) {
                            details = await this.scrape(item.link, item.title);
                            detailsCache.set(item.link, details);
                        }
                        const fallbackMedia = extractMediaFromItem(item);
                        const image = details.img || fallbackMedia.img;
                        const video = details.video || fallbackMedia.video;
                        const body = buildNewsBody(
                            item.title,
                            details.text,
                            item.contentSnippet || item.content || item.summary || ''
                        );

                        const finalBody = body || (item.contentSnippet || item.summary || item.title || '').slice(0, CONTENT_LIMITS.MAX_BODY);
                        const russianEnough = isRussianText(item.title, 0.5) && isRussianText(finalBody, 0.5);
                        if (!russianEnough) continue;

                        const forcedByTestWord = hasTestWord(item);

                        if (!forcedByTestWord && !src.reg && !image && !video) continue;
                        if (!forcedByTestWord && !body) continue;

                        if (Repo.saveNews({
                            hash,
                            title: item.title?.trim(),
                            body: finalBody,
                            image: image || ((forcedByTestWord || src.reg) ? 'https://upload.wikimedia.org/wikipedia/commons/thumb/6/65/No-Image-Placeholder.svg/640px-No-Image-Placeholder.svg.png' : null),
                            video,
                            source: src.n,
                            cat: src.cat,
                            reg: src.reg,
                            pub: safeDate(item.pubDate || item.isoDate)
                        })) added++;
                    }

                } catch (e) {
                    if (feedErrorCache.has(src.u)) return;
                    feedErrorCache.set(src.u, true);
                    SOURCE_FAILURES.set(src.u, Date.now());
                    logger.warn({ err: e?.message, url: src.u }, 'RSS fail');
                }
            }));
        }

        logger.info(`Ingester added +${added} items`);
    }
};

// ---------------- BOT ----------------

const bot = new Telegraf(process.env.BOT_TOKEN);

const getMenu = user => {
    const btns = Object.keys(CATEGORIES)
        .map(k => [
            Markup.button.callback(
                `${user.categories.includes(k) ? '✅' : '⬜'} ${CATEGORIES[k].name}`,
                `toggle_cat_${k}`
            )
        ]);

    btns.push([
        Markup.button.callback(
            `📍 Регион: ${user.regions.length ? 'Выбран' : 'Нет'}`,
            'menu_reg'
        )
    ]);

    btns.push([Markup.button.callback('🚀 СЛЕДУЮЩАЯ НОВОСТЬ', 'next')]);

    return Markup.inlineKeyboard(btns);
};

// start
bot.start(ctx => {
    const user = Repo.getUser(ctx.from.id);

    ctx.reply('Flash News PRO готов.',
        Markup.keyboard([['📱 Меню']]).resize().persistent());

    ctx.reply('Настройте интересы:', getMenu(user));
});

// toggle category
bot.action(/toggle_cat_(.+)/, ctx => {
    const user = Repo.getUser(ctx.from.id);
    const cat = ctx.match[1];

    user.categories = user.categories.includes(cat)
        ? user.categories.filter(c => c !== cat)
        : [...user.categories, cat];

    STMT.updateCats.run(JSON.stringify(user.categories), user.id);

    ctx.answerCbQuery();
    ctx.editMessageReplyMarkup(getMenu(user).reply_markup).catch(()=>{});
});

// region menu
bot.action('menu_reg', ctx => {
    const user = Repo.getUser(ctx.from.id);

    const btns = Object.keys(REGIONS).map(k => [
        Markup.button.callback(
            `${user.regions.includes(k) ? '✅' : '⬜'} ${REGIONS[k].name}`,
            `toggle_reg_${k}`
        )
    ]);

    btns.push([Markup.button.callback('⬅️ Назад', 'back_main')]);

    ctx.editMessageText('Выберите регионы:',
        Markup.inlineKeyboard(btns));
});

// toggle region
bot.action(/toggle_reg_(.+)/, ctx => {
    const user = Repo.getUser(ctx.from.id);
    const reg = ctx.match[1];

    user.regions = user.regions.includes(reg)
        ? user.regions.filter(r => r !== reg)
        : [...user.regions, reg];

    STMT.updateRegs.run(JSON.stringify(user.regions), user.id);

    const btns = Object.keys(REGIONS).map(k => [
        Markup.button.callback(
            `${user.regions.includes(k) ? '✅' : '⬜'} ${REGIONS[k].name}`,
            `toggle_reg_${k}`
        )
    ]);

    btns.push([Markup.button.callback('⬅️ Назад', 'back_main')]);

    ctx.answerCbQuery();
    ctx.editMessageText('Выберите регионы:',
        Markup.inlineKeyboard(btns)).catch(() => {});
});

bot.action('back_main', ctx => {
    const user = Repo.getUser(ctx.from.id);
    ctx.answerCbQuery();
    ctx.editMessageText('Настройте интересы:', getMenu(user)).catch(() => {});
});

// send news
bot.action('next', ctx => sendOne(ctx));
bot.hears('📱 Меню', ctx =>
    ctx.reply('Настройки:', getMenu(Repo.getUser(ctx.from.id)))
);

async function sendOne(ctx) {
    const user = Repo.getUser(ctx.from.id);
    const news = Repo.getUnseen(user.id, user.categories, user.regions);

    if (!news)
        return ctx.answerCbQuery('📭 Пока пусто', { show_alert: true });

    if (!isRussianText(news.title, 0.5) || !isRussianText(news.body, 0.5)) {
        Repo.markSeen(user.id, news.id, news.source_name);
        return sendOne(ctx);
    }

    const body = news.body.length > CONTENT_LIMITS.MAX_BODY
        ? `${news.body.slice(0, CONTENT_LIMITS.MAX_BODY - 1).trimEnd()}…`
        : news.body;

    const caption = `<b>${news.title}</b>

${body}

🔹 <i>${news.source_name}</i>`;

    const keyboard = Markup.inlineKeyboard([
        [Markup.button.callback('🔄 Еще', 'next')]
    ]).reply_markup;

    try {
        if (news.video_url) {
            await ctx.replyWithVideo(news.video_url, {
                caption,
                parse_mode: 'HTML',
                reply_markup: keyboard
            });
        } else {
            await ctx.replyWithPhoto(news.image_url, {
                caption,
                parse_mode: 'HTML',
                reply_markup: keyboard
            });
        }

        Repo.markSeen(user.id, news.id, news.source_name);

    } catch {
        try {
            if (news.image_url) {
                await ctx.replyWithPhoto(news.image_url, {
                    caption,
                    parse_mode: 'HTML',
                    reply_markup: keyboard
                });
                Repo.markSeen(user.id, news.id, news.source_name);
                return;
            }
        } catch {}

        try {
            if (news.video_url) {
                await ctx.replyWithVideo(news.video_url, {
                    caption,
                    parse_mode: 'HTML',
                    reply_markup: keyboard
                });
                Repo.markSeen(user.id, news.id, news.source_name);
                return;
            }
        } catch {}

        Repo.markSeen(user.id, news.id, news.source_name);
        return sendOne(ctx);
    }
}


// ---------------- CRON ----------------

cron.schedule('*/10 * * * *', () => Ingester.run());

cron.schedule('0 4 * * *', () => {
    const limit = Date.now() - CONFIG.NEWS_TTL_DAYS * 86400000;
    STMT.cleanupNews.run(limit);
    STMT.cleanupSeen.run();
    logger.info('Cleanup done');
});

// ---------------- START ----------------

(async () => {
    logger.info('Flash News started');

    await Ingester.run();
    await bot.launch();

    process.once('SIGINT', shutdown);
    process.once('SIGTERM', shutdown);
})();

function shutdown() {
    logger.info('Shutdown...');
    bot.stop();
    db.close();
    process.exit(0);
}
