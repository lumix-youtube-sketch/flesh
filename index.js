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
    USER_AGENT: 'Mozilla/5.0 Chrome/121'
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

CREATE INDEX IF NOT EXISTS idx_news_pub ON news(published_at);
`);

// prepared statements (быстрее)
const STMT = {
    getUser: db.prepare('SELECT * FROM users WHERE id=?'),
    insertUser: db.prepare('INSERT INTO users (id) VALUES (?)'),
    updateCats: db.prepare('UPDATE users SET categories=? WHERE id=?'),
    updateRegs: db.prepare('UPDATE users SET regions=? WHERE id=?'),
    insertSeen: db.prepare('INSERT OR IGNORE INTO seen_log VALUES (?, ?)'),
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
            return { id, categories: [], regions: [] };
        }

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

    markSeen(uid, nid) {
        STMT.insertSeen.run(uid, nid);
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

        args.push(uid);

        const q = `
        SELECT * FROM news
        WHERE (${conditions.join(' OR ')})
        AND id NOT IN (SELECT news_id FROM seen_log WHERE user_id=?)
        ORDER BY published_at DESC LIMIT 1`;

        return db.prepare(q).get(...args);
    }
};

// ---------------- SOURCES ----------------

const CATEGORIES = {
    world: {
        name: '⚡️ Молнии',
        urls: [
            { u: 'https://tass.ru/rss/v2.xml', n: 'ТАСС' },
            { u: 'https://www.kommersant.ru/RSS/main.xml', n: 'Коммерсантъ' },
            { u: 'https://rssexport.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК' },
            { u: 'https://ria.ru/export/rss2/archive/index.xml', n: 'РИА' }
        ]
    },
    tech: {
        name: '📱 Техно',
        urls: [
            { u: 'https://www.cnews.ru/inc/rss/news.xml', n: 'CNews' },
            { u: 'https://3dnews.ru/news/rss/', n: '3DNews' },
            { u: 'https://www.ixbt.com/export/news.rss', n: 'iXBT' }
        ]
    },
    games: {
        name: '🎮 Игры',
        urls: [
            { u: 'https://stopgame.ru/rss/rss_all.xml', n: 'StopGame' },
            { u: 'https://dtf.ru/rss/all', n: 'DTF' }
        ]
    },
    fashion_trends: {
        name: '✨ Тренды',
        urls: [
            { u: 'https://peopletalk.ru/feed/', n: 'PeopleTalk' },
            { u: 'https://style.rbc.ru/rss/style/', n: 'РБК Стиль' }
        ]
    }
};

const REGIONS = {
    moscow: { name: '🏰 Москва', u: 'https://www.m24.ru/rss.xml', n: 'М24' },
    spb: { name: '⚓️ СПб', u: 'https://spb.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК СПб' },
    nsk: { name: '❄️ Новосибирск', u: 'https://nsk.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК Нск' },
    ekb: { name: '⛰ Екатеринбург', u: 'https://ekb.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК Екб' },
    kzn: { name: '🕌 Казань', u: 'https://rt.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК Татарстан' },
    nn: { name: '🏰 НН', u: 'https://nn.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК НН' },
    chel: { name: '🚜 Челябинск', u: 'https://chel.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК Чел' },
    rostov: { name: '⚓️ Ростов', u: 'https://rostov.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК Ростов' },
    vrn: { name: '🌳 Воронеж', u: 'https://vrn.rbc.ru/rbcnews/news/30/full.rss', n: 'РБК Воронеж' }
};

// ---------------- INGESTER ----------------

const Ingester = {

    parser: new RSSParser({
        timeout: CONFIG.RSS_TIMEOUT,
        headers: { 'User-Agent': CONFIG.USER_AGENT }
    }),

    async scrape(url, title) {
        try {
            const { data } = await retry(() =>
                axios.get(url, {
                    timeout: CONFIG.SCRAPE_TIMEOUT,
                    headers: { 'User-Agent': CONFIG.USER_AGENT }
                })
            );

            const $ = cheerio.load(data);

            let img =
                $('meta[property="og:image"]').attr('content') ||
                $('article img').first().attr('src');

            const bad = ['logo', 'favicon', 'social', 'share'];
            if (img && bad.some(p => img.toLowerCase().includes(p))) img = null;

            let paragraphs = [];
            $('article p, .article__text p, p').each((i, el) => {
                const t = $(el).text().trim();
                if (t.length > 60 && t !== title && paragraphs.length < 4)
                    paragraphs.push(t);
            });

            return { img, text: paragraphs.join('\n\n') };

        } catch {
            return { img: null, text: null };
        }
    },

    async run() {
        logger.info('Ingester: цикл сбора');

        let added = 0;
        const sources = [];

        Object.keys(CATEGORIES)
            .forEach(k => CATEGORIES[k].urls.forEach(u =>
                sources.push({ ...u, cat: k, reg: null })
            ));

        Object.keys(REGIONS)
            .forEach(k => sources.push({
                u: REGIONS[k].u,
                n: REGIONS[k].n,
                cat: null,
                reg: k
            }));

        for (let i = 0; i < sources.length; i += CONFIG.CONCURRENCY) {
            const chunk = sources.slice(i, i + CONFIG.CONCURRENCY);

            await Promise.all(chunk.map(async src => {
                try {
                    const feed = await retry(() => this.parser.parseURL(src.u));

                    for (const item of (feed.items || []).slice(0, 10)) {
                        const hash = crypto
                            .createHash('md5')
                            .update(item.link + item.title)
                            .digest('hex');

                        const details = await this.scrape(item.link, item.title);
                        if (!details.img) continue;

                        if (Repo.saveNews({
                            hash,
                            title: item.title?.trim(),
                            body: details.text || '',
                            image: details.img,
                            video: null,
                            source: src.n,
                            cat: src.cat,
                            reg: src.reg,
                            pub: safeDate(item.pubDate || item.isoDate)
                        })) added++;
                    }

                } catch (e) {
                    logger.warn('RSS fail', src.u);
                }
            }));
        }

        logger.info(`Ingester: +${added} новостей`);
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

    ctx.answerCbQuery();
    ctx.action('menu_reg');
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

    try {
        await ctx.replyWithPhoto(news.image_url, {
            caption: `<b>${news.title}</b>\n\n${news.body.slice(0,850)}...\n\n🔹 <i>${news.source_name}</i>`,
            parse_mode: 'HTML',
            reply_markup: Markup.inlineKeyboard([
                [Markup.button.callback('🔄 Еще', 'next')]
            ]).reply_markup
        });

        Repo.markSeen(user.id, news.id);

    } catch {
        Repo.markSeen(user.id, news.id);
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
    logger.info('Flash News запуск');

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
