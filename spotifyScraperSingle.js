import fs from 'fs/promises';
import path from 'path';
import puppeteer from 'puppeteer';

// ===== CONFIGURACIÓN =====
const TARGETS = [
  { date: '2026-01-15', country: 'ph' },
  { date: '2026-01-13', country: 'jp' },
  { date: '2026-01-10', country: 'ee' },
  { date: '2026-01-10', country: 'pl' },
  // Agrega más combinaciones según necesites
];
// =========================

const OUTPUT_DIR = './data';
const BASE_URL = 'https://charts.spotify.com/charts/view';
const LOG_FILE = path.join(OUTPUT_DIR, 'scraper.log');

// Tipos de log con emojis
const LOG_TYPES = {
  INFO: { prefix: '📄', label: 'INFO' },
  SUCCESS: { prefix: '✅', label: 'SUCCESS' },
  ERROR: { prefix: '❌', label: 'ERROR' },
  WARNING: { prefix: '⚠️', label: 'WARNING' },
  WAIT: { prefix: '⏳', label: 'WAIT' },
  DOWNLOAD: { prefix: '📥', label: 'DOWNLOAD' },
  SEARCH: { prefix: '🔍', label: 'SEARCH' },
  PHOTO: { prefix: '📸', label: 'PHOTO' },
  START: { prefix: '🚀', label: 'START' }
};

// Función de logging
async function log(message, type = 'INFO') {
  const timestamp = new Date().toISOString();
  const logType = LOG_TYPES[type] || LOG_TYPES.INFO;
  const consoleMessage = `${logType.prefix} ${message}`;
  const fileMessage = `[${timestamp}] [${logType.label}] ${message}\n`;

  console.log(consoleMessage);

  try {
    await fs.appendFile(LOG_FILE, fileMessage);
  } catch (error) {
    console.error('Error escribiendo en log file:', error);
  }
}

// Validar formato de fecha
function validateDate(dateString) {
  const regex = /^\d{4}-\d{2}-\d{2}$/;
  if (!regex.test(dateString)) {
    return false;
  }
  const date = new Date(dateString);
  return date instanceof Date && !isNaN(date);
}

// Función para generar delay aleatorio
function randomDelay(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function downloadCSV(browser, country, date) {
  const page = await browser.newPage();

  try {
    // Construir URL
    const url = `${BASE_URL}/regional-${country}-daily/${date}`;
    await log(`Navegando a: ${url}`, 'INFO');

    // Configurar descarga
    const tempDir = path.resolve(OUTPUT_DIR, 'temp');
    const client = await page.target().createCDPSession();
    await client.send('Page.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: tempDir
    });

    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

    // Espera aleatoria para simular comportamiento humano
    const waitTime = randomDelay(2000, 5000);
    await log(`Esperando ${waitTime / 1000} segundos...`, 'WAIT');
    await new Promise(resolve => setTimeout(resolve, waitTime));

    // Buscar el botón de descarga
    try {
      await page.waitForSelector('button[aria-labelledby="csv_download"]', { timeout: 20000 });
      await log('Botón de descarga encontrado', 'SEARCH');
    } catch (error) {
      await page.screenshot({ path: path.join(OUTPUT_DIR, `debug_${country}_${date}.png`) });
      await log(`Screenshot guardado en data/debug_${country}_${date}.png`, 'PHOTO');
      throw new Error('No se encontró el botón de descarga');
    }

    // Click en el botón
    await page.click('button[aria-labelledby="csv_download"]');

    const downloadWaitTime = randomDelay(4000, 7000);
    await log(`Esperando descarga (${downloadWaitTime / 1000}s)...`, 'WAIT');
    await new Promise(resolve => setTimeout(resolve, downloadWaitTime));

    // Leer el archivo descargado
    const files = await fs.readdir(tempDir);
    const csvFile = files.find(f => f.endsWith('.csv'));

    if (!csvFile) {
      throw new Error('No se descargó el archivo CSV');
    }

    await log(`Archivo CSV descargado: ${csvFile}`, 'SUCCESS');

    const csvPath = path.join(tempDir, csvFile);
    const csvContent = await fs.readFile(csvPath, 'utf-8');

    // Limpiar archivo temporal
    await fs.unlink(csvPath);

    return csvContent;

  } finally {
    await page.close();
  }
}

function parseCSV(csv) {
  const lines = csv.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.trim());

  return lines.slice(1).map(line => {
    const values = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];

      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    values.push(current.trim());

    return headers.reduce((acc, header, index) => {
      acc[header] = values[index] || '';
      return acc;
    }, {});
  });
}

function normalizeTracks(rows) {
  return rows.map(row => ({
    rank: Number(row.rank),
    uri: row.uri,
    artist_names: row.artist_names,
    track_name: row.track_name,
    source: row.source,
    peak_rank: Number(row.peak_rank),
    previous_rank: row.previous_rank ? Number(row.previous_rank) : null,
    days_on_chart: Number(row.days_on_chart),
    streams: Number(row.streams)
  }));
}

async function main() {
  // Validar configuración
  if (!Array.isArray(TARGETS) || TARGETS.length === 0) {
    console.error(`❌ Error: TARGETS debe ser un array con al menos un elemento`);
    process.exit(1);
  }

  // Validar cada target
  for (let i = 0; i < TARGETS.length; i++) {
    const target = TARGETS[i];
    if (!validateDate(target.date)) {
      console.error(`❌ Error: La fecha "${target.date}" en el índice ${i} no es válida. Usa formato YYYY-MM-DD`);
      process.exit(1);
    }
    if (!target.country || target.country.trim() === '') {
      console.error(`❌ Error: El país "${target.country}" en el índice ${i} no es válido`);
      process.exit(1);
    }
  }

  await fs.mkdir(OUTPUT_DIR, { recursive: true });
  await fs.mkdir(path.join(OUTPUT_DIR, 'temp'), { recursive: true });

  await log(`Iniciando scraping para ${TARGETS.length} combinación(es) de fecha/país`, 'START');

  const browser = await puppeteer.launch({
    headless: false,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled'
    ]
  });

  try {
    // Página inicial para autenticación
    await log('Abriendo página para autenticación...', 'INFO');
    const authPage = await browser.newPage();
    await authPage.goto(`${BASE_URL}/regional-global-daily/latest`, { waitUntil: 'networkidle2', timeout: 60000 });
    await log('Esperando 40 segundos para autenticación manual si es necesaria...', 'WAIT');
    await new Promise(resolve => setTimeout(resolve, 40000));
    await authPage.close();
    await log('Autenticación completada, iniciando descarga...', 'SUCCESS');

    // Procesar cada target
    let completedCount = 0;
    let errorCount = 0;

    for (const target of TARGETS) {
      const { date, country } = target;
      
      try {
        await log(`[${completedCount + errorCount + 1}/${TARGETS.length}] Procesando ${country.toUpperCase()} - ${date}...`, 'DOWNLOAD');

        // Descargar CSV
        const csvContent = await downloadCSV(browser, country, date);

        // Parsear y normalizar datos
        const rows = parseCSV(csvContent);
        const tracks = normalizeTracks(rows);

        // Crear objeto JSON
        const result = {
          title: 'Spotify Daily Top Songs',
          country: country.toUpperCase(),
          date: date,
          total_tracks: tracks.length,
          tracks
        };

        // Crear carpeta para la fecha
        const dateFolderPath = path.join(OUTPUT_DIR, date);
        await fs.mkdir(dateFolderPath, { recursive: true });

        // Guardar JSON
        const filePath = path.join(
          dateFolderPath,
          `spotify_${country}_daily_${date}.json`
        );

        await fs.writeFile(filePath, JSON.stringify(result, null, 2));
        await log(`${country.toUpperCase()} - ${date} completado - ${tracks.length} canciones guardadas`, 'SUCCESS');
        await log(`Archivo guardado en: ${filePath}`, 'INFO');
        
        completedCount++;

        // Espera entre descargas para evitar ser bloqueado
        if (completedCount < TARGETS.length) {
          const pauseTime = randomDelay(3000, 6000);
          await log(`Pausa de ${pauseTime / 1000}s antes de la siguiente descarga...`, 'WAIT');
          await new Promise(resolve => setTimeout(resolve, pauseTime));
        }

      } catch (error) {
        errorCount++;
        await log(`Error procesando ${country.toUpperCase()} - ${date}: ${error.message}`, 'ERROR');
      }
    }

    await log(`Proceso completado: ${completedCount} exitosos, ${errorCount} errores`, completedCount === TARGETS.length ? 'SUCCESS' : 'WARNING');

  } catch (error) {
    await log(`Error crítico: ${error.message}`, 'ERROR');
    throw error;
  } finally {
    await browser.close();
  }
}

main();
