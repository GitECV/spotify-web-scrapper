import fs from 'fs/promises';
import path from 'path';
import puppeteer from 'puppeteer';

const OUTPUT_DIR = './data';
const BASE_URL = 'https://charts.spotify.com/charts/view';
const LOG_FILE = path.join(OUTPUT_DIR, 'scraper.log');

// ===== CONFIGURACIÓN DE FECHAS =====
// Especifica las fechas que quieres scrapear en formato YYYY-MM-DD
const DATES_TO_SCRAPE = ['2026-01-02', '2026-01-01'];
// ===================================

// Flag para verificar si ya existe la fecha antes de scrapear
// Cambiar a false para forzar el scrapping aunque la fecha ya exista
const SKIP_IF_EXISTS = true;

// Tipos de log con emojis
const LOG_TYPES = {
  INFO: { prefix: '📄', label: 'INFO' },
  SUCCESS: { prefix: '✅', label: 'SUCCESS' },
  ERROR: { prefix: '❌', label: 'ERROR' },
  WARNING: { prefix: '⚠️', label: 'WARNING' },
  WAIT: { prefix: '⏳', label: 'WAIT' },
  PAUSE: { prefix: '⏸️', label: 'PAUSE' },
  DOWNLOAD: { prefix: '📥', label: 'DOWNLOAD' },
  SEARCH: { prefix: '🔍', label: 'SEARCH' },
  PHOTO: { prefix: '📸', label: 'PHOTO' },
  STATS: { prefix: '📊', label: 'STATS' },
  START: { prefix: '🚀', label: 'START' }
};

// Función de logging que escribe en consola y archivo
async function log(message, type = 'INFO') {
  const timestamp = new Date().toISOString();
  const logType = LOG_TYPES[type] || LOG_TYPES.INFO;
  const consoleMessage = `${logType.prefix} ${message}`;
  const fileMessage = `[${timestamp}] [${logType.label}] ${message}\n`;

  // Mostrar en consola
  console.log(consoleMessage);

  // Escribir en archivo
  try {
    await fs.appendFile(LOG_FILE, fileMessage);
  } catch (error) {
    console.error('Error escribiendo en log file:', error);
  }
}

// Función para validar formato de fecha
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

// Función para verificar si ya existe un archivo con la fecha específica
async function checkIfDateExists(country, date) {
  try {
    const fileName = `spotify_${country}_daily_${date}.json`;
    const filePath = path.join(OUTPUT_DIR, date, fileName);

    try {
      await fs.access(filePath);
      return true; // El archivo existe
    } catch {
      return false; // El archivo no existe
    }
  } catch (error) {
    await log(`Error al verificar archivo para ${country}: ${error.message}`, 'WARNING');
    return false;
  }
}

async function downloadCSV(browser, country, date) {
  const page = await browser.newPage();

  try {
    const url = `${BASE_URL}/regional-${country}-daily/${date}`;
    await log(`📍 URL construida: ${url}`, 'INFO');

    // Configurar descarga ANTES de navegar
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

    // Buscar el botón de descarga por aria-labelledby con más tiempo
    try {
      await page.waitForSelector('button[aria-labelledby="csv_download"]', { timeout: 20000 });
      await log('Botón de descarga encontrado', 'SEARCH');
    } catch (error) {
      // Tomar screenshot para debug
      await page.screenshot({ path: path.join(OUTPUT_DIR, `debug_${country}_${date}.png`) });
      await log(`Screenshot guardado en data/debug_${country}_${date}.png`, 'PHOTO');
      await log(`No se encontró el botón de descarga para ${country}. Revisa el screenshot.`, 'ERROR');
      throw new Error('No se encontró el botón de descarga. Revisa el screenshot.');
    }

    // Click en el botón
    await page.click('button[aria-labelledby="csv_download"]');

    const downloadWaitTime = randomDelay(4000, 7000);
    await log(`Esperando descarga (${downloadWaitTime / 1000}s)...`, 'WAIT');

    // Esperar a que se complete la descarga
    await new Promise(resolve => setTimeout(resolve, downloadWaitTime));

    // Leer el archivo descargado
    const files = await fs.readdir(tempDir);
    const csvFile = files.find(f => f.endsWith('.csv'));

    if (!csvFile) {
      throw new Error('No se descargó el archivo CSV');
    }

    await log(`Archivo descargado: ${csvFile}`, 'SUCCESS');

    const csvPath = path.join(tempDir, csvFile);
    const csvContent = await fs.readFile(csvPath, 'utf-8');

    // Extraer la fecha del nombre del archivo CSV
    // Formato esperado: regional-XX-daily-YYYY-MM-DD.csv
    const csvDateMatch = csvFile.match(/(\d{4}-\d{2}-\d{2})/);
    let csvDate = null;
    if (csvDateMatch) {
      csvDate = csvDateMatch[1];
      await log(`Fecha extraída del nombre del CSV: ${csvDate}`, 'INFO');
    }

    // Limpiar archivo temporal
    await fs.unlink(csvPath);

    return { csvContent, csvDate, csvFileName: csvFile };

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
    values.push(current.trim()); // Push the last value

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

async function processCountry(browser, country, date) {
  await log(`Procesando ${country} para fecha ${date}`, 'DOWNLOAD');

  // Descargar el CSV para obtener la fecha real
  const { csvContent, csvDate, csvFileName } = await downloadCSV(browser, country, date);

  if (!csvDate) {
    throw new Error(`No se pudo extraer la fecha del CSV: ${csvFileName}`);
  }

  // Verificar si la fecha del CSV coincide con la fecha solicitada
  if (csvDate !== date) {
    await log(`⚠️  Advertencia: La fecha del CSV (${csvDate}) no coincide con la fecha solicitada (${date})`, 'WARNING');
  }

  // Verificar si ya existe archivo con esta fecha (si SKIP_IF_EXISTS está activado)
  if (SKIP_IF_EXISTS) {
    const exists = await checkIfDateExists(country, csvDate);
    if (exists) {
      await log(`⏭️  ${country.toUpperCase()} saltado - Ya existe archivo para la fecha ${csvDate}`, 'INFO');
      return { skipped: true, country, date: csvDate };
    }
  }

  const rows = parseCSV(csvContent);
  const tracks = normalizeTracks(rows);

  const result = {
    title: 'Spotify Daily Top Songs',
    country: country.toUpperCase(),
    date: csvDate,
    total_tracks: tracks.length,
    tracks
  };

  // Crear carpeta para la fecha si no existe
  const dateFolderPath = path.join(OUTPUT_DIR, csvDate);
  await fs.mkdir(dateFolderPath, { recursive: true });

  const filePath = path.join(
    dateFolderPath,
    `spotify_${country}_daily_${csvDate}.json`
  );

  await fs.writeFile(filePath, JSON.stringify(result, null, 2));
  await log(`${country.toUpperCase()} completado - ${tracks.length} canciones guardadas en ${filePath}`, 'SUCCESS');

  // Espera aleatoria entre países
  const betweenCountriesDelay = randomDelay(1000, 3000);
  await log(`Pausa de ${betweenCountriesDelay / 1000}s antes del siguiente país...\n`, 'PAUSE');
  await new Promise(resolve => setTimeout(resolve, betweenCountriesDelay));

  return { skipped: false, country, date };
}

async function main() {
  // Validar formato de todas las fechas
  for (const date of DATES_TO_SCRAPE) {
    if (!validateDate(date)) {
      console.error(`❌ Error: La fecha "${date}" no es válida. Usa formato YYYY-MM-DD`);
      process.exit(1);
    }
  }

  await fs.mkdir(OUTPUT_DIR, { recursive: true });
  await fs.mkdir(path.join(OUTPUT_DIR, 'temp'), { recursive: true });

  await log(`Iniciando scraping para ${DATES_TO_SCRAPE.length} fecha(s): ${DATES_TO_SCRAPE.join(', ')}`, 'START');
  await log('Iniciando navegador...', 'START');

  const browser = await puppeteer.launch({
    headless: false,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled'
    ]
  });

  // Página inicial para autenticación
  await log('Abriendo página para autenticación...', 'INFO');
  const authPage = await browser.newPage();
  await authPage.goto(`${BASE_URL}/regional-global-daily/latest`, { waitUntil: 'networkidle2', timeout: 60000 });
  await log('Esperando 30 segundos para autenticación manual...', 'WAIT');
  await new Promise(resolve => setTimeout(resolve, 30000));
  await authPage.close();
  await log('Autenticación completada, iniciando descargas...', 'SUCCESS');

  // Lista de países a descargar
  const countries = [
    'global', 'ar', 'au', 'at', 'by', 'be', 'bo', 'br', 'bg', 'ca', 'cl', 'co', 'cr', 'cz', 'dk', 'do',
    'ec', 'eg', 'sv', 'ee', 'fi', 'fr', 'de', 'gr', 'gt', 'hn', 'hk', 'hu', 'is', 'in', 'id', 'ie', 'il',
    'it', 'jp', 'kz', 'lv', 'lt', 'lu', 'my', 'mx', 'ma', 'nl', 'nz', 'ni', 'ng', 'no', 'pk', 'pa', 'py',
    'pe', 'ph', 'pl', 'pt', 'ro', 'sa', 'sg', 'sk', 'za', 'kr', 'es', 'se', 'ch', 'tw', 'th', 'tr', 'ae',
    'ua', 'gb', 'uy', 'us', 've', 'vn'
  ];

  await log(`Modo de verificación: ${SKIP_IF_EXISTS ? 'ACTIVADO (saltará fechas ya scrappeadas)' : 'DESACTIVADO (descargará todo)'}`, 'INFO');

  let totalSuccessCount = 0;
  let totalErrorCount = 0;
  let totalSkippedCount = 0;

  // Procesar cada fecha
  for (let dateIndex = 0; dateIndex < DATES_TO_SCRAPE.length; dateIndex++) {
    const currentDate = DATES_TO_SCRAPE[dateIndex];
    await log(`\n${'='.repeat(60)}`, 'INFO');
    await log(`Procesando fecha ${dateIndex + 1}/${DATES_TO_SCRAPE.length}: ${currentDate}`, 'START');
    await log(`${'='.repeat(60)}`, 'INFO');

    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;

    for (let i = 0; i < countries.length; i++) {
      const country = countries[i];

      try {
        const result = await processCountry(browser, country, currentDate, false);
        if (result && result.skipped) {
          skippedCount++;
        } else {
          successCount++;
        }
      } catch (err) {
        await log(`Error en ${country}: ${err.message}`, 'ERROR');
        errorCount++;
      }
    }

    await log(`\nResumen para fecha ${currentDate}:`, 'STATS');
    await log(`   Exitosos: ${successCount}`, 'SUCCESS');
    await log(`   Saltados: ${skippedCount}`, 'INFO');
    await log(`   Errores: ${errorCount}`, 'ERROR');

    totalSuccessCount += successCount;
    totalErrorCount += errorCount;
    totalSkippedCount += skippedCount;

    // Pausa entre fechas (excepto en la última)
    if (dateIndex < DATES_TO_SCRAPE.length - 1) {
      await log(`\nPróxima fecha: ${DATES_TO_SCRAPE[dateIndex + 1]}`, 'INFO');
      const betweenDatesDelay = randomDelay(2000, 4000);
      await log(`Pausa de ${betweenDatesDelay / 1000}s antes de la siguiente fecha...\n`, 'PAUSE');
      await new Promise(resolve => setTimeout(resolve, betweenDatesDelay));
    }
  }

  await log(`\n${'='.repeat(60)}`, 'INFO');
  await log(`RESUMEN FINAL - ${DATES_TO_SCRAPE.length} fecha(s) procesada(s)`, 'STATS');
  await log(`${'='.repeat(60)}`, 'INFO');
  await log(`   Total Exitosos: ${totalSuccessCount}`, 'SUCCESS');
  await log(`   Total Saltados: ${totalSkippedCount}`, 'INFO');
  await log(`   Total Errores: ${totalErrorCount}`, 'ERROR');
  await log(`   Archivos en: ${OUTPUT_DIR}`, 'INFO');
  await log(`   Fechas procesadas: ${DATES_TO_SCRAPE.join(', ')}`, 'INFO');

  await browser.close();
  await log('Proceso completado', 'SUCCESS');
}

main();
