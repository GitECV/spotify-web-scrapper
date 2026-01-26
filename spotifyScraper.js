import fs from 'fs/promises';
import path from 'path';
import puppeteer from 'puppeteer';

const OUTPUT_DIR = './data';
const BASE_URL = 'https://charts.spotify.com/charts/view';

// Función para generar delay aleatorio
function randomDelay(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function downloadCSV(browser, country, isFirstDownload = false) {
  const page = await browser.newPage();
  
  try {
    const url = `${BASE_URL}/regional-${country}-daily/latest`;
    console.log(`📄 Navegando a ${url}`);
    
    // Configurar descarga ANTES de navegar
    const tempDir = path.resolve(OUTPUT_DIR, 'temp');
    const client = await page.target().createCDPSession();
    await client.send('Page.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: tempDir
    });
    
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
    
    // Solo esperar 30 segundos en la primera descarga para autenticación
    if (isFirstDownload) {
      console.log('⏳ Esperando 30 segundos para autenticación manual...');
      await new Promise(resolve => setTimeout(resolve, 30000));
    } else {
      const waitTime = randomDelay(2000, 5000);
      console.log(`⏳ Esperando ${waitTime / 1000} segundos...`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
    
    // Buscar el botón de descarga por aria-labelledby con más tiempo
    try {
      await page.waitForSelector('button[aria-labelledby="csv_download"]', { timeout: 20000 });
      console.log('🔍 Botón de descarga encontrado');
    } catch (error) {
      // Tomar screenshot para debug
      await page.screenshot({ path: path.join(OUTPUT_DIR, 'debug.png') });
      console.log('📸 Screenshot guardado en data/debug.png');
      throw new Error('No se encontró el botón de descarga. Revisa el screenshot.');
    }
    
    // Click en el botón
    await page.click('button[aria-labelledby="csv_download"]');
    
    const downloadWaitTime = randomDelay(4000, 7000);
    console.log(`⏳ Esperando descarga (${downloadWaitTime / 1000}s)...`);
    
    // Esperar a que se complete la descarga
    await new Promise(resolve => setTimeout(resolve, downloadWaitTime));
    
    // Leer el archivo descargado
    const files = await fs.readdir(tempDir);
    const csvFile = files.find(f => f.endsWith('.csv'));
    
    if (!csvFile) {
      throw new Error('No se descargó el archivo CSV');
    }
    
    console.log(`✅ Archivo descargado: ${csvFile}`);
    
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

async function processCountry(browser, country, isFirstDownload = false) {
  console.log(`📥 Descargando ${country}`);

  const csv = await downloadCSV(browser, country, isFirstDownload);
  const rows = parseCSV(csv);
  const tracks = normalizeTracks(rows);

  const result = {
    title: 'Spotify Daily Top Songs',
    country: country.toUpperCase(),
    date: new Date().toISOString().split('T')[0],
    total_tracks: tracks.length,
    tracks
  };

  const filePath = path.join(
    OUTPUT_DIR,
    `spotify_${country}_daily_${result.date}.json`
  );

  await fs.writeFile(filePath, JSON.stringify(result, null, 2));
  console.log(`✅ ${country.toUpperCase()} completado - ${tracks.length} canciones guardadas en ${filePath}`);
  
  // Espera aleatoria entre países (excepto en el último)
  if (!isFirstDownload) {
    const betweenCountriesDelay = randomDelay(1000, 3000);
    console.log(`⏸️  Pausa de ${betweenCountriesDelay / 1000}s antes del siguiente país...\n`);
    await new Promise(resolve => setTimeout(resolve, betweenCountriesDelay));
  }
}

async function main() {
  await fs.mkdir(OUTPUT_DIR, { recursive: true });
  await fs.mkdir(path.join(OUTPUT_DIR, 'temp'), { recursive: true });

  console.log('🚀 Iniciando navegador...');
  
  const browser = await puppeteer.launch({ 
    headless: false,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-blink-features=AutomationControlled'
    ]
  });

  // Lista de países a descargar
  const countries = ['global', 'ar', 'au', 'at', 'by', 'be', 'br', 'bg', 'ca', 'cl', 'co', 'cr', 'hr', 'cy', 'cz', 'dk', 'do', 'ec', 'eg', 'ee', 'fi', 'fr', 'de', 'gr', 'gt', 'hn', 'hu', 'is', 'in', 'id', 'ie', 'il', 'it', 'jp', 'kz', 'lv', 'lt', 'lu', 'my', 'mx', 'md', 'ma', 'nl', 'nz', 'ni', 'ng', 'no', 'ph', 'pl', 'pt', 'ro', 'ru', 'sa', 'rs', 'sg', 'sk', 'si', 'za', 'kr', 'es', 'se', 'ch', 'tw', 'th', 'tr', 'ua', 'ae', 'gb', 'us', 've'];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < countries.length; i++) {
    const country = countries[i];
    const isFirstDownload = (i === 0); // Solo la primera vez
    
    try {
      await processCountry(browser, country, isFirstDownload);
      successCount++;
    } catch (err) {
      console.error(`❌ Error en ${country}:`, err.message);
      errorCount++;
    }
  }

  console.log(`\n📊 Resumen:`);
  console.log(`   ✅ Exitosos: ${successCount}`);
  console.log(`   ❌ Errores: ${errorCount}`);
  console.log(`   📁 Archivos en: ${OUTPUT_DIR}`);

  await browser.close();
  console.log('✅ Proceso completado');
}

main();
