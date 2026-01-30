import sql from 'mssql';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuración de la base de datos
const dbConfig = {
    user: 'sa',
    password: 'TuPasswordSegura123!',
    server: 'localhost',
    port: 1433,
    database: 'spotify_charts',
    options: {
        encrypt: false,
        trustServerCertificate: true,
        enableArithAbort: true
    }
};

// Configuración de logs
const LOG_FILE = path.join(__dirname, 'database_load.log');

// Función para escribir logs
function log(message, isError = false) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${isError ? 'ERROR: ' : ''}${message}`;
    
    // Log en consola
    if (isError) {
        console.error(logMessage);
    } else {
        console.log(logMessage);
    }
    
    // Log en archivo
    fs.appendFileSync(LOG_FILE, logMessage + '\n');
}

// Función para extraer el código de país del nombre del archivo
function extractCountryFromFilename(filename) {
    const match = filename.match(/spotify_([a-z]+)_daily_/i);
    return match ? match[1].toLowerCase() : null;
}

// Función para extraer la fecha del nombre del archivo
function extractDateFromFilename(filename) {
    const match = filename.match(/daily_(\d{4}-\d{2}-\d{2})\.json$/);
    return match ? match[1] : null;
}

// Función para insertar datos en la base de datos
async function insertTrackData(pool, country, date, totalTracks, track) {
    const tableName = `spotify_data_${country}`;
    
    try {
        const query = `
            INSERT INTO ${tableName} (
                date, 
                total_tracks, 
                rank, 
                uri, 
                artist_names, 
                track_name, 
                source, 
                peak_rank, 
                previous_rank, 
                days_on_chart, 
                streams_today
            ) VALUES (
                @date, 
                @total_tracks, 
                @rank, 
                @uri, 
                @artist_names, 
                @track_name, 
                @source, 
                @peak_rank, 
                @previous_rank, 
                @days_on_chart, 
                @streams_today
            )
        `;
        
        const request = pool.request();
        request.input('date', sql.Date, new Date(date));
        request.input('total_tracks', sql.Int, totalTracks);
        request.input('rank', sql.Int, track.rank);
        request.input('uri', sql.NVarChar(255), track.uri);
        request.input('artist_names', sql.NVarChar(255), track.artist_names);
        request.input('track_name', sql.NVarChar(255), track.track_name);
        request.input('source', sql.NVarChar(255), track.source || null);
        request.input('peak_rank', sql.Int, track.peak_rank || null);
        request.input('previous_rank', sql.Int, track.previous_rank || null);
        request.input('days_on_chart', sql.Int, track.days_on_chart || null);
        request.input('streams_today', sql.Int, track.streams);
        
        await request.query(query);
        
        return true;
    } catch (error) {
        log(`Error insertando track rank ${track.rank} en tabla ${tableName}: ${error.message}`, true);
        log(`Detalles del track: ${JSON.stringify(track)}`, true);
        return false;
    }
}

// Función para procesar un archivo JSON
async function processJsonFile(pool, filePath, filename) {
    log(`Procesando archivo: ${filename}`);
    
    try {
        // Leer el archivo JSON
        const fileContent = fs.readFileSync(filePath, 'utf8');
        const data = JSON.parse(fileContent);
        
        // Extraer información
        const country = extractCountryFromFilename(filename);
        const date = data.date || extractDateFromFilename(filename);
        const totalTracks = data.total_tracks || 200;
        
        if (!country) {
            log(`No se pudo extraer el código de país del archivo: ${filename}`, true);
            return { success: 0, failed: 0 };
        }
        
        log(`País: ${country.toUpperCase()}, Fecha: ${date}, Total tracks: ${totalTracks}`);
        
        // Verificar que exista el array de tracks
        if (!data.tracks || !Array.isArray(data.tracks)) {
            log(`No se encontró el array de tracks en el archivo: ${filename}`, true);
            return { success: 0, failed: 0 };
        }
        
        let successCount = 0;
        let failedCount = 0;
        
        // Insertar cada track
        for (const track of data.tracks) {
            const result = await insertTrackData(pool, country, date, totalTracks, track);
            if (result) {
                successCount++;
                if (successCount % 50 === 0) {
                    log(`  Insertadas ${successCount} de ${data.tracks.length} canciones...`);
                }
            } else {
                failedCount++;
            }
        }
        
        log(`✓ Archivo ${filename} procesado: ${successCount} éxitos, ${failedCount} fallos`);
        
        // Eliminar el archivo después de procesarlo
        try {
            fs.unlinkSync(filePath);
            log(`✓ Archivo ${filename} eliminado correctamente`);
        } catch (deleteError) {
            log(`Error al eliminar el archivo ${filename}: ${deleteError.message}`, true);
        }
        
        return { success: successCount, failed: failedCount };
        
    } catch (error) {
        log(`Error procesando archivo ${filename}: ${error.message}`, true);
        log(`Stack trace: ${error.stack}`, true);
        return { success: 0, failed: 0 };
    }
}

// Función para procesar una carpeta de fecha
async function processDateFolder(pool, dateFolder) {
    const dateFolderPath = path.join(__dirname, 'data', dateFolder);
    
    if (!fs.existsSync(dateFolderPath)) {
        log(`La carpeta ${dateFolder} no existe`, true);
        return { success: 0, failed: 0, files: 0 };
    }
    
    log(`\n${'='.repeat(80)}`);
    log(`Procesando carpeta: ${dateFolder}`);
    log(`${'='.repeat(80)}`);
    
    const files = fs.readdirSync(dateFolderPath);
    const jsonFiles = files.filter(file => file.endsWith('.json'));
    
    log(`Encontrados ${jsonFiles.length} archivos JSON en ${dateFolder}`);
    
    let totalSuccess = 0;
    let totalFailed = 0;
    
    for (const file of jsonFiles) {
        const filePath = path.join(dateFolderPath, file);
        const result = await processJsonFile(pool, filePath, file);
        totalSuccess += result.success;
        totalFailed += result.failed;
    }
    
    return { success: totalSuccess, failed: totalFailed, files: jsonFiles.length };
}

// Función principal
async function main() {
    // Limpiar archivo de log anterior
    if (fs.existsSync(LOG_FILE)) {
        fs.unlinkSync(LOG_FILE);
    }
    
    log('='.repeat(80));
    log('INICIO DE CARGA DE DATOS A SQL SERVER');
    log('='.repeat(80));
    
    let pool;
    
    try {
        // Conectar a la base de datos
        log('Conectando a la base de datos...');
        pool = await sql.connect(dbConfig);
        log('✓ Conexión establecida correctamente');
        
        // Leer las carpetas en el directorio data
        const dataDir = path.join(__dirname, 'data');
        const folders = fs.readdirSync(dataDir).filter(item => {
            const itemPath = path.join(dataDir, item);
            return fs.statSync(itemPath).isDirectory();
        });
        
        log(`\nEncontradas ${folders.length} carpetas de fechas`);
        
        // Procesar cada carpeta
        let totalFilesProcessed = 0;
        let totalSuccessInserts = 0;
        let totalFailedInserts = 0;
        
        for (const folder of folders) {
            const result = await processDateFolder(pool, folder);
            totalFilesProcessed += result.files;
            totalSuccessInserts += result.success;
            totalFailedInserts += result.failed;
        }
        
        // Resumen final
        log('\n' + '='.repeat(80));
        log('RESUMEN FINAL');
        log('='.repeat(80));
        log(`Total de archivos procesados: ${totalFilesProcessed}`);
        log(`Total de inserciones exitosas: ${totalSuccessInserts}`);
        log(`Total de inserciones fallidas: ${totalFailedInserts}`);
        log(`Tasa de éxito: ${totalFilesProcessed > 0 ? ((totalSuccessInserts / (totalSuccessInserts + totalFailedInserts)) * 100).toFixed(2) : 0}%`);
        log('='.repeat(80));
        
    } catch (error) {
        log(`Error fatal en la ejecución: ${error.message}`, true);
        log(`Stack trace: ${error.stack}`, true);
    } finally {
        // Cerrar conexión
        if (pool) {
            await pool.close();
            log('Conexión cerrada');
        }
    }
}

// Ejecutar
main().catch(error => {
    log(`Error no capturado: ${error.message}`, true);
    process.exit(1);
});
